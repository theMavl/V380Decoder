using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace V380Decoder.src
{
    /// <summary>
    /// Feeds raw V380 media to GStreamer, which decodes legacy IMA audio,
    /// converts it to G.711 and publishes both tracks to MediaMTX.
    /// MediaMTX owns RTSP sessions, RTCP and client fan-out.
    /// </summary>
    public sealed class MediaMtxBridge : IMediaSink, IDisposable
    {
        private const int MaxPendingVideoFrames = 120;
        private const int MaxPendingAudioFrames = 128;

        private readonly int rtspPort;
        private readonly bool secure;
        private readonly string username;
        private readonly string password;
        private readonly string audioDumpPath;
        private readonly object stateLock = new();
        private readonly Dictionary<string, StreamState> streams = new(StringComparer.Ordinal);

        private Process mediaMtxProcess;
        private FileStream audioDumpStream;
        private string configDirectory;
        private int disposed;

        public MediaMtxBridge(
            int rtspPort,
            bool secure,
            string username,
            string password,
            string audioDumpPath = "")
        {
            this.rtspPort = rtspPort;
            this.secure = secure;
            this.username = username;
            this.password = password;
            this.audioDumpPath = audioDumpPath;
        }

        public void Start()
        {
            lock (stateLock)
            {
                ThrowIfDisposed();
                if (mediaMtxProcess != null) return;

                configDirectory = Path.Combine(
                    Path.GetTempPath(),
                    $"v380decoder-mediamtx-{Environment.ProcessId}-{Guid.NewGuid():N}");
                Directory.CreateDirectory(configDirectory);

                string configPath = Path.Combine(configDirectory, "mediamtx.yml");
                File.WriteAllText(configPath, BuildMediaMtxConfig(), new UTF8Encoding(false));
                TryRestrictConfigPermissions(configPath);

                var startInfo = new ProcessStartInfo
                {
                    FileName = "mediamtx",
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    CreateNoWindow = true
                };
                startInfo.ArgumentList.Add(configPath);

                mediaMtxProcess = new Process { StartInfo = startInfo };
                try
                {
                    if (!mediaMtxProcess.Start())
                        throw new InvalidOperationException("Failed to start MediaMTX");
                }
                catch
                {
                    StopProcess(mediaMtxProcess);
                    mediaMtxProcess = null;
                    throw;
                }

                StartLogPump(mediaMtxProcess.StandardOutput, "[MEDIAMTX]");
                StartLogPump(mediaMtxProcess.StandardError, "[MEDIAMTX]");
            }

            WaitForMediaMtx();

            if (!string.IsNullOrWhiteSpace(audioDumpPath))
            {
                string fullDumpPath = Path.GetFullPath(audioDumpPath);
                string parent = Path.GetDirectoryName(fullDumpPath);
                if (!string.IsNullOrEmpty(parent))
                    Directory.CreateDirectory(parent);
                audioDumpStream = new FileStream(
                    fullDumpPath,
                    FileMode.Create,
                    FileAccess.Write,
                    FileShare.Read,
                    64 * 1024,
                    FileOptions.SequentialScan);
                Console.Error.WriteLine($"[AUDIO-DUMP] recording raw IMA WAV blocks to {fullDumpPath}");
            }

            string address = $"rtsp://{NetworkHelper.GetLocalIPAddress()}:{rtspPort}/live";
            Console.Error.WriteLine($"[RTSP] {address}{(secure ? " (authentication enabled)" : string.Empty)}");
        }

        public void PushVideo(FrameData frame)
            => PushVideo("live", frame);

        public void PushAudio(FrameData frame)
            => PushAudio("live", frame);

        public void Reset()
            => Reset("live");

        public IMediaSink CreateSink(string path, bool includeAudio = true)
        {
            if (string.IsNullOrWhiteSpace(path) ||
                path.Any(c => !(char.IsAsciiLetterOrDigit(c) || c == '-' || c == '_')))
                throw new ArgumentException("RTSP path contains unsupported characters", nameof(path));

            lock (stateLock)
            {
                ThrowIfDisposed();
                GetStreamLocked(path).AudioEnabled = includeAudio;
            }
            return new PathSink(this, path);
        }

        private void PushVideo(string path, FrameData frame)
        {
            if (frame?.Payload == null || frame.Payload.Length == 0) return;
            bool isH265 = frame.RawType == 0x28 || frame.RawType == 0x29;
            bool isKeyframe = frame.IsKeyframe;

            Publisher activePublisher;
            lock (stateLock)
            {
                ThrowIfDisposed();
                EnsureMediaMtxAlive();
                StreamState stream = GetStreamLocked(path);

                if (stream.Publisher == null)
                {
                    if (!stream.HaveVideoKeyframe)
                    {
                        if (!isKeyframe) return;
                        stream.HaveVideoKeyframe = true;
                        stream.VideoInputFormat = isH265 ? "hevc" : "h264";
                    }

                    if (stream.PendingVideo.Count >= MaxPendingVideoFrames)
                        throw new IOException("GStreamer publisher did not start before the video queue filled");
                    stream.PendingVideo.Add(frame);
                    TryStartPublisherLocked(path, stream);
                    return;
                }

                activePublisher = stream.Publisher;
            }

            if (!activePublisher.TryPushVideo(frame))
                throw new IOException("GStreamer video publisher stopped or its queue filled");
        }

        private void PushAudio(string path, FrameData frame)
        {
            if (frame?.Payload == null || frame.Payload.Length == 0) return;

            Publisher activePublisher = null;
            FrameData directFrame = null;
            lock (stateLock)
            {
                ThrowIfDisposed();
                EnsureMediaMtxAlive();
                StreamState stream = GetStreamLocked(path);
                if (!stream.AudioEnabled) return;

                switch (frame.RawType)
                {
                    case 0x16:
                        if (frame.Payload.Length < 64 || frame.Payload.Length > 8192 ||
                            frame.Payload[2] > 88 ||
                            frame.Payload[3] != 0)
                            throw new InvalidDataException("Invalid V380 IMA WAV block");
                        EnsureAudioFormat(stream, "adpcm_ima_wav", frame.Payload.Length);
                        directFrame = frame;
                        audioDumpStream?.Write(frame.Payload, 0, frame.Payload.Length);
                        break;
                    case 0x1A:
                        EnsureAudioFormat(stream, "alaw");
                        directFrame = frame;
                        break;
                    default:
                        throw new InvalidDataException($"Unsupported V380 audio type 0x{frame.RawType:X2}");
                }

                if (stream.Publisher == null)
                {
                    QueuePendingAudio(stream, directFrame);
                    TryStartPublisherLocked(path, stream);
                    return;
                }
                activePublisher = stream.Publisher;
            }

            if (!activePublisher.TryPushAudio(directFrame))
                throw new IOException("GStreamer audio publisher stopped or its queue filled");
        }

        private static void QueuePendingAudio(StreamState stream, FrameData frame)
        {
            if (stream.PendingAudio.Count >= MaxPendingAudioFrames)
                throw new IOException("GStreamer publisher did not start before the audio queue filled");
            stream.PendingAudio.Add(frame);
        }

        private static void EnsureAudioFormat(
            StreamState stream,
            string format,
            int blockAlign = 0)
        {
            if (stream.AudioInputFormat == null)
            {
                stream.AudioInputFormat = format;
                stream.AudioBlockAlign = blockAlign;
                return;
            }
            if (!string.Equals(stream.AudioInputFormat, format, StringComparison.Ordinal))
                throw new InvalidDataException("V380 audio codec changed while streaming");
            if (stream.AudioBlockAlign != blockAlign)
                throw new InvalidDataException("V380 audio block size changed while streaming");
        }

        private void Reset(string path)
        {
            Publisher oldPublisher;
            lock (stateLock)
            {
                if (Volatile.Read(ref disposed) != 0) return;
                StreamState stream = GetStreamLocked(path);

                oldPublisher = stream.Publisher;
                stream.Publisher = null;
                stream.VideoInputFormat = null;
                stream.AudioInputFormat = null;
                stream.AudioBlockAlign = 0;
                stream.HaveVideoKeyframe = false;
                stream.PendingVideo.Clear();
                stream.PendingAudio.Clear();
            }

            oldPublisher?.Dispose();
            if (oldPublisher != null)
                Console.Error.WriteLine($"[PUBLISH:{path}] source reset; waiting for a new keyframe");
        }

        private void TryStartPublisherLocked(string path, StreamState stream)
        {
            if (stream.Publisher != null || !stream.HaveVideoKeyframe ||
                (stream.AudioEnabled &&
                    (stream.AudioInputFormat == null || stream.PendingAudio.Count == 0)))
                return;

            ulong mediaOriginTimestamp = stream.PendingVideo
                .Concat(stream.PendingAudio)
                .Where(frame => frame.Timestamp != 0)
                .Select(frame => frame.Timestamp)
                .DefaultIfEmpty(0UL)
                .Min();

            var newPublisher = new Publisher(
                rtspPort,
                path,
                stream.VideoInputFormat,
                stream.AudioEnabled ? stream.AudioInputFormat : "none",
                stream.AudioEnabled ? stream.AudioBlockAlign : 0,
                mediaOriginTimestamp);

            try
            {
                foreach (FrameData frame in stream.PendingVideo)
                {
                    if (!newPublisher.TryPushVideo(frame))
                        throw new IOException("Unable to queue initial video for GStreamer");
                }
                foreach (FrameData frame in stream.PendingAudio)
                {
                    if (!newPublisher.TryPushAudio(frame))
                        throw new IOException("Unable to queue initial audio for GStreamer");
                }

                stream.Publisher = newPublisher;
            }
            catch
            {
                newPublisher.Dispose();
                throw;
            }

            stream.PendingVideo.Clear();
            stream.PendingAudio.Clear();
            Console.Error.WriteLine(
                $"[PUBLISH:{path}] GStreamer started video={stream.VideoInputFormat} " +
                $"audio={(stream.AudioEnabled ? stream.AudioInputFormat : "none")}");
        }

        private StreamState GetStreamLocked(string path)
        {
            if (!streams.TryGetValue(path, out StreamState stream))
            {
                stream = new StreamState();
                streams.Add(path, stream);
            }
            return stream;
        }

        private string BuildMediaMtxConfig()
        {
            string readUser = secure ? JsonSerializer.Serialize(username) : "any";
            string readPassword = secure ? JsonSerializer.Serialize(password) : string.Empty;

            var config = new StringBuilder();
            config.AppendLine("logLevel: info");
            config.AppendLine("logDestinations: [stdout]");
            config.AppendLine("readTimeout: 10s");
            config.AppendLine("writeTimeout: 10s");
            config.AppendLine("writeQueueSize: 512");
            config.AppendLine("authMethod: internal");
            config.AppendLine("authInternalUsers:");
            config.AppendLine("  - user: any");
            config.AppendLine("    pass:");
            config.AppendLine("    ips: [\"127.0.0.1\", \"::1\"]");
            config.AppendLine("    permissions:");
            config.AppendLine("      - action: publish");
            config.AppendLine("        path: ~^live(?:-[a-z0-9_-]+)?$");
            config.AppendLine($"  - user: {readUser}");
            config.AppendLine($"    pass: {readPassword}");
            config.AppendLine("    ips: []");
            config.AppendLine("    permissions:");
            config.AppendLine("      - action: read");
            config.AppendLine("        path: ~^live(?:-[a-z0-9_-]+)?$");
            config.AppendLine("rtsp: true");
            config.AppendLine("rtspTransports: [tcp]");
            config.AppendLine($"rtspAddress: :{rtspPort}");
            config.AppendLine("rtspAuthMethods: [basic]");
            config.AppendLine("rtmp: false");
            config.AppendLine("hls: false");
            config.AppendLine("webrtc: false");
            config.AppendLine("srt: false");
            config.AppendLine("moq: false");
            config.AppendLine("paths:");
            config.AppendLine("  all_others:");
            config.AppendLine("    source: publisher");
            config.AppendLine("    overridePublisher: true");
            return config.ToString();
        }

        private void WaitForMediaMtx()
        {
            var deadline = Stopwatch.StartNew();
            while (deadline.Elapsed < TimeSpan.FromSeconds(10))
            {
                lock (stateLock)
                {
                    ThrowIfDisposed();
                    EnsureMediaMtxAlive();
                }

                try
                {
                    using var probe = new TcpClient();
                    Task connect = probe.ConnectAsync(IPAddress.Loopback, rtspPort);
                    if (connect.Wait(TimeSpan.FromMilliseconds(200)) && probe.Connected)
                        return;
                }
                catch { }

                Thread.Sleep(50);
            }

            throw new TimeoutException($"MediaMTX did not listen on RTSP port {rtspPort}");
        }

        private void EnsureMediaMtxAlive()
        {
            if (mediaMtxProcess == null || mediaMtxProcess.HasExited)
                throw new IOException("MediaMTX process stopped");
        }

        private static void StartLogPump(StreamReader reader, string prefix)
        {
            var thread = new Thread(() =>
            {
                try
                {
                    string line;
                    while ((line = reader.ReadLine()) != null)
                        Console.Error.WriteLine($"{prefix} {line}");
                }
                catch { }
            })
            {
                IsBackground = true,
                Name = $"{prefix.Trim('[', ']')}-log"
            };
            thread.Start();
        }

        private static void TryRestrictConfigPermissions(string path)
        {
            try
            {
                File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
            }
            catch (PlatformNotSupportedException) { }
        }

        private void ThrowIfDisposed()
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref disposed) != 0, this);
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0) return;

            Publisher[] oldPublishers;
            Process oldMediaMtx;
            FileStream oldAudioDump;
            string oldConfigDirectory;
            lock (stateLock)
            {
                oldPublishers = streams.Values
                    .Select(stream => stream.Publisher)
                    .Where(publisher => publisher != null)
                    .ToArray();
                streams.Clear();
                oldMediaMtx = mediaMtxProcess;
                mediaMtxProcess = null;
                oldAudioDump = audioDumpStream;
                audioDumpStream = null;
                oldConfigDirectory = configDirectory;
                configDirectory = null;
            }

            foreach (Publisher publisher in oldPublishers)
                publisher.Dispose();
            StopProcess(oldMediaMtx);
            oldAudioDump?.Dispose();

            if (!string.IsNullOrEmpty(oldConfigDirectory))
            {
                try { Directory.Delete(oldConfigDirectory, recursive: true); } catch { }
            }
        }

        private static void StopProcess(Process process)
        {
            if (process == null) return;
            try
            {
                if (!process.HasExited)
                    process.Kill(entireProcessTree: true);
            }
            catch { }
            try { process.WaitForExit(2000); } catch { }
            process.Dispose();
        }

        private sealed class StreamState
        {
            public readonly List<FrameData> PendingVideo = new();
            public readonly List<FrameData> PendingAudio = new();
            public Publisher Publisher;
            public string VideoInputFormat;
            public string AudioInputFormat;
            public int AudioBlockAlign;
            public bool HaveVideoKeyframe;
            public bool AudioEnabled = true;
        }

        private sealed class PathSink : IMediaSink
        {
            private readonly MediaMtxBridge owner;
            private readonly string path;

            public PathSink(MediaMtxBridge owner, string path)
            {
                this.owner = owner;
                this.path = path;
            }

            public void PushVideo(FrameData frame) => owner.PushVideo(path, frame);
            public void PushAudio(FrameData frame) => owner.PushAudio(path, frame);
            public void Reset() => owner.Reset(path);
        }

        private sealed class Publisher : IDisposable
        {
            private const int BridgeHeaderSize = 28;
            private const int PacketVideo = 1;
            private const int PacketAudio = 2;
            private const int KeyframeFlag = 1;
            private const long NanosecondsPerSecond = 1_000_000_000;
            private const int QueueCapacity =
                MaxPendingVideoFrames + MaxPendingAudioFrames + 64;

            private readonly BlockingCollection<BridgePacket> queue = new(QueueCapacity);
            private readonly object timestampLock = new();
            private readonly Stream input;
            private readonly Thread writerThread;
            private readonly Process process;
            private readonly string audioFormat;
            private readonly int audioBlockAlign;
            private readonly ulong mediaOriginTimestamp;
            private long audioSampleNumber;
            private ulong audioStartPts;
            private ulong lastVideoPts;
            private bool audioStarted;
            private bool videoStarted;
            private int writerFailed;
            private int disposed;

            public Publisher(
                int rtspPort,
                string path,
                string videoFormat,
                string audioFormat,
                int audioBlockAlign,
                ulong mediaOriginTimestamp)
            {
                this.audioFormat = audioFormat;
                this.audioBlockAlign = audioBlockAlign;
                this.mediaOriginTimestamp = mediaOriginTimestamp;
                if (audioFormat == "adpcm_ima_wav" &&
                    (audioBlockAlign < 64 || audioBlockAlign > 8192))
                    throw new ArgumentOutOfRangeException(
                        nameof(audioBlockAlign),
                        "IMA WAV block alignment is outside the GStreamer decoder range");
                if (audioFormat != "adpcm_ima_wav" &&
                    audioFormat != "alaw" &&
                    audioFormat != "none")
                {
                    throw new ArgumentException(
                        $"Unsupported audio input format: {audioFormat}",
                        nameof(audioFormat));
                }

                var startInfo = new ProcessStartInfo
                {
                    FileName = Path.Combine(AppContext.BaseDirectory, "v380-gst-bridge"),
                    UseShellExecute = false,
                    RedirectStandardInput = true,
                    RedirectStandardError = true,
                    RedirectStandardOutput = true,
                    CreateNoWindow = true
                };
                AddArguments(
                    startInfo,
                    "--url", $"rtsp://127.0.0.1:{rtspPort}/{path}",
                    "--video", videoFormat,
                    "--audio", audioFormat,
                    "--audio-block-align", audioBlockAlign.ToString());

                process = new Process { StartInfo = startInfo };
                try
                {
                    if (!process.Start())
                        throw new InvalidOperationException("Failed to start GStreamer publisher");
                }
                catch
                {
                    Dispose();
                    throw;
                }

                input = process.StandardInput.BaseStream;
                StartLogPump(process.StandardError, "[GSTREAMER]");
                StartLogPump(process.StandardOutput, "[GSTREAMER]");
                writerThread = new Thread(PumpInput)
                {
                    IsBackground = true,
                    Name = "v380-gstreamer-input"
                };
                writerThread.Start();
            }

            public bool TryPushVideo(FrameData frame)
            {
                if (!IsAlive()) return false;

                BridgePacket packet;
                lock (timestampLock)
                {
                    if (frame.FrameRate == 0)
                        return false;
                    int frameRate = frame.FrameRate;
                    ulong duration = UnitsToNanoseconds(1, frameRate);
                    ulong pts = TimestampToNanoseconds(frame.Timestamp);
                    if (videoStarted && pts <= lastVideoPts)
                        return false;
                    lastVideoPts = pts;
                    videoStarted = true;
                    packet = new BridgePacket(
                        PacketVideo,
                        frame.IsKeyframe ? KeyframeFlag : 0,
                        pts,
                        duration,
                        frame.Payload);
                }
                return queue.TryAdd(packet);
            }

            public bool TryPushAudio(FrameData frame)
            {
                if (audioFormat == "none" || !IsAlive() ||
                    frame?.Payload == null || frame.Payload.Length == 0)
                    return false;

                BridgePacket packet;
                lock (timestampLock)
                {
                    int sampleCount;
                    if (audioFormat == "adpcm_ima_wav")
                    {
                        if (frame.Payload.Length != audioBlockAlign)
                            return false;
                        sampleCount = checked(1 + (frame.Payload.Length - 4) * 2);
                    }
                    else
                    {
                        sampleCount = frame.Payload.Length;
                    }
                    if (!audioStarted)
                    {
                        audioStartPts = TimestampToNanoseconds(frame.Timestamp);
                        audioStarted = true;
                    }
                    long firstSample = audioSampleNumber;
                    audioSampleNumber += sampleCount;
                    ulong pts = checked(audioStartPts + UnitsToNanoseconds(firstSample, 8000));
                    ulong end = checked(audioStartPts + UnitsToNanoseconds(audioSampleNumber, 8000));
                    packet = new BridgePacket(PacketAudio, 0, pts, end - pts, frame.Payload);
                }
                return queue.TryAdd(packet);
            }

            private ulong TimestampToNanoseconds(ulong timestamp)
            {
                if (timestamp == 0 || mediaOriginTimestamp == 0 ||
                    timestamp < mediaOriginTimestamp)
                    return 0;
                return checked((timestamp - mediaOriginTimestamp) * 1_000_000UL);
            }

            private bool IsAlive() =>
                Volatile.Read(ref disposed) == 0 &&
                Volatile.Read(ref writerFailed) == 0 &&
                !process.HasExited;

            private void PumpInput()
            {
                try
                {
                    foreach (BridgePacket packet in queue.GetConsumingEnumerable())
                        WritePacket(packet);
                }
                catch (Exception ex)
                {
                    Interlocked.Exchange(ref writerFailed, 1);
                    if (Volatile.Read(ref disposed) == 0)
                        Console.Error.WriteLine($"[PUBLISH] GStreamer input stopped: {ex.Message}");
                }
            }

            private void WritePacket(BridgePacket packet)
            {
                Span<byte> header = stackalloc byte[BridgeHeaderSize];
                header[0] = (byte)'V';
                header[1] = (byte)'3';
                header[2] = (byte)'8';
                header[3] = (byte)'B';
                header[4] = (byte)packet.Type;
                header[5] = (byte)packet.Flags;
                BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(8, 8), packet.Pts);
                BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(16, 8), packet.Duration);
                BinaryPrimitives.WriteUInt32LittleEndian(
                    header.Slice(24, 4),
                    checked((uint)packet.Payload.Length));
                input.Write(header);
                input.Write(packet.Payload, 0, packet.Payload.Length);
                input.Flush();
            }

            private static ulong UnitsToNanoseconds(long units, int unitsPerSecond)
            {
                long seconds = units / unitsPerSecond;
                long remainder = units % unitsPerSecond;
                return checked((ulong)(seconds * NanosecondsPerSecond +
                    remainder * NanosecondsPerSecond / unitsPerSecond));
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref disposed, 1) != 0) return;

                queue.CompleteAdding();
                try { input?.Close(); } catch { }
                StopProcess(process);
                queue.Dispose();
            }

            private static void AddArguments(ProcessStartInfo startInfo, params string[] arguments)
            {
                foreach (string argument in arguments)
                    startInfo.ArgumentList.Add(argument);
            }

            private sealed record BridgePacket(
                int Type,
                int Flags,
                ulong Pts,
                ulong Duration,
                byte[] Payload);
        }
    }
}
