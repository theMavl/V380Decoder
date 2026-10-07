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
    /// Decodes legacy IMA blocks to PCM, then feeds bounded tracks to GStreamer
    /// for G.711 conversion and publishing to MediaMTX.
    /// MediaMTX owns RTSP sessions, RTCP and client fan-out.
    /// </summary>
    public sealed class MediaMtxBridge : IMediaSink, IDisposable
    {
        private static readonly bool UseGStreamerAdpcm = string.Equals(
            Environment.GetEnvironmentVariable("V380_AUDIO_DECODER"),
            "gstreamer",
            StringComparison.OrdinalIgnoreCase);
        private const int MaxPendingVideoFrames = 5;
        private const int MaxPendingAudioFrames = 4;

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
        private Action<ValidatedMediaPacket> validationOutput;

        /// <summary>Runs the production ingress, decoder, clock and queues
        /// without external processes. Consumers drain explicitly to inject stalls.</summary>
        public static MediaMtxBridge CreateOfflineValidation(Action<ValidatedMediaPacket> output)
        {
            ArgumentNullException.ThrowIfNull(output);
            return new MediaMtxBridge(0, false, "", "") { validationOutput = output };
        }

        public void DrainValidation(string track = "both")
        {
            if (validationOutput == null) throw new InvalidOperationException("Not an offline bridge");
            lock (stateLock)
                foreach (StreamState stream in streams.Values)
                    stream.Publisher?.DrainValidation(track);
        }

        public ValidationMediaState ValidationState
        {
            get
            {
                lock (stateLock)
                {
                    StreamState stream = GetStreamLocked("live");
                    Publisher publisher = stream.Publisher;
                    Publisher.BridgePacket[] queuedAudio = publisher?.audioQueue.ToArray() ?? [];
                    Publisher.BridgePacket[] queuedVideo = publisher?.videoQueue.ToArray() ?? [];
                    ulong audioDuration = queuedAudio.Aggregate(0UL, (sum, packet) =>
                        checked(sum + (packet.Duration == ulong.MaxValue ? 0 : packet.Duration)));
                    ulong videoSpan = queuedVideo.Length > 1 &&
                        queuedVideo[^1].Pts > queuedVideo[0].Pts
                            ? queuedVideo[^1].Pts - queuedVideo[0].Pts : 0;
                    return new ValidationMediaState(stream.ResetCount, stream.PublisherStarts,
                        stream.Publisher?.audioClock.TimelineSamples ?? 0,
                        stream.Publisher?.audioQueueDrops ?? 0,
                        stream.Publisher?.videoQueueDrops ?? 0,
                        queuedAudio.Length, queuedVideo.Length, audioDuration, videoSpan,
                        queuedAudio.Select(packet => packet.Pts).ToArray(),
                        queuedVideo.Select(packet => packet.Pts).ToArray());
                }
            }
        }

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
            Console.Error.WriteLine($"[AUDIO] legacyDecoder={(UseGStreamerAdpcm ? "gstreamer" : "in-process-ima")}");
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
                EnsureFrameEpoch(stream, frame);

                if (stream.Publisher == null)
                {
                    if (!stream.HaveVideoKeyframe)
                    {
                        if (!isKeyframe) return;
                        stream.HaveVideoKeyframe = true;
                        stream.VideoInputFormat = isH265 ? "hevc" : "h264";
                    }

                    bool pendingVideoTooOld = stream.PendingVideo.Count > 0 &&
                        frame.Timestamp > stream.PendingVideo[0].Timestamp &&
                        frame.Timestamp - stream.PendingVideo[0].Timestamp > 500;
                    if (stream.PendingVideo.Count >= MaxPendingVideoFrames || pendingVideoTooOld)
                    {
                        stream.PendingVideoDrops += stream.PendingVideo.Count;
                        stream.PendingVideo.Clear();
                        stream.HaveVideoKeyframe = false;
                        stream.VideoWaitingForKeyframe = true;
                        if (!isKeyframe)
                        {
                            stream.PendingVideoDrops++;
                            Console.Error.WriteLine("[MEDIA-DROP] track=video reason=startup_overflow waiting_for_keyframe");
                            return;
                        }
                        stream.HaveVideoKeyframe = true;
                        stream.VideoInputFormat = isH265 ? "hevc" : "h264";
                    }
                    if (stream.VideoWaitingForKeyframe && isKeyframe)
                    {
                        frame.Discontinuity = true;
                        stream.VideoWaitingForKeyframe = false;
                    }
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
                EnsureFrameEpoch(stream, frame);
                if (!stream.AudioEnabled) return;

                switch (frame.RawType)
                {
                    case 0x16:
                        if (frame.Payload.Length < 4 ||
                            frame.Payload[2] > 88 ||
                            frame.Payload[3] != 0)
                            throw new InvalidDataException("Invalid V380 IMA WAV block");
                        if (stream.LegacyBlockAlign != 0 && stream.LegacyBlockAlign != frame.Payload.Length)
                            throw new InvalidDataException("V380 audio block size changed while streaming");
                        stream.LegacyBlockAlign = frame.Payload.Length;
                        audioDumpStream?.Write(frame.Payload, 0, frame.Payload.Length);
                        if (UseGStreamerAdpcm)
                        {
                            EnsureAudioFormat(stream, "adpcm_ima_wav", frame.Payload.Length);
                            directFrame = frame;
                            break;
                        }
                        EnsureAudioFormat(stream, "pcm_s16le", 0);
                        short[] decoded = ImaAdpcmDecoder.Decode(frame.Payload);
                        byte[] pcm = new byte[decoded.Length * sizeof(short)];
                        Buffer.BlockCopy(decoded, 0, pcm, 0, pcm.Length);
                        directFrame = new FrameData
                        {
                            RawType = frame.RawType,
                            FrameId = frame.FrameId,
                            FrameType = frame.FrameType,
                            FrameRate = frame.FrameRate,
                            Timestamp = frame.Timestamp,
                            ArrivalTimestamp = frame.ArrivalTimestamp,
                            Epoch = frame.Epoch,
                            SampleCount = decoded.Length,
                            Discontinuity = frame.Discontinuity,
                            Payload = pcm
                        };
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
            ulong frameDuration = AudioDurationNanoseconds(frame);
            if (frameDuration > 280_000_000)
                throw new InvalidDataException("Audio frame duration exceeds the live queue hard limit");
            ulong queuedDuration = stream.PendingAudio.Aggregate(
                0UL, (sum, item) => checked(sum + AudioDurationNanoseconds(item)));
            bool droppedAny = false;
            while (stream.PendingAudio.Count >= MaxPendingAudioFrames ||
                   queuedDuration + frameDuration > 140_000_000)
            {
                if (stream.PendingAudio.Count == 0) break;
                queuedDuration -= AudioDurationNanoseconds(stream.PendingAudio[0]);
                stream.PendingAudio.RemoveAt(0);
                stream.PendingAudioDrops++;
                droppedAny = true;
                Console.Error.WriteLine("[MEDIA-DROP] track=audio reason=startup_overflow oldest_block");
            }
            if (droppedAny) frame.Discontinuity = true;
            stream.PendingAudio.Add(frame);
        }

        private static ulong AudioDurationNanoseconds(FrameData frame)
        {
            long samples = frame.SampleCount > 0
                ? frame.SampleCount
                : frame.RawType == 0x16
                    ? 1 + (frame.Payload.Length - 4) * 2
                    : frame.Payload.Length;
            return checked((ulong)samples * 1_000_000_000UL / 8000);
        }

        private static void EnsureFrameEpoch(StreamState stream, FrameData frame)
        {
            if (!stream.HasEpoch)
            {
                stream.Epoch = frame.Epoch;
                stream.HasEpoch = true;
            }
            else if (frame.Epoch != stream.Epoch)
                throw new IOException($"Media epoch changed without a stream reset: {stream.Epoch} -> {frame.Epoch}");
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
                stream.LegacyBlockAlign = 0;
                stream.HaveVideoKeyframe = false;
                stream.VideoWaitingForKeyframe = false;
                stream.HasEpoch = false;
                stream.ResetCount++;
                stream.PendingVideoDrops = 0;
                stream.PendingAudioDrops = 0;
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
                mediaOriginTimestamp,
                validationOutput);

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
                stream.PublisherStarts++;
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
            Console.Error.WriteLine(
                $"[MEDIA-METRICS:{path}] startupDropA={stream.PendingAudioDrops} " +
                $"startupDropV={stream.PendingVideoDrops} publisherStarts={stream.PublisherStarts} " +
                $"resets={stream.ResetCount}");
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
            if (validationOutput != null) return;
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
            public int LegacyBlockAlign;
            public bool HaveVideoKeyframe;
            public bool AudioEnabled = true;
            public bool HasEpoch;
            public int Epoch;
            public long ResetCount;
            public long PublisherStarts;
            public bool VideoWaitingForKeyframe;
            public long PendingVideoDrops;
            public long PendingAudioDrops;
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
            private const int DiscontinuityFlag = 2;
            private const ulong AudioHardLimitNs = 280_000_000;
            private const ulong VideoHardLimitNs = 500_000_000;
            private const int AudioQueueSafetyCapacity = 16;
            private const int VideoQueueSafetyCapacity = 32;
            internal readonly BlockingCollection<BridgePacket> videoQueue = new(VideoQueueSafetyCapacity);
            internal readonly BlockingCollection<BridgePacket> audioQueue = new(AudioQueueSafetyCapacity);
            private readonly object timestampLock = new();
            private readonly object inputLock = new();
            private readonly Stream input;
            private readonly Thread videoWriterThread;
            private readonly Thread audioWriterThread;
            private readonly Process process;
            private readonly string audioFormat;
            private readonly ulong mediaOriginTimestamp;
            internal readonly AudioSampleClock audioClock;
            private readonly Action<ValidatedMediaPacket> validationOutput;
            private readonly string path;
            private ulong lastVideoPts;
            private long lastAudioMetricsTicks;
            private bool videoStarted;
            private bool videoWaitingForKeyframe;
            private long videoIngress;
            private long audioIngress;
            private long videoEmitted;
            private long audioEmitted;
            internal long videoQueueDrops;
            internal long audioQueueDrops;
            private long backwardVideoPts;
            private long rejectedAudioPts;
            private long slowWrites;
            private long maxWriteMicroseconds;
            private long maxAudioQueueAgeMs;
            private long maxVideoQueueAgeMs;
            private long lastAudioOverflowLogTicks;
            private long videoWaitingDropEvents;
            private int writerFailed;
            private int disposed;

            public Publisher(
                int rtspPort,
                string path,
                string videoFormat,
                string audioFormat,
                int audioBlockAlign,
                ulong mediaOriginTimestamp,
                Action<ValidatedMediaPacket> validationOutput = null)
            {
                this.audioFormat = audioFormat;
                this.path = path;
                this.mediaOriginTimestamp = mediaOriginTimestamp;
                this.validationOutput = validationOutput;
                audioClock = new AudioSampleClock(mediaOriginTimestamp);
                if (audioFormat != "pcm_s16le" &&
                    audioFormat != "adpcm_ima_wav" &&
                    audioFormat != "alaw" &&
                    audioFormat != "none")
                {
                    throw new ArgumentException(
                        $"Unsupported audio input format: {audioFormat}",
                        nameof(audioFormat));
                }

                if (validationOutput != null) return;

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
                videoWriterThread = new Thread(() => PumpInput(videoQueue, "video"))
                {
                    IsBackground = true,
                    Name = "v380-gstreamer-video-input"
                };
                audioWriterThread = new Thread(() => PumpInput(audioQueue, "audio"))
                {
                    IsBackground = true,
                    Name = "v380-gstreamer-audio-input"
                };
                videoWriterThread.Start();
                audioWriterThread.Start();
            }

            public bool TryPushVideo(FrameData frame)
            {
                if (!IsAlive()) return false;
                lock (timestampLock)
                {
                    videoIngress++;
                    if (videoWaitingForKeyframe && !frame.IsKeyframe)
                    {
                        videoQueueDrops++;
                        videoWaitingDropEvents++;
                        if (videoWaitingDropEvents == 1 || videoWaitingDropEvents % 30 == 0)
                            Console.Error.WriteLine($"[MEDIA-DROP:{path}] track=video reason=waiting_for_keyframe dropped={videoQueueDrops} event={videoWaitingDropEvents}");
                        return true;
                    }
                    ulong pts = TimestampToNanoseconds(frame.Timestamp);
                    if (videoStarted && pts <= lastVideoPts)
                    {
                        backwardVideoPts++;
                        Console.Error.WriteLine($"[MEDIA-PTS:{path}] track=video reason=backward_or_duplicate pts={pts} previous={lastVideoPts} rejected={backwardVideoPts}");
                        return false;
                    }
                    lastVideoPts = pts;
                    videoStarted = true;
                    var packet = new BridgePacket(
                        PacketVideo,
                        (frame.IsKeyframe ? KeyframeFlag : 0) |
                            (frame.Discontinuity ? DiscontinuityFlag : 0),
                        pts,
                        ulong.MaxValue,
                        frame.Payload,
                        frame.ArrivalTimestamp);
                    return EnqueueBounded(videoQueue, packet, true);
                }
            }

            public bool TryPushAudio(FrameData frame)
            {
                if (audioFormat == "none" || !IsAlive() ||
                    frame?.Payload == null || frame.Payload.Length == 0)
                    return false;

                lock (timestampLock)
                {
                    audioIngress++;
                    int sampleCount = audioFormat switch
                    {
                        "pcm_s16le" => frame.SampleCount,
                        "adpcm_ima_wav" => checked(1 + (frame.Payload.Length - 4) * 2),
                        _ => frame.Payload.Length
                    };
                    if (sampleCount <= 0) return false;
                    if (audioFormat == "pcm_s16le" && frame.Payload.Length != sampleCount * sizeof(short))
                        throw new InvalidDataException("PCM payload length does not match SampleCount");
                    AudioTiming timing;
                    try { timing = audioClock.Next(frame.FrameId, frame.Timestamp, sampleCount); }
                    catch (InvalidDataException ex)
                    {
                        rejectedAudioPts++;
                        Console.Error.WriteLine($"[MEDIA-PTS:{path}] track=audio reason={ex.Message} rejected={rejectedAudioPts}");
                        throw;
                    }
                    var packet = new BridgePacket(PacketAudio,
                        (frame.Discontinuity || timing.Discontinuity) ? DiscontinuityFlag : 0, timing.Pts,
                        timing.Duration, frame.Payload, frame.ArrivalTimestamp);
                    bool accepted = EnqueueBounded(audioQueue, packet, false);
                    LogMetricsIfDue(timing);
                    return accepted;
                }
            }

            private bool EnqueueBounded(
                BlockingCollection<BridgePacket> target,
                BridgePacket packet,
                bool video)
            {
                // Called with timestampLock held; writers only remove entries.
                ulong packetDuration = packet.Duration == ulong.MaxValue ? 0 : packet.Duration;
                BridgePacket[] queued = target.ToArray();
                ulong queuedDuration = queued.Aggregate(0UL, (sum, item) => checked(sum +
                    (item.Duration == ulong.MaxValue ? 0 : item.Duration)));
                ulong hardLimit = video ? VideoHardLimitNs : AudioHardLimitNs;
                // Recovery is complete only when a new keyframe is accepted.
                // Apply this before the fast enqueue path so that a keyframe
                // cannot bypass DISCONT merely because the queue has room.
                if (video && videoWaitingForKeyframe &&
                    (packet.Flags & KeyframeFlag) != 0)
                {
                    videoWaitingForKeyframe = false;
                    packet = packet with { Flags = packet.Flags | DiscontinuityFlag };
                }
                if (!video && packetDuration > hardLimit)
                {
                    Console.Error.WriteLine("[MEDIA-DROP] track=audio reason=block_exceeds_hard_limit");
                    return false;
                }
                bool overLimit = queued.Length >= (video ? VideoQueueSafetyCapacity : AudioQueueSafetyCapacity) || (video
                    ? queued.Length > 0 && packet.Pts > queued[0].Pts &&
                        packet.Pts - queued[0].Pts > VideoHardLimitNs
                    : queuedDuration + packetDuration > hardLimit);
                if (!overLimit && target.TryAdd(packet)) return true;

                int dropped = 0;
                while (target.TryTake(out _)) dropped++;
                if (video)
                {
                    videoQueueDrops += dropped;
                    videoWaitingForKeyframe = true;
                    if ((packet.Flags & KeyframeFlag) == 0)
                    {
                        videoQueueDrops++;
                        videoWaitingDropEvents++;
                        Console.Error.WriteLine($"[MEDIA-DROP:{path}] track=video reason=queue_overflow dropped={dropped + 1} waiting_for_keyframe");
                        return true;
                    }
                    videoWaitingForKeyframe = false;
                }
                else
                    audioQueueDrops += dropped;

                if (video)
                    Console.Error.WriteLine($"[MEDIA-DROP:{path}] track=video reason=queue_overflow dropped={dropped} latestPts={packet.Pts}");
                else
                    LogAudioOverflowIfDue(dropped, packet.Pts);
                packet = packet with { Flags = packet.Flags | DiscontinuityFlag };
                return target.TryAdd(packet);
            }

            private void LogAudioOverflowIfDue(int dropped, ulong latestPts)
            {
                long now = Stopwatch.GetTimestamp();
                if (lastAudioOverflowLogTicks != 0 &&
                    Stopwatch.GetElapsedTime(lastAudioOverflowLogTicks, now) < TimeSpan.FromSeconds(1))
                    return;
                lastAudioOverflowLogTicks = now;
                Console.Error.WriteLine($"[MEDIA-DROP:{path}] track=audio reason=queue_overflow dropped={dropped} totalDropped={audioQueueDrops} latestPts={latestPts}");
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
                (validationOutput != null || !process.HasExited);

            public void DrainValidation(string track)
            {
                if (track is not ("both" or "audio" or "video"))
                    throw new ArgumentException("Unknown validation track", nameof(track));
                lock (timestampLock)
                {
                    if (track != "audio") Drain(videoQueue);
                    if (track != "video") Drain(audioQueue);
                }
            }

            private void Drain(BlockingCollection<BridgePacket> packets)
            {
                while (packets.TryTake(out BridgePacket packet))
                {
                    WritePacket(packet);
                    if (packet.Type == PacketAudio) audioEmitted++;
                    else videoEmitted++;
                }
            }

            private void PumpInput(BlockingCollection<BridgePacket> packets, string track)
            {
                try
                {
                    foreach (BridgePacket packet in packets.GetConsumingEnumerable())
                    {
                        WritePacket(packet);
                        if (packet.Type == PacketAudio)
                            Interlocked.Increment(ref audioEmitted);
                        else
                            Interlocked.Increment(ref videoEmitted);
                    }
                }
                catch (Exception ex)
                {
                    Interlocked.Exchange(ref writerFailed, 1);
                    if (Volatile.Read(ref disposed) == 0)
                        Console.Error.WriteLine($"[PUBLISH] {track} GStreamer input stopped: {ex.Message}");
                }
            }

            private void WritePacket(BridgePacket packet)
            {
                if (validationOutput != null)
                {
                    validationOutput(new ValidatedMediaPacket(packet.Type == PacketAudio,
                        packet.Flags, packet.Pts, packet.Duration, packet.Payload, audioFormat));
                    return;
                }
                long started = Stopwatch.GetTimestamp();
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
                lock (inputLock)
                {
                    input.Write(header);
                    input.Write(packet.Payload, 0, packet.Payload.Length);
                    input.Flush();
                }
                long microseconds = (long)(Stopwatch.GetElapsedTime(started).TotalMilliseconds * 1000);
                if (microseconds > 20_000) Interlocked.Increment(ref slowWrites);
                long previous;
                do
                {
                    previous = Volatile.Read(ref maxWriteMicroseconds);
                    if (microseconds <= previous) break;
                } while (Interlocked.CompareExchange(ref maxWriteMicroseconds, microseconds, previous) != previous);
            }

            private void LogMetricsIfDue(AudioTiming timing)
            {
                long now = Stopwatch.GetTimestamp();
                if (lastAudioMetricsTicks != 0 &&
                    Stopwatch.GetElapsedTime(lastAudioMetricsTicks, now) < TimeSpan.FromSeconds(10))
                    return;
                lastAudioMetricsTicks = now;
                long audioAge = QueueAgeMs(audioQueue, now);
                long videoAge = QueueAgeMs(videoQueue, now);
                maxAudioQueueAgeMs = Math.Max(maxAudioQueueAgeMs, audioAge);
                maxVideoQueueAgeMs = Math.Max(maxVideoQueueAgeMs, videoAge);
                Console.Error.WriteLine(
                    $"[MEDIA-METRICS:{path}] ingressA={audioIngress} ingressV={videoIngress} " +
                    $"writtenA={Volatile.Read(ref audioEmitted)} writtenV={Volatile.Read(ref videoEmitted)} " +
                    $"dropA={audioQueueDrops} dropV={videoQueueDrops} " +
                    $"queueAgeA={audioAge}ms queueAgeV={videoAge}ms " +
                    $"maxAgeA={maxAudioQueueAgeMs}ms maxAgeV={maxVideoQueueAgeMs}ms " +
                    $"inputPtsA={timing.Pts} inputPtsV={lastVideoPts} " +
                    $"timelineSamples={audioClock.TimelineSamples} filteredErrorMs={timing.FilteredErrorNs / 1_000_000.0:F1} " +
                    $"driftMs={timing.DriftFromBaselineNs / 1_000_000.0:F1} " +
                    $"slowWrites={Volatile.Read(ref slowWrites)} maxWriteUs={Volatile.Read(ref maxWriteMicroseconds)}");
            }

            private static long QueueAgeMs(BlockingCollection<BridgePacket> queue, long now)
            {
                BridgePacket[] pending = queue.ToArray();
                return pending.Length == 0 ? 0 :
                    (long)Stopwatch.GetElapsedTime(pending[0].ArrivalTimestamp, now).TotalMilliseconds;
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref disposed, 1) != 0) return;

                videoQueue.CompleteAdding();
                audioQueue.CompleteAdding();
                try { input?.Close(); } catch { }
                StopProcess(process);
                try { videoWriterThread?.Join(500); } catch { }
                try { audioWriterThread?.Join(500); } catch { }
                videoQueue.Dispose();
                audioQueue.Dispose();
            }

            private static void AddArguments(ProcessStartInfo startInfo, params string[] arguments)
            {
                foreach (string argument in arguments)
                    startInfo.ArgumentList.Add(argument);
            }

            internal sealed record BridgePacket(
                int Type,
                int Flags,
                ulong Pts,
                ulong Duration,
                byte[] Payload,
                long ArrivalTimestamp);
        }
    }

    public sealed record ValidatedMediaPacket(bool Audio, int Flags, ulong Pts,
        ulong Duration, byte[] Payload, string AudioFormat);
    public readonly record struct ValidationMediaState(long Resets, long PublisherStarts,
        long TimelineSamples, long AudioDrops, long VideoDrops, int QueuedAudio, int QueuedVideo,
        ulong QueuedAudioDurationNs, ulong QueuedVideoSpanNs,
        ulong[] QueuedAudioPts, ulong[] QueuedVideoPts);
}
