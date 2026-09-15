using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace V380Decoder.src
{
    /// <summary>
    /// Feeds elementary V380 video/audio into FFmpeg and publishes the muxed
    /// stream to MediaMTX.  FFmpeg owns timestamps, pacing and RTP muxing;
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
        private readonly object stateLock = new();
        private readonly List<byte[]> pendingVideo = new();
        private readonly List<byte[]> pendingAudio = new();

        private Process mediaMtxProcess;
        private Publisher publisher;
        private string videoInputFormat;
        private string audioInputFormat;
        private string audioDecoder;
        private bool haveVideoKeyframe;
        private string configDirectory;
        private int disposed;

        public MediaMtxBridge(int rtspPort, bool secure, string username, string password)
        {
            this.rtspPort = rtspPort;
            this.secure = secure;
            this.username = username;
            this.password = password;
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

            string address = $"rtsp://{NetworkHelper.GetLocalIPAddress()}:{rtspPort}/live";
            Console.Error.WriteLine($"[RTSP] {address}{(secure ? " (authentication enabled)" : string.Empty)}");
        }

        public void PushVideo(FrameData frame)
        {
            if (frame?.Payload == null || frame.Payload.Length == 0) return;

            Publisher activePublisher;
            lock (stateLock)
            {
                ThrowIfDisposed();
                EnsureMediaMtxAlive();

                if (publisher == null)
                {
                    bool isH265 = frame.RawType == 0x28 || frame.RawType == 0x29;
                    bool isKeyframe = frame.RawType == 0x00 || frame.RawType == 0x28;

                    if (!haveVideoKeyframe)
                    {
                        if (!isKeyframe) return;
                        haveVideoKeyframe = true;
                        videoInputFormat = isH265 ? "hevc" : "h264";
                    }

                    if (pendingVideo.Count >= MaxPendingVideoFrames)
                        throw new IOException("FFmpeg publisher did not start before the video queue filled");
                    pendingVideo.Add(frame.Payload);
                    TryStartPublisherLocked();
                    return;
                }

                activePublisher = publisher;
            }

            if (!activePublisher.TryPushVideo(frame.Payload))
                throw new IOException("FFmpeg video publisher stopped or its queue filled");
        }

        public void PushAudio(FrameData frame)
        {
            if (frame?.Payload == null || frame.Payload.Length == 0) return;

            Publisher activePublisher;
            lock (stateLock)
            {
                ThrowIfDisposed();
                EnsureMediaMtxAlive();

                if (publisher == null)
                {
                    switch (frame.RawType)
                    {
                        case 0x16:
                            audioInputFormat = "s16le";
                            audioDecoder = "adpcm_ima_ws";
                            break;
                        case 0x1A:
                            audioInputFormat = "alaw";
                            audioDecoder = null;
                            break;
                        default:
                            throw new InvalidDataException($"Unsupported V380 audio type 0x{frame.RawType:X2}");
                    }

                    if (pendingAudio.Count >= MaxPendingAudioFrames)
                        throw new IOException("FFmpeg publisher did not start before the audio queue filled");
                    pendingAudio.Add(frame.Payload);
                    TryStartPublisherLocked();
                    return;
                }

                activePublisher = publisher;
            }

            if (!activePublisher.TryPushAudio(frame.Payload))
                throw new IOException("FFmpeg audio publisher stopped or its queue filled");
        }

        public void Reset()
        {
            Publisher oldPublisher;
            lock (stateLock)
            {
                if (Volatile.Read(ref disposed) != 0) return;

                oldPublisher = publisher;
                publisher = null;
                videoInputFormat = null;
                audioInputFormat = null;
                audioDecoder = null;
                haveVideoKeyframe = false;
                pendingVideo.Clear();
                pendingAudio.Clear();
            }

            oldPublisher?.Dispose();
            if (oldPublisher != null)
                Console.Error.WriteLine("[PUBLISH] source reset; waiting for a new keyframe");
        }

        private void TryStartPublisherLocked()
        {
            if (publisher != null || !haveVideoKeyframe || audioInputFormat == null)
                return;

            publisher = new Publisher(
                rtspPort,
                videoInputFormat,
                audioInputFormat,
                audioDecoder);

            foreach (byte[] frame in pendingVideo)
            {
                if (!publisher.TryPushVideo(frame))
                    throw new IOException("Unable to queue initial video for FFmpeg");
            }
            foreach (byte[] frame in pendingAudio)
            {
                if (!publisher.TryPushAudio(frame))
                    throw new IOException("Unable to queue initial audio for FFmpeg");
            }

            pendingVideo.Clear();
            pendingAudio.Clear();
            Console.Error.WriteLine(
                $"[PUBLISH] FFmpeg started video={videoInputFormat} audio={audioDecoder ?? audioInputFormat}");
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
            config.AppendLine("        path: live");
            config.AppendLine($"  - user: {readUser}");
            config.AppendLine($"    pass: {readPassword}");
            config.AppendLine("    ips: []");
            config.AppendLine("    permissions:");
            config.AppendLine("      - action: read");
            config.AppendLine("        path: live");
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
            config.AppendLine("  live:");
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

            Publisher oldPublisher;
            Process oldMediaMtx;
            string oldConfigDirectory;
            lock (stateLock)
            {
                oldPublisher = publisher;
                publisher = null;
                oldMediaMtx = mediaMtxProcess;
                mediaMtxProcess = null;
                oldConfigDirectory = configDirectory;
                configDirectory = null;
            }

            oldPublisher?.Dispose();
            StopProcess(oldMediaMtx);

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

        private sealed class Publisher : IDisposable
        {
            private const int VideoQueueCapacity = 48;
            private const int AudioQueueCapacity = 64;

            private readonly BlockingCollection<byte[]> videoQueue = new(VideoQueueCapacity);
            private readonly BlockingCollection<byte[]> audioQueue = new(AudioQueueCapacity);
            private readonly TcpListener videoListener;
            private readonly TcpListener audioListener;
            private readonly Thread videoWriterThread;
            private readonly Thread audioWriterThread;
            private readonly Process process;
            private TcpClient videoClient;
            private TcpClient audioClient;
            private int disposed;

            public Publisher(int rtspPort, string videoFormat, string audioFormat, string audioCodec)
            {
                videoListener = CreateListener(out int videoPort);
                audioListener = CreateListener(out int audioPort);

                videoWriterThread = CreateWriterThread(
                    "v380-ffmpeg-video", videoListener, videoQueue, client => videoClient = client);
                audioWriterThread = CreateWriterThread(
                    "v380-ffmpeg-audio", audioListener, audioQueue, client => audioClient = client);
                videoWriterThread.Start();
                audioWriterThread.Start();

                var startInfo = new ProcessStartInfo
                {
                    FileName = "ffmpeg",
                    UseShellExecute = false,
                    RedirectStandardError = true,
                    RedirectStandardOutput = true,
                    CreateNoWindow = true
                };

                AddArguments(startInfo,
                    "-hide_banner", "-nostats", "-loglevel", "warning",
                    "-fflags", "+genpts+nobuffer",
                    "-thread_queue_size", "64",
                    "-use_wallclock_as_timestamps", "1",
                    "-probesize", "1000000", "-analyzeduration", "1000000",
                    "-f", videoFormat,
                    "-i", $"tcp://127.0.0.1:{videoPort}",
                    "-thread_queue_size", "64",
                    "-use_wallclock_as_timestamps", "1",
                    "-f", audioFormat,
                    "-ar", "8000", "-ac", "1");

                if (audioCodec != null)
                    AddArguments(startInfo, "-c:a", audioCodec);

                AddArguments(startInfo,
                    "-i", $"tcp://127.0.0.1:{audioPort}",
                    "-map", "0:v:0", "-map", "1:a:0",
                    "-c:v", "copy",
                    "-c:a", "pcm_alaw", "-ar", "8000", "-ac", "1",
                    "-af", "aresample=async=1000:min_hard_comp=0.100",
                    "-fps_mode", "passthrough",
                    "-max_interleave_delta", "100000",
                    "-muxdelay", "0",
                    "-f", "rtsp", "-rtsp_transport", "tcp",
                    $"rtsp://127.0.0.1:{rtspPort}/live");

                process = new Process { StartInfo = startInfo };
                try
                {
                    if (!process.Start())
                        throw new InvalidOperationException("Failed to start FFmpeg publisher");
                }
                catch
                {
                    Dispose();
                    throw;
                }

                StartLogPump(process.StandardError, "[FFMPEG]");
                StartLogPump(process.StandardOutput, "[FFMPEG]");
            }

            public bool TryPushVideo(byte[] payload) =>
                IsAlive() && videoQueue.TryAdd(payload);

            public bool TryPushAudio(byte[] payload) =>
                IsAlive() && audioQueue.TryAdd(payload);

            private bool IsAlive() =>
                Volatile.Read(ref disposed) == 0 && !process.HasExited;

            private Thread CreateWriterThread(
                string name,
                TcpListener listener,
                BlockingCollection<byte[]> queue,
                Action<TcpClient> setClient)
            {
                return new Thread(() =>
                {
                    try
                    {
                        TcpClient client = listener.AcceptTcpClient();
                        client.NoDelay = true;
                        client.SendBufferSize = 64 * 1024;
                        setClient(client);
                        using NetworkStream stream = client.GetStream();
                        foreach (byte[] payload in queue.GetConsumingEnumerable())
                            stream.Write(payload, 0, payload.Length);
                    }
                    catch (Exception ex)
                    {
                        if (Volatile.Read(ref disposed) == 0)
                            Console.Error.WriteLine($"[PUBLISH] {name} stopped: {ex.Message}");
                    }
                })
                {
                    IsBackground = true,
                    Name = name
                };
            }

            private static TcpListener CreateListener(out int port)
            {
                var listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start(1);
                port = ((IPEndPoint)listener.LocalEndpoint).Port;
                return listener;
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref disposed, 1) != 0) return;

                videoQueue.CompleteAdding();
                audioQueue.CompleteAdding();
                try { videoClient?.Close(); } catch { }
                try { audioClient?.Close(); } catch { }
                try { videoListener.Stop(); } catch { }
                try { audioListener.Stop(); } catch { }
                StopProcess(process);
                videoQueue.Dispose();
                audioQueue.Dispose();
            }

            private static void AddArguments(ProcessStartInfo startInfo, params string[] arguments)
            {
                foreach (string argument in arguments)
                    startInfo.ArgumentList.Add(argument);
            }
        }
    }
}
