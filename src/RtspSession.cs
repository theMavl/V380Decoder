using System.Diagnostics;
using System.Net.Sockets;
using System.Text;

namespace V380Decoder.src
{
    public class RtspSession
    {
        private readonly int id;
        private readonly TcpClient tcp;
        private readonly NetworkStream ns;
        private readonly RtspServer server;
        private readonly bool secure;
        private readonly object sendLock = new();
        private bool authenticated;
        private Thread readThread;
        private volatile bool playing;
        private volatile bool alive = true;
        private int closed;

        // interleaved channels negotiated in SETUP
        private byte videoCh = 0, audioCh = 2;

        // RTP state
        private ushort videoSeq = (ushort)Random.Shared.Next(0, 1 << 16);
        private ushort audioSeq = (ushort)Random.Shared.Next(0, 1 << 16);
        private readonly uint videoSsrc = (uint)Random.Shared.NextInt64(1, 1L << 32);
        private readonly uint audioSsrc = (uint)Random.Shared.NextInt64(1, 1L << 32);
        private readonly byte[] rtcpCname;

        // Both tracks are tied to the same monotonic clock.  The previous
        // implementation advanced video by a fixed 7500 ticks per frame,
        // effectively claiming that every camera ran at exactly 12 fps.
        // That made A/V drift indefinitely whenever the real rate differed.
        private readonly long mediaClockOriginTicks = Stopwatch.GetTimestamp();
        private readonly uint videoRtpBase = (uint)Random.Shared.NextInt64(0, 1L << 32);
        private readonly uint audioRtpBase = (uint)Random.Shared.NextInt64(0, 1L << 32);
        private uint lastVideoRtpTimestamp;
        private bool videoClockInitialized;
        private uint nextAudioRtpTimestamp;
        private bool audioClockInitialized;

        // A small amount of scheduler jitter is normal.  A larger difference
        // means that the upstream camera/decoder stopped or restarted, so the
        // audio clock must follow real time instead of accumulating latency.
        private const int MaxAudioClockDriftSamples = 1600; // 200 ms at 8 kHz

        private long audioPacketCount;
        private long videoPacketCount;
        private long incomingInterleavedCount;
        private long lastAudioSenderReportTicks;
        private long lastVideoSenderReportTicks;
        private uint audioRtcpPacketCount;
        private uint audioRtcpOctetCount;
        private uint videoRtcpPacketCount;
        private uint videoRtcpOctetCount;

        public event Action OnClose;

        public RtspSession(int id, TcpClient tcp, RtspServer server, bool secure = false)
        {
            this.id = id; this.tcp = tcp; this.server = server;
            this.secure = secure;
            this.authenticated = !secure; // if not secure, auto-authenticate
            rtcpCname = Encoding.ASCII.GetBytes($"v380decoder-{id}");
            ns = tcp.GetStream();
        }

        public void Start()
        {
            readThread = new Thread(ReadLoop) { IsBackground = true, Name = $"rtsp-{id}" };
            readThread.Start();
        }

        public void Close()
        {
            if (Interlocked.Exchange(ref closed, 1) != 0) return;

            alive = false;
            playing = false;
            try { tcp.Close(); } catch { }
            OnClose?.Invoke();
        }

        // ── RTSP request reader ──────────────────────────────────
        void ReadLoop()
        {
            const int MaxRtspMessageSize = 1024 * 1024;
            var pending = new List<byte>();
            var buf = new byte[4096];

            try
            {
                while (alive)
                {
                    int n = ns.Read(buf, 0, buf.Length);
                    if (n <= 0) break;

                    for (int i = 0; i < n; i++)
                        pending.Add(buf[i]);

                    while (alive && pending.Count > 0)
                    {
                        // Client RTCP reports share this stream with RTSP requests.
                        if (pending[0] == 0x24)
                        {
                            if (pending.Count < 4) break;

                            byte channel = pending[1];
                            int payloadLength = (pending[2] << 8) | pending[3];
                            int frameLength = 4 + payloadLength;
                            if (pending.Count < frameLength) break;

                            long count = Interlocked.Increment(ref incomingInterleavedCount);
                            if (count <= 3 || count % 100 == 0)
                                LogUtils.debug($"[RTSP#{id}] incoming RTCP/interleaved channel={channel} len={payloadLength} count={count}");

                            pending.RemoveRange(0, frameLength);
                            continue;
                        }

                        int headerEnd = FindRtspHeaderEnd(pending);
                        if (headerEnd < 0)
                        {
                            if (pending.Count > MaxRtspMessageSize)
                                throw new InvalidDataException("RTSP header exceeds maximum size");
                            break;
                        }

                        string header = Encoding.ASCII.GetString(
                            pending.GetRange(0, headerEnd).ToArray());
                        int contentLength = ParseContentLength(header);
                        if (contentLength > MaxRtspMessageSize - headerEnd)
                            throw new InvalidDataException("RTSP message exceeds maximum size");

                        int requestLength = headerEnd + contentLength;
                        if (pending.Count < requestLength) break;

                        string req = Encoding.ASCII.GetString(
                            pending.GetRange(0, requestLength).ToArray());
                        pending.RemoveRange(0, requestLength);
                        HandleRequest(req);
                    }
                }
            }
            catch (Exception ex)
            {
                if (alive)
                    Console.Error.WriteLine($"[RTSP#{id}] read error: {ex.Message}");
            }
            finally { Close(); }
        }

        private static int FindRtspHeaderEnd(List<byte> pending)
        {
            for (int i = 0; i + 3 < pending.Count; i++)
            {
                if (pending[i] == '\r' && pending[i + 1] == '\n' &&
                    pending[i + 2] == '\r' && pending[i + 3] == '\n')
                    return i + 4;
            }

            return -1;
        }

        private static int ParseContentLength(string header)
        {
            foreach (string line in header.Split("\r\n", StringSplitOptions.None))
            {
                if (!line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase))
                    continue;

                string value = line[(line.IndexOf(':') + 1)..].Trim();
                if (!int.TryParse(value, out int length) || length < 0)
                    throw new InvalidDataException("Invalid RTSP Content-Length");
                return length;
            }

            return 0;
        }

        // ── Authentication ──────────────────────────────────
        private bool CheckAuth(string authHeader)
        {
            if (string.IsNullOrEmpty(authHeader)) return false;

            var headerParts = authHeader.Split(':', 2);
            if (headerParts.Length != 2) return false;

            string authValue = headerParts[1].Trim();
            if (!authValue.StartsWith("Basic ", StringComparison.OrdinalIgnoreCase))
                return false;

            try
            {
                string encoded = authValue["Basic ".Length..].Trim();
                string credential = Encoding.ASCII.GetString(Convert.FromBase64String(encoded));
                var parts = credential.Split(':', 2);

                if (parts.Length == 2 &&
                    parts[0] == server.Username &&
                    parts[1] == server.Password)
                {
                    return true;
                }
            }
            catch { }

            return false;
        }

        void HandleRequest(string req)
        {
            string[] lines = req.Split("\r\n", StringSplitOptions.None);
            if (lines.Length == 0) return;

            string method = lines[0].Split(' ')[0];
            string url = lines[0].Split(' ').ElementAtOrDefault(1) ?? "";
            string cseq = lines.FirstOrDefault(l => l.StartsWith("CSeq:", StringComparison.OrdinalIgnoreCase))
                                  ?.Split(':', 2)[1].Trim() ?? "0";
            string transport = lines.FirstOrDefault(l => l.StartsWith("Transport:", StringComparison.OrdinalIgnoreCase)) ?? "";
            string authHeader = lines.FirstOrDefault(l => l.StartsWith("Authorization:", StringComparison.OrdinalIgnoreCase)) ?? "";

            // Check authentication for methods other than OPTIONS (if secure)
            if (secure && method != "OPTIONS" && !authenticated)
            {
                if (!CheckAuth(authHeader))
                {
                    Send($"RTSP/1.0 401 Unauthorized\r\nCSeq: {cseq}\r\n" +
                         $"WWW-Authenticate: Basic realm=\"V380 Authentication\"\r\n\r\n");
                    return;
                }
                authenticated = true;
            }

            switch (method)
            {
                case "OPTIONS":
                    Reply(cseq, "Public: OPTIONS,DESCRIBE,SETUP,PLAY,GET_PARAMETER,SET_PARAMETER,TEARDOWN");
                    break;

                case "DESCRIBE":
                    {
                        string sdp = server.BuildSdp();
                        byte[] body = Encoding.ASCII.GetBytes(sdp);
                        Send($"RTSP/1.0 200 OK\r\nCSeq: {cseq}\r\n" +
                             $"Content-Type: application/sdp\r\nContent-Length: {body.Length}\r\n\r\n{sdp}");
                        break;
                    }

                case "SETUP":
                    {
                        bool isAudio = url.Contains("trackID=1", StringComparison.OrdinalIgnoreCase);
                        // Parse interleaved channels from client Transport header
                        // e.g. Transport: RTP/AVP/TCP;unicast;interleaved=0-1
                        byte ch = (byte)(isAudio ? 2 : 0);
                        var m = System.Text.RegularExpressions.Regex.Match(
                            transport, @"interleaved=(\d+)-(\d+)",
                            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
                        if (m.Success &&
                            byte.TryParse(m.Groups[1].Value, out byte requestedRtpChannel) &&
                            byte.TryParse(m.Groups[2].Value, out byte requestedRtcpChannel) &&
                            requestedRtcpChannel == requestedRtpChannel + 1)
                        {
                            ch = requestedRtpChannel;
                        }

                        if (isAudio)
                            audioCh = ch;
                        else
                            videoCh = ch;

                        Reply(cseq,
                            $"Transport: RTP/AVP/TCP;unicast;interleaved={ch}-{ch + 1}",
                            "Session: 1");
                        break;
                    }

                case "PLAY":
                    if (Reply(cseq,
                            "Session: 1",
                            $"RTP-Info: url={url}/trackID=0;seq={videoSeq},url={url}/trackID=1;seq={audioSeq}"))
                    {
                        playing = true;
                        Console.Error.WriteLine($"[RTSP#{id}] playing videoCh={videoCh} audioCh={audioCh}");
                    }
                    break;

                case "GET_PARAMETER":
                case "SET_PARAMETER":
                    LogUtils.debug($"[RTSP#{id}] {method}");
                    Reply(cseq, "Session: 1");
                    break;

                case "TEARDOWN":
                    Reply(cseq, "Session: 1");
                    Close();
                    break;

                default:
                    Send($"RTSP/1.0 501 Not Implemented\r\nCSeq: {cseq}\r\n\r\n");
                    break;
            }
        }

        bool Reply(string cseq, params string[] headers)
        {
            var sb = new StringBuilder();
            sb.Append($"RTSP/1.0 200 OK\r\nCSeq: {cseq}\r\n");
            foreach (var h in headers) sb.Append(h + "\r\n");
            sb.Append("\r\n");
            return Send(sb.ToString());
        }

        bool Send(string s)
        {
            byte[] b = Encoding.ASCII.GetBytes(s);
            return WriteToClient(b, "RTSP");
        }

        private bool WriteToClient(byte[] data, string kind)
        {
            if (!alive) return false;

            try
            {
                lock (sendLock)
                {
                    if (!alive) return false;
                    ns.Write(data, 0, data.Length);
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[RTSP#{id}] {kind} write error: {ex.Message}");
                Close();
                return false;
            }
        }

        // ── RTP video push  (H.264/H.265 Annex-B → RTP NAL/FU-A) ──────
        public void PushVideo(FrameData f)
        {
            if (!playing) return;

            uint rts = GetMonotonicVideoTimestamp();

            if (server.IsH265)
            {
                PushVideoH265(f.Payload, rts);
                return;
            }

            RtspServer.ParseNals(f.Payload, (nalType, nal) =>
            {
                const int MTU = 1400;
                if (nal.Length <= MTU)
                {
                    // Single NAL unit packet
                    SendRtp(videoCh, 96, videoSeq++, rts, videoSsrc, nal, 0, nal.Length, marker: true);
                }
                else
                {
                    // FU-A fragmentation
                    byte nalHdr = nal[0];
                    byte fuInd = (byte)((nalHdr & 0xE0) | 28); // NRI from original, type=28
                    int offset = 1; // skip original NAL header
                    bool first = true;

                    while (offset < nal.Length)
                    {
                        int chunk = Math.Min(MTU - 2, nal.Length - offset);
                        bool last = offset + chunk >= nal.Length;

                        byte fuHdr = (byte)(nalHdr & 0x1F);              // NAL type
                        if (first) fuHdr |= 0x80;                        // S bit
                        if (last) fuHdr |= 0x40;                        // E bit

                        var frag = new byte[2 + chunk];
                        frag[0] = fuInd;
                        frag[1] = fuHdr;
                        Array.Copy(nal, offset, frag, 2, chunk);

                        SendRtp(videoCh, 96, videoSeq++, rts, videoSsrc,
                                frag, 0, frag.Length, marker: last);
                        offset += chunk;
                        first = false;
                    }
                }
            });
        }

        // ── H.265 RTP push ──────────────────────────────────────────────
        void PushVideoH265(byte[] data, uint rts)
        {
            RtspServer.ParseNalsH265(data, (nalType, nal) =>
            {
                const int MTU = 1400;
                // Non-VCL NAL types (32+): VPS, SPS, PPS, AUD, etc. — send as single units
                if (nalType >= 32)
                {
                    if (nal.Length <= MTU)
                    {
                        SendRtp(videoCh, 96, videoSeq++, rts, videoSsrc, nal, 0, nal.Length, marker: false);
                    }
                    return;
                }

                if (nal.Length <= MTU)
                {
                    // Single NAL unit packet
                    SendRtp(videoCh, 96, videoSeq++, rts, videoSsrc, nal, 0, nal.Length, marker: true);
                }
                else
                {
                    // H.265 FU (Fragmentation Unit) - type 49
                    byte nalHdr0 = nal[0];
                    byte nalHdr1 = nal.Length > 1 ? nal[1] : (byte)0;
                    byte fuIndicator = (byte)((nalHdr0 & 0x81) | (49 << 1)); // type=49 FU
                    int offset = 2; // 2-byte H.265 NAL header
                    bool first = true;

                    while (offset < nal.Length)
                    {
                        int chunk = Math.Min(MTU - 3, nal.Length - offset);
                        bool last = offset + chunk >= nal.Length;

                        // FU header byte
                        byte fuHdr = (byte)(nalType & 0x3F);
                        if (first) fuHdr |= 0x80; // S bit
                        if (last) fuHdr |= 0x40;  // E bit

                        var frag = new byte[3 + chunk];
                        frag[0] = fuIndicator;
                        frag[1] = nalHdr1; // keep second header byte
                        frag[2] = fuHdr;
                        Array.Copy(nal, offset, frag, 3, chunk);

                        SendRtp(videoCh, 96, videoSeq++, rts, videoSsrc,
                                frag, 0, frag.Length, marker: last);
                        offset += chunk;
                        first = false;
                    }
                }
            });
        }

        // ── RTP audio push  (PCMA raw samples) ──────────────────
        public void PushAudio(FrameData f)
        {
            if (!playing || f.Payload == null || f.Payload.Length == 0) return;

            // PCMA has one sample per byte.  Normally timestamps advance by the
            // exact sample count.  After an upstream pause/reconnect, jump to
            // the shared wall-clock timeline so stale audio cannot accumulate.
            uint wallClockTimestamp = GetRtpTimestampNow(audioRtpBase, 8000);
            if (!audioClockInitialized)
            {
                nextAudioRtpTimestamp = wallClockTimestamp;
                audioClockInitialized = true;
            }
            else
            {
                int drift = unchecked((int)(wallClockTimestamp - nextAudioRtpTimestamp));
                if (drift > MaxAudioClockDriftSamples || drift < -MaxAudioClockDriftSamples)
                {
                    LogUtils.debug($"[RTSP#{id}] AUDIO clock resync driftSamples={drift}");
                    nextAudioRtpTimestamp = wallClockTimestamp;
                }
            }

            ushort seq = audioSeq;
            uint timestamp = nextAudioRtpTimestamp;
            if (SendRtp(audioCh, 8, seq, timestamp, audioSsrc,
                        f.Payload, 0, f.Payload.Length, marker: false))
            {
                audioSeq = unchecked((ushort)(audioSeq + 1));
                nextAudioRtpTimestamp = unchecked(nextAudioRtpTimestamp + (uint)f.Payload.Length);
            }
        }

        private uint GetMonotonicVideoTimestamp()
        {
            uint timestamp = GetRtpTimestampNow(videoRtpBase, 90000);

            // Two frames can occasionally be delivered in one burst.  Keep
            // their RTP timestamps strictly increasing without inventing a
            // fixed frame rate.
            if (videoClockInitialized && unchecked((int)(timestamp - lastVideoRtpTimestamp)) <= 0)
                timestamp = unchecked(lastVideoRtpTimestamp + 1);

            lastVideoRtpTimestamp = timestamp;
            videoClockInitialized = true;
            return timestamp;
        }

        private uint GetRtpTimestampNow(uint timestampBase, uint clockRate)
        {
            long elapsed = Stopwatch.GetTimestamp() - mediaClockOriginTicks;
            if (elapsed < 0) elapsed = 0;

            ulong wholeSeconds = (ulong)(elapsed / Stopwatch.Frequency);
            ulong remainder = (ulong)(elapsed % Stopwatch.Frequency);
            ulong clockTicks = wholeSeconds * clockRate +
                               remainder * clockRate / (ulong)Stopwatch.Frequency;
            return unchecked(timestampBase + (uint)clockTicks);
        }

        // ── Low-level RTP sender with RTSP interleaved framing ───
        // RFC 2326 §10.12:  $ | channel (1B) | length (2B BE) | RTP packet
        bool SendRtp(byte channel, byte pt, ushort seq, uint ts, uint ssrc,
                     byte[] payload, int offset, int length, bool marker)
        {
            if (offset < 0 || length < 0 || offset > payload.Length - length)
                throw new ArgumentOutOfRangeException(nameof(length));
            if (length > ushort.MaxValue - 12)
                throw new ArgumentOutOfRangeException(nameof(length), "RTP packet is too large for RTSP interleaving");

            var rtp = new byte[12 + length];
            rtp[0] = 0x80;
            rtp[1] = (byte)((marker ? 0x80 : 0) | (pt & 0x7F));
            rtp[2] = (byte)(seq >> 8);
            rtp[3] = (byte)seq;
            rtp[4] = (byte)(ts >> 24); rtp[5] = (byte)(ts >> 16);
            rtp[6] = (byte)(ts >> 8); rtp[7] = (byte)ts;
            rtp[8] = (byte)(ssrc >> 24); rtp[9] = (byte)(ssrc >> 16);
            rtp[10] = (byte)(ssrc >> 8); rtp[11] = (byte)ssrc;
            Array.Copy(payload, offset, rtp, 12, length);

            bool isAudio = pt == 8;
            if (!SendInterleaved(channel, rtp, isAudio ? "AUDIO RTP" : "VIDEO RTP"))
                return false;

            uint senderReportTimestamp;
            long count;
            long logInterval;
            if (isAudio)
            {
                audioRtcpPacketCount = unchecked(audioRtcpPacketCount + 1);
                audioRtcpOctetCount = unchecked(audioRtcpOctetCount + (uint)length);
                senderReportTimestamp = unchecked(ts + (uint)length);
                count = Interlocked.Increment(ref audioPacketCount);
                logInterval = 500;
            }
            else
            {
                videoRtcpPacketCount = unchecked(videoRtcpPacketCount + 1);
                videoRtcpOctetCount = unchecked(videoRtcpOctetCount + (uint)length);
                senderReportTimestamp = ts;
                count = Interlocked.Increment(ref videoPacketCount);
                logInterval = 1000;
            }

            if (count % logInterval == 0)
                LogUtils.debug($"[RTSP#{id}] {(isAudio ? "AUDIO" : "VIDEO")} seq={seq} ts={ts} len={length} count={count}");

            MaybeSendSenderReport(isAudio, senderReportTimestamp);

            return true;
        }

        private bool SendInterleaved(byte channel, byte[] payload, string kind)
        {
            if (payload.Length > ushort.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(payload), "Interleaved payload is too large");

            var frame = new byte[4 + payload.Length];
            frame[0] = 0x24; // '$'
            frame[1] = channel;
            frame[2] = (byte)(payload.Length >> 8);
            frame[3] = (byte)payload.Length;
            Array.Copy(payload, 0, frame, 4, payload.Length);
            return WriteToClient(frame, kind);
        }

        // RTCP Sender Reports map each track's independent RTP clock to the
        // same wall clock. VLC uses this mapping for stable A/V synchronization.
        private void MaybeSendSenderReport(bool isAudio, uint rtpTimestamp)
        {
            const long SenderReportIntervalSeconds = 5;
            long nowTicks = Stopwatch.GetTimestamp();
            long lastTicks = isAudio
                ? Volatile.Read(ref lastAudioSenderReportTicks)
                : Volatile.Read(ref lastVideoSenderReportTicks);

            if (lastTicks != 0 &&
                nowTicks - lastTicks < Stopwatch.Frequency * SenderReportIntervalSeconds)
                return;

            if (isAudio)
                Volatile.Write(ref lastAudioSenderReportTicks, nowTicks);
            else
                Volatile.Write(ref lastVideoSenderReportTicks, nowTicks);

            uint ssrc = isAudio ? audioSsrc : videoSsrc;
            uint packetCount = isAudio ? audioRtcpPacketCount : videoRtcpPacketCount;
            uint octetCount = isAudio ? audioRtcpOctetCount : videoRtcpOctetCount;
            byte channel = (byte)((isAudio ? audioCh : videoCh) + 1);
            byte[] report = BuildSenderReport(ssrc, rtpTimestamp, packetCount, octetCount);

            if (SendInterleaved(channel, report, isAudio ? "AUDIO RTCP" : "VIDEO RTCP"))
                LogUtils.debug($"[RTSP#{id}] RTCP SR {(isAudio ? "audio" : "video")} channel={channel} rtpTs={rtpTimestamp} packets={packetCount}");
        }

        private byte[] BuildSenderReport(uint ssrc, uint rtpTimestamp,
                                         uint packetCount, uint octetCount)
        {
            const int senderReportLength = 28;
            int sdesLength = (4 + 4 + 2 + rtcpCname.Length + 1 + 3) & ~3;
            var compound = new byte[senderReportLength + sdesLength];

            // Sender Report: V=2, RC=0, PT=200, length=6.
            compound[0] = 0x80;
            compound[1] = 200;
            WriteUInt16BigEndian(compound, 2, 6);
            WriteUInt32BigEndian(compound, 4, ssrc);

            long utcTicksSinceUnixEpoch = DateTime.UtcNow.Ticks - DateTime.UnixEpoch.Ticks;
            ulong ntpSeconds = (ulong)(utcTicksSinceUnixEpoch / TimeSpan.TicksPerSecond) + 2208988800UL;
            ulong ticksWithinSecond = (ulong)(utcTicksSinceUnixEpoch % TimeSpan.TicksPerSecond);
            uint ntpFraction = (uint)((ticksWithinSecond << 32) / TimeSpan.TicksPerSecond);

            WriteUInt32BigEndian(compound, 8, (uint)ntpSeconds);
            WriteUInt32BigEndian(compound, 12, ntpFraction);
            WriteUInt32BigEndian(compound, 16, rtpTimestamp);
            WriteUInt32BigEndian(compound, 20, packetCount);
            WriteUInt32BigEndian(compound, 24, octetCount);

            // SDES with a shared CNAME links audio and video SSRCs.
            int sdesOffset = senderReportLength;
            compound[sdesOffset] = 0x81; // V=2, source count=1
            compound[sdesOffset + 1] = 202;
            WriteUInt16BigEndian(compound, sdesOffset + 2,
                (ushort)(sdesLength / 4 - 1));
            WriteUInt32BigEndian(compound, sdesOffset + 4, ssrc);
            compound[sdesOffset + 8] = 1; // CNAME
            compound[sdesOffset + 9] = (byte)rtcpCname.Length;
            Array.Copy(rtcpCname, 0, compound, sdesOffset + 10, rtcpCname.Length);

            return compound;
        }

        private static void WriteUInt16BigEndian(byte[] buffer, int offset, ushort value)
        {
            buffer[offset] = (byte)(value >> 8);
            buffer[offset + 1] = (byte)value;
        }

        private static void WriteUInt32BigEndian(byte[] buffer, int offset, uint value)
        {
            buffer[offset] = (byte)(value >> 24);
            buffer[offset + 1] = (byte)(value >> 16);
            buffer[offset + 2] = (byte)(value >> 8);
            buffer[offset + 3] = (byte)value;
        }
    }
}
