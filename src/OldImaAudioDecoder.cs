using System.Diagnostics;

namespace V380Decoder.src
{
    // prsyahmi/v380 does not decode 0x16 audio itself: it strips the
    // 20-byte V380 header and feeds one continuous stream to FFmpeg's
    // adpcm_ima_ws decoder. Keep the FFmpeg process alive so codec state is
    // preserved between camera frames.
    internal sealed class OldImaAudioDecoder : IDisposable
    {
        private const int RtpPacketSamples = 160;
        private const int MaxQueuedSamples = 8000;    // one second
        private const int ResumeQueuedSamples = 4000; // half a second

        private readonly Action<byte[]> onDecodedPcma;
        private readonly Process process;
        private readonly Stream input;
        private readonly object inputLock = new();
        private readonly ManualResetEventSlim stopSignal = new(false);
        private readonly Thread outputThread;
        private readonly Thread errorThread;
        private long inputSampleCount;
        private long outputSampleCount;
        private int outputFailed;
        private int disposed;

        public OldImaAudioDecoder(Action<byte[]> onDecodedPcma)
        {
            this.onDecodedPcma = onDecodedPcma;

            var startInfo = new ProcessStartInfo
            {
                FileName = "ffmpeg",
                UseShellExecute = false,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            AddArguments(startInfo,
                "-hide_banner", "-nostats", "-loglevel", "warning",
                // This is a raw stream with all parameters already known.
                // Do not buffer seconds of camera audio for stream probing.
                "-probesize", "32", "-analyzeduration", "0",
                "-f", "s16le", "-ar", "8000", "-ac", "1",
                "-c:a", "adpcm_ima_ws", "-i", "pipe:0",
                "-map", "0:a:0", "-c:a", "pcm_alaw",
                "-ar", "8000", "-ac", "1", "-f", "alaw",
                "-flush_packets", "1", "pipe:1");

            process = new Process { StartInfo = startInfo };
            if (!process.Start())
                throw new InvalidOperationException("Failed to start FFmpeg ADPCM decoder");

            input = process.StandardInput.BaseStream;
            outputThread = new Thread(PumpOutput)
            {
                IsBackground = true,
                Name = "v380-old-audio-out"
            };
            errorThread = new Thread(PumpErrors)
            {
                IsBackground = true,
                Name = "v380-old-audio-err"
            };
            outputThread.Start();
            errorThread.Start();
            Console.Error.WriteLine("[AUDIO-OLD] FFmpeg adpcm_ima_ws decoder started");
        }

        public bool WriteFrame(byte[] frame, int headerSize)
        {
            if (frame == null || frame.Length <= headerSize)
                return true;
            if (Volatile.Read(ref disposed) != 0 || Volatile.Read(ref outputFailed) != 0)
                return false;

            try
            {
                lock (inputLock)
                {
                    if (disposed != 0 || process.HasExited)
                        return false;

                    int encodedLength = frame.Length - headerSize;
                    // adpcm_ima_ws expands every byte into two PCM samples.
                    // Count before Write so the output thread cannot observe
                    // decoded data before its input has been accounted for.
                    Interlocked.Add(ref inputSampleCount, encodedLength * 2L);
                    try
                    {
                        input.Write(frame, headerSize, encodedLength);
                    }
                    catch
                    {
                        Interlocked.Add(ref inputSampleCount, -encodedLength * 2L);
                        throw;
                    }
                    input.Flush();
                }
                return true;
            }
            catch (Exception ex)
            {
                if (Volatile.Read(ref disposed) == 0)
                    Console.Error.WriteLine($"[AUDIO-OLD] FFmpeg input error: {ex.Message}");
                return false;
            }
        }

        private void PumpOutput()
        {
            var packet = new byte[RtpPacketSamples];
            int packetLength = 0;
            long nextPacketDueTicks = 0;
            bool droppingBacklog = false;

            try
            {
                Stream output = process.StandardOutput.BaseStream;
                while (Volatile.Read(ref disposed) == 0)
                {
                    int read = output.Read(packet, packetLength, packet.Length - packetLength);
                    if (read <= 0)
                    {
                        if (Volatile.Read(ref disposed) == 0)
                            Interlocked.Exchange(ref outputFailed, 1);
                        break;
                    }

                    packetLength += read;
                    if (packetLength != packet.Length) continue;

                    long samplesRead = Interlocked.Add(ref outputSampleCount, packet.Length);
                    long queuedSamples = Volatile.Read(ref inputSampleCount) - samplesRead;

                    // If the camera clock is slightly faster than the nominal
                    // 8 kHz rate, an unlimited pipe queue turns that difference
                    // into ever-growing A/V delay.  Drop only stale, already
                    // decoded audio and keep at most a small bounded cushion.
                    if (!droppingBacklog && queuedSamples > MaxQueuedSamples)
                    {
                        droppingBacklog = true;
                        Console.Error.WriteLine(
                            $"[AUDIO-OLD] dropping stale audio backlog queuedSamples={queuedSamples}");
                    }

                    if (droppingBacklog)
                    {
                        packetLength = 0;
                        nextPacketDueTicks = 0;
                        if (queuedSamples <= ResumeQueuedSamples)
                            droppingBacklog = false;
                        continue;
                    }

                    if (!WaitForPacketTime(ref nextPacketDueTicks))
                        break;

                    if (Volatile.Read(ref disposed) == 0)
                        onDecodedPcma((byte[])packet.Clone());
                    packetLength = 0;
                }
            }
            catch (Exception ex)
            {
                Interlocked.Exchange(ref outputFailed, 1);
                if (Volatile.Read(ref disposed) == 0)
                    Console.Error.WriteLine($"[AUDIO-OLD] FFmpeg output error: {ex.Message}");
            }
        }

        private bool WaitForPacketTime(ref long nextPacketDueTicks)
        {
            long now = Stopwatch.GetTimestamp();
            long packetTicks = Stopwatch.Frequency * RtpPacketSamples / 8000;
            long maxLatenessTicks = Stopwatch.Frequency / 4;

            if (nextPacketDueTicks == 0 || now - nextPacketDueTicks > maxLatenessTicks)
                nextPacketDueTicks = now;

            while (nextPacketDueTicks > now)
            {
                double seconds = (double)(nextPacketDueTicks - now) / Stopwatch.Frequency;
                if (stopSignal.Wait(TimeSpan.FromSeconds(seconds)))
                    return false;
                now = Stopwatch.GetTimestamp();
            }

            nextPacketDueTicks += packetTicks;
            return Volatile.Read(ref disposed) == 0;
        }

        private void PumpErrors()
        {
            try
            {
                string line;
                while ((line = process.StandardError.ReadLine()) != null)
                    Console.Error.WriteLine($"[AUDIO-OLD] ffmpeg: {line}");
            }
            catch (Exception ex)
            {
                if (Volatile.Read(ref disposed) == 0)
                    Console.Error.WriteLine($"[AUDIO-OLD] FFmpeg stderr error: {ex.Message}");
            }
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0) return;
            stopSignal.Set();

            lock (inputLock)
            {
                try { input.Close(); } catch { }
            }

            try
            {
                if (!process.HasExited)
                    process.Kill(entireProcessTree: true);
            }
            catch { }

            try { process.WaitForExit(2000); } catch { }
            process.Dispose();
        }

        private static void AddArguments(ProcessStartInfo startInfo, params string[] arguments)
        {
            foreach (string argument in arguments)
                startInfo.ArgumentList.Add(argument);
        }
    }
}
