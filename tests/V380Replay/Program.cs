using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json;
using V380Decoder.src;

if (args.Length == 1 && args[0] == "--self-test")
    return OfflineValidationChecks.Run();

if (args.Length < 1)
{
    Console.Error.WriteLine("usage: V380Replay <capture.v380dump> [--speed factor] [--rtsp-port port]");
    return 2;
}

string dumpPath = args[0];
double speed = 1.0;
int rtspPort = 8554;
bool validateOnly = false;
for (int i = 1; i < args.Length; i++)
{
    if (args[i] == "--validate-only")
        validateOnly = true;
    else if (args[i] == "--speed" && i + 1 < args.Length && double.TryParse(args[++i], CultureInfo.InvariantCulture, out double parsedSpeed))
        speed = parsedSpeed;
    else if (args[i] == "--rtsp-port" && i + 1 < args.Length && int.TryParse(args[++i], out int parsedPort))
        rtspPort = parsedPort;
    else
        throw new ArgumentException($"Unknown or invalid option {args[i]}");
}
if (!double.IsFinite(speed) || speed <= 0) throw new ArgumentOutOfRangeException(nameof(speed));

using var file = File.OpenRead(dumpPath);
byte[] fixedHeader = new byte[12];
file.ReadExactly(fixedHeader);
if (!fixedHeader.AsSpan(0, 8).SequenceEqual("V380DMP1"u8))
    throw new InvalidDataException("Not a V380DMP1 capture");
uint metadataLength = BinaryPrimitives.ReadUInt32LittleEndian(fixedHeader.AsSpan(8));
if (metadataLength > 4 * 1024 * 1024) throw new InvalidDataException("Capture metadata is too large");
byte[] metadataBytes = new byte[checked((int)metadataLength)];
file.ReadExactly(metadataBytes);
using JsonDocument metadata = JsonDocument.Parse(metadataBytes);
int version = metadata.RootElement.GetProperty("camera").GetProperty("device_version").GetInt32();
if (version > 30)
    throw new NotSupportedException("Replay currently accepts unencrypted (device version <= 30) captures.");

MediaMtxBridge? bridge = null;
ValidationSink? validation = null;
IMediaSink sink;
if (validateOnly)
    sink = validation = new ValidationSink();
else
{
    bridge = new MediaMtxBridge(rtspPort, false, string.Empty, string.Empty);
    bridge.Start();
    sink = bridge.CreateSink("live", includeAudio: true);
}
using (bridge)
using (validation)
{
var reassembler = new CaptureReassembler(sink, speed, realtime: !validateOnly);
byte[] recordHeader = new byte[12];
while (true)
{
    int headerBytes = ReadAtMost(file, recordHeader);
    if (headerBytes == 0) break;
    if (headerBytes != recordHeader.Length) throw new InvalidDataException("Truncated capture record header");
    ulong elapsedNs = BinaryPrimitives.ReadUInt64LittleEndian(recordHeader.AsSpan(0, 8));
    uint recordLength = BinaryPrimitives.ReadUInt32LittleEndian(recordHeader.AsSpan(8, 4));
    if (recordLength == 0 || recordLength > 64 * 1024 * 1024)
        throw new InvalidDataException($"Invalid capture record length {recordLength}");
    byte[] bytes = new byte[checked((int)recordLength)];
    file.ReadExactly(bytes);
    reassembler.WaitUntil(elapsedNs);
    reassembler.Push(bytes);
}
reassembler.Finish();
validation?.Finish(reassembler.VideoFrames, reassembler.AudioFrames);
Console.Error.WriteLine($"[REPLAY] complete video={reassembler.VideoFrames} audio={reassembler.AudioFrames} speed={speed:F2}x");
return 0;
}

static int ReadAtMost(Stream input, byte[] buffer)
{
    int total = 0;
    while (total < buffer.Length)
    {
        int read = input.Read(buffer, total, buffer.Length - total);
        if (read == 0) break;
        total += read;
    }
    return total;
}

sealed class CaptureReassembler(IMediaSink sink, double speed, bool realtime)
{
    private readonly Stopwatch clock = Stopwatch.StartNew();
    private readonly List<byte> buffered = new();
    private readonly Dictionary<byte, FrameAssembly> frames = new();
    private int offset;
    private int epoch;
    private long arrivalTimestamp;
    public int VideoFrames { get; private set; }
    public int AudioFrames { get; private set; }

    public void WaitUntil(ulong elapsedNs)
    {
        arrivalTimestamp = checked((long)(elapsedNs / 1_000_000_000.0 * Stopwatch.Frequency));
        if (!realtime) return;
        long dueTicks = checked((long)(elapsedNs / speed / 100.0));
        TimeSpan remaining = TimeSpan.FromTicks(dueTicks) - clock.Elapsed;
        if (remaining > TimeSpan.FromMilliseconds(1)) Thread.Sleep(remaining - TimeSpan.FromMilliseconds(1));
        while (clock.Elapsed < TimeSpan.FromTicks(dueTicks)) Thread.SpinWait(32);
    }

    public void Push(byte[] bytes)
    {
        buffered.AddRange(bytes);
        while (buffered.Count - offset >= 12)
        {
            ReadOnlySpan<byte> span = CollectionsMarshal.AsSpan(buffered);
            if (span[offset] != 0x7f)
                throw new InvalidDataException($"Capture framing lost at byte {offset}");
            byte type = span[offset + 1];
            ushort total = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(offset + 3, 2));
            ushort current = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(offset + 5, 2));
            ushort length = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(offset + 7, 2));
            if (length == 0 || total == 0 || current >= total)
                throw new InvalidDataException("Invalid V380 fragment in capture");
            if (buffered.Count - offset < 12 + length) break;
            byte[] payload = span.Slice(offset + 12, length).ToArray();
            offset += 12 + length;
            ProcessFragment(type, total, current, payload);
        }
        if (offset > 0 && (offset > 64 * 1024 || offset == buffered.Count))
        {
            buffered.RemoveRange(0, offset);
            offset = 0;
        }
    }

    private void ProcessFragment(byte type, ushort total, ushort current, byte[] payload)
    {
        if (type is not (0x00 or 0x01 or 0x28 or 0x29 or 0x16 or 0x1a)) return;
        if (!frames.TryGetValue(type, out FrameAssembly? frame))
            frames[type] = frame = new FrameAssembly();
        if (current == 0) frame.Reset(total);
        if (frame.Total != total || frame.Next != current)
        {
            foreach (FrameAssembly pending in frames.Values) pending.Reset(0);
            sink.Reset();
            epoch++;
            return;
        }
        frame.Data.Write(payload);
        frame.Next++;
        if (frame.Next != total) return;
        byte[] complete = frame.Data.ToArray();
        frame.Reset(0);
        if (complete.Length < 16) return;
        var data = new FrameData
        {
            RawType = type,
            FrameId = BinaryPrimitives.ReadUInt32LittleEndian(complete.AsSpan(0, 4)),
            FrameType = BinaryPrimitives.ReadUInt16LittleEndian(complete.AsSpan(4, 2)),
            FrameRate = BinaryPrimitives.ReadUInt16LittleEndian(complete.AsSpan(6, 2)),
            Timestamp = BinaryPrimitives.ReadUInt64LittleEndian(complete.AsSpan(8, 8)),
            ArrivalTimestamp = arrivalTimestamp,
            Epoch = epoch,
            Payload = complete.AsSpan(16).ToArray()
        };
        if (type is 0x16 or 0x1a)
        {
            sink.PushAudio(data);
            AudioFrames++;
        }
        else
        {
            sink.PushVideo(data);
            VideoFrames++;
        }
    }

    public void Finish()
    {
        if (buffered.Count != offset)
            throw new InvalidDataException("Capture ended with a partial V380 fragment");
        if (frames.Values.Any(frame => frame.Data.Length != 0))
            throw new InvalidDataException("Capture ended with an incomplete media frame");
    }

    private sealed class FrameAssembly
    {
        public ushort Total;
        public ushort Next;
        public MemoryStream Data = new();
        public void Reset(ushort total)
        {
            Data.Dispose();
            Data = new MemoryStream();
            Total = total;
            Next = 0;
        }
    }
}

sealed class ValidationSink : IMediaSink, IDisposable
{
    private readonly MediaMtxBridge bridge;
    private readonly IMediaSink sink;
    private ulong? audioPts;
    private ulong? videoPts;
    private long outputAudio;
    private long outputVideo;
    private long outputSamples;
    private long expectedSamples;
    private long resets;

    public ValidationSink()
    {
        bridge = MediaMtxBridge.CreateOfflineValidation(Observe);
        sink = bridge.CreateSink("live");
    }

    private void Observe(ValidatedMediaPacket packet)
    {
        ulong? previous = packet.Audio ? audioPts : videoPts;
        if (previous.HasValue && packet.Pts <= previous.Value)
            throw new InvalidDataException("Production bridge emitted non-monotonic PTS");
        if (packet.Audio)
        {
            audioPts = packet.Pts;
            long count = packet.AudioFormat switch
            {
                "pcm_s16le" => packet.Payload.Length / 2,
                "alaw" => packet.Payload.Length,
                _ => throw new InvalidDataException("Offline validation requires the production in-process decoder")
            };
            if ((packet.AudioFormat == "pcm_s16le" && packet.Payload.Length % 2 != 0) ||
                packet.Duration != (ulong)count * 125_000)
                throw new InvalidDataException("Production decoder sample count/duration mismatch");
            outputSamples += count;
            outputAudio++;
        }
        else { videoPts = packet.Pts; outputVideo++; }
    }

    public void PushVideo(FrameData frame) { sink.PushVideo(frame); bridge.DrainValidation(); }
    public void PushAudio(FrameData frame)
    {
        expectedSamples += frame.RawType == 0x16 ? 1 + (frame.Payload.Length - 4) * 2 : frame.Payload.Length;
        sink.PushAudio(frame);
        bridge.DrainValidation();
    }
    public void Reset()
    {
        sink.Reset();
        audioPts = videoPts = null;
        resets++;
    }
    public void Finish(int video, int audio)
    {
        bridge.DrainValidation();
        ValidationMediaState state = bridge.ValidationState;
        if (outputAudio != audio || outputVideo != video ||
            state.QueuedAudio != 0 || state.QueuedVideo != 0 || state.Resets != resets)
            throw new InvalidDataException($"Production replay count mismatch: input={video}/{audio} output={outputVideo}/{outputAudio} resets={resets}");
        if (outputSamples != expectedSamples)
            throw new InvalidDataException($"Replay sample total mismatch: expected={expectedSamples} output={outputSamples}");
        Console.WriteLine($"offline production validation passed: video={outputVideo} audio={outputAudio} samples={outputSamples} resets={resets} publishers={state.PublisherStarts}");
    }
    public void Dispose() => bridge.Dispose();
}
