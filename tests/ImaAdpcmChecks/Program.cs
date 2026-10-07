using System.Buffers.Binary;
using System.Globalization;
using V380Decoder.src;

if (args.Length == 1 && args[0] == "--clock")
    return RunClockChecks();

if (args.Length == 2 && args[0] == "--timestamps")
    return RunTimestampChecks(args[1]);

if (args.Length != 2)
{
    Console.Error.WriteLine("usage: ImaAdpcmChecks <concatenated-ima-blocks> <reference-pcm-wav> | --timestamps <audio-frames.csv>");
    return 2;
}

byte[] blocks = File.ReadAllBytes(args[0]);
byte[] referenceWave = File.ReadAllBytes(args[1]);
byte[] reference = ReadWaveData(referenceWave);
using var actual = new MemoryStream();
if (blocks.Length % 256 != 0)
    throw new InvalidDataException("The reference capture must contain complete 256-byte IMA blocks");
for (int offset = 0; offset < blocks.Length; offset += 256)
{
    short[] decoded = ImaAdpcmDecoder.Decode(blocks.AsSpan(offset, 256));
    byte[] encoded = new byte[decoded.Length * sizeof(short)];
    for (int i = 0; i < decoded.Length; i++)
        BinaryPrimitives.WriteInt16LittleEndian(encoded.AsSpan(i * 2, 2), decoded[i]);
    actual.Write(encoded);
}

byte[] output = actual.ToArray();
if (!output.AsSpan().SequenceEqual(reference))
{
    int mismatch = FirstMismatch(output, reference);
    throw new InvalidDataException($"PCM differs from the FFmpeg reference at byte {mismatch}");
}

AssertThrows<InvalidDataException>(() => ImaAdpcmDecoder.Decode(new byte[3]));
AssertThrows<InvalidDataException>(() => ImaAdpcmDecoder.Decode([0, 0, 89, 0]));
AssertThrows<InvalidDataException>(() => ImaAdpcmDecoder.Decode([0, 0, 0, 1]));
if (ImaAdpcmDecoder.Decode([0, 0, 0, 0]).Length != 1)
    throw new InvalidDataException("A header-only block must emit its predictor sample");

Console.WriteLine($"matched {output.Length / 2} PCM samples across {blocks.Length / 256} blocks");
return 0;

static byte[] ReadWaveData(byte[] wave)
{
    if (wave.Length < 12 || !wave.AsSpan(0, 4).SequenceEqual("RIFF"u8) ||
        !wave.AsSpan(8, 4).SequenceEqual("WAVE"u8))
        throw new InvalidDataException("Invalid WAV reference");
    int offset = 12;
    while (offset + 8 <= wave.Length)
    {
        int length = checked((int)BinaryPrimitives.ReadUInt32LittleEndian(wave.AsSpan(offset + 4, 4)));
        int start = offset + 8;
        if (length < 0 || start + length > wave.Length)
            throw new InvalidDataException("Truncated WAV chunk");
        if (wave.AsSpan(offset, 4).SequenceEqual("data"u8))
            return wave.AsSpan(start, length).ToArray();
        offset = start + length + (length & 1);
    }
    throw new InvalidDataException("WAV data chunk is missing");
}

static int FirstMismatch(byte[] left, byte[] right)
{
    int limit = Math.Min(left.Length, right.Length);
    for (int i = 0; i < limit; i++)
        if (left[i] != right[i]) return i;
    return limit;
}

static void AssertThrows<T>(Action action) where T : Exception
{
    try { action(); }
    catch (T) { return; }
    throw new InvalidDataException($"Expected {typeof(T).Name}");
}

static int RunTimestampChecks(string path)
{
    string[] rows = File.ReadAllLines(path);
    if (rows.Length != 651)
        throw new InvalidDataException($"Expected 650 captured audio frames; found {rows.Length - 1}");
    var first = ParseTimestampRow(rows[1]);
    var clock = new AudioSampleClock(first.TimestampMs);
    ulong previousPts = 0;
    long totalSamples = 0;
    int batchedDeltas = 0;
    int normalBatchJumps = 0;
    ulong previousCameraTimestamp = first.TimestampMs;
    long maxAbsoluteSourceError = 0;
    foreach (string row in rows.Skip(1))
    {
        var item = ParseTimestampRow(row);
        AudioTiming timing = clock.Next(item.FrameId, item.TimestampMs, 505);
        if (timing.Duration != 63_125_000)
            throw new InvalidDataException($"Unexpected frame duration {timing.Duration} ns");
        if (totalSamples > 0 && timing.Pts <= previousPts)
            throw new InvalidDataException("Output PTS did not strictly increase");
        previousPts = timing.Pts;
        totalSamples += 505;
        maxAbsoluteSourceError = Math.Max(maxAbsoluteSourceError, Math.Abs(timing.SourceErrorNs));
        if (totalSamples > 505)
        {
            ulong delta = item.TimestampMs >= previousCameraTimestamp
                ? item.TimestampMs - previousCameraTimestamp
                : previousCameraTimestamp - item.TimestampMs;
            if (delta <= 1) batchedDeltas++;
            if (delta is >= 115 and <= 140) normalBatchJumps++;
        }
        previousCameraTimestamp = item.TimestampMs;
    }
    if (totalSamples != 328_250 || clock.TimelineSamples != 328_250)
        throw new InvalidDataException($"Expected 328250 samples; got {totalSamples}");
    if (clock.LastPts + 63_125_000 != 41_031_250_000UL)
        throw new InvalidDataException("The captured sample timeline must span exactly 41.03125 seconds");
    if (maxAbsoluteSourceError > 250_000_000)
        throw new InvalidDataException($"Camera/sample error exceeded 250 ms: {maxAbsoluteSourceError} ns");
    if (batchedDeltas == 0 || normalBatchJumps == 0)
        throw new InvalidDataException("The capture no longer exercises both timestamp batching patterns");

    var batchingClock = new AudioSampleClock(1000);
    batchingClock.Next(1, 1000, 505);
    batchingClock.Next(4, 1000, 505);
    batchingClock.Next(7, 1130, 505);
    AudioTiming beforeBackward = batchingClock.Next(10, 1129, 505);
    AudioTiming afterBackward = batchingClock.Next(13, 1128, 505);
    if (afterBackward.Pts <= beforeBackward.Pts || !afterBackward.Discontinuity)
        throw new InvalidDataException("Backward camera time must retain monotonic sample PTS and mark DISCONT");
    var brokenClock = new AudioSampleClock(1000);
    brokenClock.Next(1, 1000, 505);
    try
    {
        brokenClock.Next(4, 2501, 505);
        throw new InvalidDataException("A confirmed >1 second camera timestamp jump was accepted");
    }
    catch (InvalidDataException ex) when (ex.Message == "Large camera audio timestamp jump") { }

    Console.WriteLine($"timestamp regression passed: frames=650 samples={totalSamples} " +
        $"timeline=41.03125s max_source_error={maxAbsoluteSourceError / 1_000_000.0:F1}ms " +
        $"batched_deltas={batchedDeltas} batch_jumps={normalBatchJumps} " +
        $"filtered_error={clock.FilteredErrorNs / 1_000_000.0:F1}ms " +
        $"drift={clock.DriftFromBaselineNs / 1_000_000.0:F1}ms");
    return 0;
}

static int RunClockChecks()
{
    const ulong origin = 1000;
    var normal = new AudioSampleClock(origin);
    uint id = 1;
    for (int i = 0; i < 650; i++)
    {
        ulong timestamp = origin + (ulong)(i / 2 * 126.25);
        AudioTiming timing = normal.Next(id, timestamp, 505);
        if (timing.Pts != (ulong)i * 63_125_000 || timing.Discontinuity)
            throw new InvalidDataException("Normal 3/4 global IDs and batched timestamps changed the sample timeline");
        id += (uint)(i % 2 == 0 ? 3 : 4);
    }
    var jitteredCadence = new AudioSampleClock(origin);
    jitteredCadence.Next(100, origin, 505);
    jitteredCadence.Next(105, origin + 63, 505);
    if (jitteredCadence.TimelineSamples != 1010)
        throw new InvalidDataException("A global FrameId step of 5 must remain accepted");
    var wrapped = new AudioSampleClock(origin);
    wrapped.Next(uint.MaxValue - 2, origin, 505);
    wrapped.Next(1, origin + 63, 505); // unchecked forward distance is 4
    wrapped.Next(4, origin + 126, 505);
    if (wrapped.TimelineSamples != 1515)
        throw new InvalidDataException("A small forward FrameId step across uint wrap was rejected");
    var wrappedThroughZero = new AudioSampleClock(origin);
    wrappedThroughZero.Next(uint.MaxValue, origin, 505);
    wrappedThroughZero.Next(0, origin + 63, 505);
    wrappedThroughZero.Next(3, origin + 126, 505);
    if (wrappedThroughZero.TimelineSamples != 1515)
        throw new InvalidDataException("The uint.MaxValue -> 0 FrameId wrap was rejected");
    // Inject loss before, during and after baseline. Every reset discards the
    // old epoch; its fresh clock anchors to source time, never to fewer samples.
    foreach (int lost in new[] { 1, 2, 10, 79, 81, 160 })
    {
        var clock = new AudioSampleClock(origin);
        id = 1;
        bool reset = false;
        for (int i = 0; i < lost + 6; i++)
        {
            ulong timestamp = origin + (ulong)(i / 2 * 126.25);
            if (i != lost)
            {
                try { clock.Next(id, timestamp, 505); }
                catch (InvalidDataException ex) when (ex.Message.StartsWith("Uncertain audio continuity"))
                {
                    reset = true;
                    clock = new AudioSampleClock(origin);
                    AudioTiming fresh = clock.Next(id, timestamp, 505);
                    if (fresh.Pts != (timestamp - origin) * 1_000_000 || fresh.FirstSample != 0)
                        throw new InvalidDataException("Reset must reanchor to source time without accumulated A/V offset");
                }
            }
            id += (uint)(i % 2 == 0 ? 3 : 4);
        }
        if (!reset) throw new InvalidDataException($"Missing block {lost} was absorbed into baseline");
    }
    // Exact one-block loss vectors: two valid global-ID periods collapse into
    // a single observed step of 6, 7, or 8. Check both bootstrap and warmed-up
    // clocks, then model the production reconnect/reanchor at the current
    // source timestamp and prove subsequent PTS remains source-anchored.
    foreach (uint gap in new uint[] { 6, 7, 8 })
    foreach (bool warmedUp in new[] { false, true })
    {
        var clock = new AudioSampleClock(origin);
        uint previousId = 100;
        ulong currentTimestamp = origin;
        clock.Next(previousId, currentTimestamp, 505);
        if (warmedUp)
        {
            for (int i = 0; i < 80; i++)
            {
                previousId += (uint)(i % 2 == 0 ? 3 : 4);
                currentTimestamp += (ulong)(i % 2 == 0 ? 63 : 64);
                clock.Next(previousId, currentTimestamp, 505);
            }
        }
        uint lostFrameId = previousId + gap;
        currentTimestamp += 127;
        long samplesBeforeGap = clock.TimelineSamples;
        try
        {
            clock.Next(lostFrameId, currentTimestamp, 505);
            throw new InvalidDataException($"Missing audio block gap {gap} was accepted (warmed={warmedUp})");
        }
        catch (InvalidDataException ex) when (ex.Message.StartsWith("Uncertain audio continuity")) { }
        if (clock.TimelineSamples != samplesBeforeGap)
            throw new InvalidDataException("A rejected missing block advanced the sample clock");

        clock = new AudioSampleClock(origin);
        AudioTiming reanchored = clock.Next(lostFrameId, currentTimestamp, 505);
        AudioTiming following = clock.Next(lostFrameId + 3, currentTimestamp + 63, 505);
        ulong expectedAnchor = (currentTimestamp - origin) * 1_000_000;
        if (reanchored.FirstSample != 0 || reanchored.Pts != expectedAnchor ||
            following.Pts != expectedAnchor + 63_125_000 || clock.TimelineSamples != 1010)
            throw new InvalidDataException("Missing-block reset did not reanchor without cumulative A/V offset");
    }
    var duplicate = new AudioSampleClock(origin);
    duplicate.Next(10, origin, 505);
    AssertThrows<InvalidDataException>(() => duplicate.Next(10, origin, 505));
    if (duplicate.TimelineSamples != 505) throw new InvalidDataException("Duplicate advanced timeline");
    duplicate.Next(13, origin, 505);
    long beforeBackward = duplicate.TimelineSamples;
    AssertThrows<InvalidDataException>(() => duplicate.Next(12, origin + 1, 505));
    if (duplicate.TimelineSamples != beforeBackward)
        throw new InvalidDataException("Backward ID advanced the sample clock");
    Console.WriteLine("clock checks passed: batching, global 3/4 IDs, duplicate/backward, early/late loss and epoch source reanchor");
    return 0;
}

static (uint FrameId, ulong TimestampMs) ParseTimestampRow(string csv)
{
    string[] columns = csv.Split(',');
    uint frameId = uint.Parse(columns[5], CultureInfo.InvariantCulture);
    ulong timestampMs = ulong.Parse(columns[8], CultureInfo.InvariantCulture);
    return (frameId, timestampMs);
}
