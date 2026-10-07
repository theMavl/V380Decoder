namespace V380Decoder.src;

/// <summary>
/// Generates audio PTS from samples. Camera timestamps are diagnostic input:
/// V380 batches several 505-sample blocks under nearly identical timestamps.
/// </summary>
public sealed class AudioSampleClock
{
    private const int SampleRate = 8000;
    private const long NanosecondsPerSecond = 1_000_000_000;
    private const int DriftWindowFrames = 80; // about five seconds for 505-sample blocks
    private readonly ulong originTimestampMs;
    private readonly Queue<long> clockErrors = new();
    private ulong startPts;
    private ulong lastPts;
    private ulong lastCameraTimestampMs;
    private uint lastFrameId;
    private long samples;
    private long errorSum;
    private long baselineErrorNs;
    private int sustainedDriftFrames;
    private bool hasFrameId;
    private bool started;
    private bool baselineReady;
    private const uint MinimumMissingAudioGap = 6;

    public long TimelineSamples => samples;
    public long FilteredErrorNs { get; private set; }
    public long DriftFromBaselineNs { get; private set; }
    public ulong LastPts => lastPts;

    public AudioSampleClock(ulong originTimestampMs)
    {
        this.originTimestampMs = originTimestampMs;
    }

    public AudioTiming Next(uint frameId, ulong cameraTimestampMs, int sampleCount)
    {
        if (sampleCount <= 0)
            throw new InvalidDataException("Audio frame contains no samples");
        bool discontinuity = started && cameraTimestampMs < lastCameraTimestampMs;
        if (started)
        {
            if (hasFrameId && frameId == lastFrameId)
                throw new InvalidDataException("Duplicate audio frame ID");
            if (hasFrameId && frameId < lastFrameId &&
                !(lastFrameId > uint.MaxValue - 128 && frameId < 128))
                throw new InvalidDataException("Backward audio frame ID");
            if (hasFrameId)
            {
                uint step = unchecked(frameId - lastFrameId);
                // These are global media IDs. The checked capture and the
                // generated regression fixture show audio steps of 3 or 4;
                // losing one audio block therefore produces a step of at least
                // 6 (two adjacent global-ID intervals). Detect that directly,
                // including during bootstrap, rather than learning a min/max
                // ratio that can absorb an early gap before seeing step 3.
                if (step >= MinimumMissingAudioGap)
                    throw new InvalidDataException("Uncertain audio continuity: frame cadence gap; reset epoch");
            }
            // A 0-140 ms delta is normal for this camera's timestamp batching.
            if ((cameraTimestampMs > lastCameraTimestampMs &&
                 cameraTimestampMs - lastCameraTimestampMs > 1000) ||
                (cameraTimestampMs < lastCameraTimestampMs &&
                 lastCameraTimestampMs - cameraTimestampMs > 1000))
                throw new InvalidDataException("Large camera audio timestamp jump");
        }

        ulong sourcePts = cameraTimestampMs >= originTimestampMs
            ? checked((cameraTimestampMs - originTimestampMs) * 1_000_000UL)
            : 0;
        if (!started)
        {
            startPts = sourcePts;
            started = true;
        }

        ulong pts = checked(startPts + SamplesToNanoseconds(samples));
        ulong end = checked(startPts + SamplesToNanoseconds(checked(samples + sampleCount)));
        if (samples > 0 && pts <= lastPts)
            throw new InvalidDataException("Non-increasing audio output PTS");

        long error = sourcePts >= pts
            ? (long)Math.Min(sourcePts - pts, (ulong)long.MaxValue)
            : -(long)Math.Min(pts - sourcePts, (ulong)long.MaxValue);
        clockErrors.Enqueue(error);
        errorSum = checked(errorSum + error);
        if (clockErrors.Count > DriftWindowFrames)
            errorSum -= clockErrors.Dequeue();
        FilteredErrorNs = errorSum / clockErrors.Count;
        if (clockErrors.Count == DriftWindowFrames)
        {
            if (!baselineReady)
            {
                baselineErrorNs = FilteredErrorNs;
                baselineReady = true;
            }
            DriftFromBaselineNs = FilteredErrorNs - baselineErrorNs;
            sustainedDriftFrames = Math.Abs(DriftFromBaselineNs) > 250_000_000
                ? sustainedDriftFrames + 1 : 0;
            if (sustainedDriftFrames >= 32)
                throw new InvalidDataException("Sustained camera/sample clock divergence");
        }

        lastPts = pts;
        lastCameraTimestampMs = cameraTimestampMs;
        lastFrameId = frameId;
        hasFrameId = true;
        long firstSample = samples;
        samples = checked(samples + sampleCount);
        return new AudioTiming(pts, end - pts, firstSample, error,
            FilteredErrorNs, DriftFromBaselineNs, discontinuity);
    }

    private static ulong SamplesToNanoseconds(long value)
    {
        long seconds = value / SampleRate;
        long remainder = value % SampleRate;
        return checked((ulong)(seconds * NanosecondsPerSecond +
            remainder * NanosecondsPerSecond / SampleRate));
    }
}

public readonly record struct AudioTiming(
    ulong Pts, ulong Duration, long FirstSample, long SourceErrorNs,
    long FilteredErrorNs, long DriftFromBaselineNs, bool Discontinuity);
