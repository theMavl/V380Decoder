using V380Decoder.src;

static class OfflineValidationChecks
{
    public static int Run()
    {
        var emitted = new List<ValidatedMediaPacket>();
        using var bridge = MediaMtxBridge.CreateOfflineValidation(emitted.Add);
        IMediaSink sink = bridge.CreateSink("live");
        sink.PushVideo(Video(0, true));
        sink.PushAudio(Audio(0));
        bridge.DrainValidation();
        Assert(emitted.Count == 2 && emitted.Single(p => p.Audio).Payload.Length == 1010,
            "production decoder must emit 505 PCM samples");
        // Stall audio while video progresses. Overflow keeps source sample PTS.
        for (int i = 1; i <= 3; i++)
        {
            sink.PushAudio(Audio(i));
            sink.PushVideo(Video(i, true));
            bridge.DrainValidation("video");
        }
        Assert(bridge.ValidationState.AudioDrops == 2 && bridge.ValidationState.QueuedAudio == 1,
            "audio stall overflow counters/live edge");
        bridge.DrainValidation("audio");
        var last = emitted.Last();
        Assert(last.Audio && last.Pts == 189_375_000 && (last.Flags & 2) != 0,
            "audio overflow must retain gap and DISCONT");
        sink.PushAudio(Audio(4));
        bridge.DrainValidation("audio");
        Assert(emitted.Last().Pts == 252_500_000 && (emitted.Last().Flags & 2) == 0,
            "no cumulative audio PTS subtraction");
        // Stall video; the production GOP policy must reject deltas until key.
        for (int i = 4; i <= 7; i++)
        {
            sink.PushVideo(Video(i, false));
            sink.PushAudio(Audio(i + 1));
            bridge.DrainValidation("audio");
        }
        Assert(bridge.ValidationState.VideoDrops == 4 && bridge.ValidationState.QueuedVideo == 0,
            "video stall removes GOP and rejects following delta");
        sink.PushVideo(Video(8, true));
        bridge.DrainValidation("video");
        Assert(!emitted.Last().Audio && emitted.Last().Pts == 800_000_000 && (emitted.Last().Flags & 2) != 0,
            "video recovery keyframe must be queued with DISCONT even when capacity is available");
        long samples = bridge.ValidationState.TimelineSamples;
        ExpectInvalid(() => sink.PushAudio(Audio(8)));
        Assert(bridge.ValidationState.TimelineSamples == samples, "duplicate cannot advance clock");
        ExpectInvalid(() => sink.PushAudio(new FrameData { RawType = 0x16, FrameId = 100,
            Timestamp = 1510, Payload = new byte[128] }));
        // Pending packets from the old epoch are discarded by the actual Reset.
        sink.PushVideo(Video(9, true));
        sink.Reset();
        emitted.Clear();
        sink.PushVideo(Video(0, false, 1));
        Assert(bridge.ValidationState.QueuedVideo == 0, "new epoch waits for keyframe");
        sink.PushVideo(Video(0, true, 1));
        sink.PushAudio(Audio(0, 1));
        bridge.DrainValidation();
        Assert(emitted.Count == 2 && emitted.All(p => p.Pts == 0) &&
            bridge.ValidationState.Resets == 1 && bridge.ValidationState.PublisherStarts == 2 &&
            bridge.ValidationState.TimelineSamples == 505, "reset clears clocks, queues, codec state and epoch");
        ExpectIOException(() => sink.PushAudio(Audio(1, 2)));
        Console.WriteLine("production media checks passed: PCM/counts, track stalls, overflow/gap/DISCONT/GOP, duplicates, codec size, reset and epoch");
        return 0;
    }
    private static FrameData Audio(int i, int epoch = 0) => new()
    {
        RawType = 0x16, FrameId = (uint)(1 + i / 2 * 7 + (i % 2) * 3),
        Timestamp = 1000 + (ulong)(i / 2 * 126.25), Payload = new byte[256], Epoch = epoch
    };
    private static FrameData Video(int i, bool keyframe, int epoch = 0) => new()
    {
        RawType = keyframe ? (byte)0 : (byte)1, FrameId = (uint)(2 + i * 5),
        Timestamp = (ulong)(1000 + i * 100), Payload = [0, 0, 0, 1, 0x65], Epoch = epoch
    };
    private static void Assert(bool condition, string message)
    {
        if (!condition) throw new InvalidDataException(message);
    }
    private static void ExpectInvalid(Action action)
    {
        try { action(); } catch (InvalidDataException) { return; }
        throw new InvalidDataException("Expected invalid audio to fail");
    }
    private static void ExpectIOException(Action action)
    {
        try { action(); } catch (IOException) { return; }
        throw new InvalidDataException("Expected epoch mismatch to fail");
    }
}
