using V380Decoder.src;

static class OfflineValidationChecks
{
    public static int Run()
    {
        EnvironmentConfigurationChecks.Run();
        var emitted = new List<ValidatedMediaPacket>();
        using var bridge = MediaMtxBridge.CreateOfflineValidation(emitted.Add);
        IMediaSink sink = bridge.CreateSink("live");
        sink.PushVideo(Video(0, true));
        sink.PushAudio(Audio(0));
        bridge.DrainValidation();
        Assert(emitted.Count == 2 && emitted.Single(p => p.Audio).Payload.Length == 1010,
            "production decoder must emit 505 PCM samples");
        // Stall audio while video progresses. Four complete blocks span 252.5 ms
        // and must fit beneath the 280 ms hard limit without packet-count drops.
        for (int i = 1; i <= 4; i++)
        {
            sink.PushAudio(Audio(i));
            sink.PushVideo(Video(i, i == 1));
            bridge.DrainValidation("video");
        }
        var audioBeforeOverflow = bridge.ValidationState;
        Assert(audioBeforeOverflow.AudioDrops == 0 && audioBeforeOverflow.QueuedAudio == 4 &&
            audioBeforeOverflow.QueuedAudioDurationNs == 252_500_000 &&
            audioBeforeOverflow.QueuedAudioPts.SequenceEqual(new ulong[] {
                63_125_000, 126_250_000, 189_375_000, 252_500_000 }),
            "four audio blocks / 252.5 ms must be preserved without overflow");
        sink.PushAudio(Audio(5));
        var audioAfterOverflow = bridge.ValidationState;
        Assert(audioAfterOverflow.AudioDrops == 4 && audioAfterOverflow.QueuedAudio == 1 &&
            audioAfterOverflow.QueuedAudioDurationNs == 63_125_000 &&
            audioAfterOverflow.QueuedAudioPts.SequenceEqual(new ulong[] { 315_625_000 }),
            "fifth audio block must drop four stale packets and preserve live-edge PTS");
        bridge.DrainValidation("audio");
        var last = emitted.Last();
        Assert(last.Audio && last.Pts == 315_625_000 && (last.Flags & 2) != 0,
            "audio overflow must retain gap and DISCONT");
        sink.PushAudio(Audio(6));
        bridge.DrainValidation("audio");
        Assert(emitted.Last().Pts == 378_750_000 && (emitted.Last().Flags & 2) == 0,
            "no cumulative audio PTS subtraction");

        // A stalled video queue accepts the full 500 ms span, then discards
        // the GOP when the next delta extends its range beyond the hard limit.
        for (int i = 5; i <= 10; i++)
        {
            sink.PushVideo(Video(i, i == 5));
        }
        var videoBeforeOverflow = bridge.ValidationState;
        Assert(videoBeforeOverflow.VideoDrops == 0 && videoBeforeOverflow.QueuedVideo == 6 &&
            videoBeforeOverflow.QueuedVideoSpanNs == 500_000_000 &&
            videoBeforeOverflow.QueuedVideoPts.First() == 500_000_000 &&
            videoBeforeOverflow.QueuedVideoPts.Last() == 1_000_000_000,
            "video span through 500 ms must remain queued without overflow");
        sink.PushVideo(Video(11, false));
        Assert(bridge.ValidationState.VideoDrops == 7 && bridge.ValidationState.QueuedVideo == 0,
            "over-limit video delta must drop the queued GOP and itself");
        sink.PushVideo(Video(12, false));
        Assert(bridge.ValidationState.VideoDrops == 8 && bridge.ValidationState.QueuedVideo == 0,
            "deltas must remain discarded while waiting for a keyframe");
        sink.PushVideo(Video(13, true));
        bridge.DrainValidation("video");
        Assert(!emitted.Last().Audio && emitted.Last().Pts == 1_300_000_000 &&
            (emitted.Last().Flags & 3) == 3,
            "video recovery keyframe must be queued with DISCONT even when capacity is available");
        sink.PushVideo(Video(14, true));
        bridge.DrainValidation("video");
        Assert(emitted.Last().Pts == 1_400_000_000 && (emitted.Last().Flags & 2) == 0,
            "next video keyframe must not receive an unnecessary DISCONT");
        long samples = bridge.ValidationState.TimelineSamples;
        ExpectInvalid(() => sink.PushAudio(Audio(8)));
        Assert(bridge.ValidationState.TimelineSamples == samples, "duplicate cannot advance clock");
        ExpectInvalid(() => sink.PushAudio(new FrameData { RawType = 0x16, FrameId = 100,
            Timestamp = 1510, Payload = new byte[128] }));
        // Pending packets from the old epoch are discarded by the actual Reset.
        sink.PushVideo(Video(15, true));
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
