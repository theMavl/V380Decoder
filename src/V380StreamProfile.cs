namespace V380Decoder.src
{
    public sealed class V380StreamProfile
    {
        public int Quality { get; init; }
        public string Path { get; init; }
        public int Width { get; init; }
        public int Height { get; init; }
        public int FrameRate { get; init; }
        public string Encoding { get; init; } = string.Empty;

        public string ProfileToken => $"Profile_{Quality}";
        public string VideoSourceToken => "VideoSource_1";
        public string VideoSourceConfigToken => "VideoSrcCfg_1";
        public string VideoEncoderToken => $"VideoEnc_{Quality}";
        public string DisplayName => Quality == 1 ? "V380 High" : "V380 Low";

        public bool HasSaneVideoParameters =>
            Width > 0 && Width <= 16384 &&
            Height > 0 && Height <= 16384 &&
            FrameRate > 0 && FrameRate <= 240;
    }

    public sealed class V380StreamCatalog
    {
        private readonly object sync = new();
        private readonly Dictionary<int, V380StreamProfile> profiles = new();
        private bool discoveryComplete;

        public bool RegisterPrimary(V380StreamProfile profile)
        {
            if (profile == null || !profile.HasSaneVideoParameters)
                return false;

            lock (sync)
            {
                profiles[profile.Quality] = profile;
                Monitor.PulseAll(sync);
                return true;
            }
        }

        public bool RegisterAdditional(V380StreamProfile profile)
        {
            if (profile == null || !profile.HasSaneVideoParameters)
                return false;

            lock (sync)
            {
                bool duplicate = profiles.Values.Any(existing =>
                    existing.Quality != profile.Quality &&
                    existing.Width == profile.Width &&
                    existing.Height == profile.Height &&
                    existing.FrameRate == profile.FrameRate);

                if (duplicate)
                    return false;

                profiles[profile.Quality] = profile;
                Monitor.PulseAll(sync);
                return true;
            }
        }

        public IReadOnlyList<V380StreamProfile> GetProfiles(TimeSpan waitForFirst)
        {
            lock (sync)
            {
                DateTime deadline = DateTime.UtcNow + waitForFirst;
                while ((!discoveryComplete ||
                        !profiles.Values.Any(profile => !string.IsNullOrEmpty(profile.Encoding)) ||
                        profiles.Values.Any(profile => string.IsNullOrEmpty(profile.Encoding))))
                {
                    TimeSpan remaining = deadline - DateTime.UtcNow;
                    if (remaining <= TimeSpan.Zero)
                        break;
                    Monitor.Wait(sync, remaining);
                }

                return profiles.Values
                    .Where(profile => !string.IsNullOrEmpty(profile.Encoding))
                    .OrderByDescending(profile => profile.Quality)
                    .ToArray();
            }
        }

        public void MarkDiscoveryComplete()
        {
            lock (sync)
            {
                discoveryComplete = true;
                Monitor.PulseAll(sync);
            }
        }
    }
}
