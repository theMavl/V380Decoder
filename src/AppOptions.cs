using System.Globalization;

namespace V380Decoder.src;

/// <summary>Application options resolved from explicit CLI arguments first,
/// then the corresponding V380_* environment variable, then defaults.</summary>
public sealed record AppOptions
{
    public int Id { get; init; }
    public int Port { get; init; }
    public string Username { get; init; } = "admin";
    public string Password { get; init; } = "";
    public string Ip { get; init; } = "";
    public string Source { get; init; } = "lan";
    public string Output { get; init; } = "rtsp";
    public bool EnableOnvif { get; init; }
    public bool EnableApi { get; init; }
    public bool EnableMjpeg { get; init; }
    public int RtspPort { get; init; } = 8554;
    public int HttpPort { get; init; } = 8080;
    public string AudioDumpPath { get; init; } = "";
    public bool Secure { get; init; }
    public bool Debug { get; init; }
    public bool HasEnvironmentId { get; init; }

    public static AppOptions Parse(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);
        string rawEnvironmentId = Environment.GetEnvironmentVariable("V380_ID");
        return new AppOptions
        {
            Id = GetInt(args, "--id", "V380_ID", 0, idMayBeEmpty: true),
            Port = GetInt(args, "--port", "V380_PORT", 8800),
            Username = GetString(args, "--username", "V380_USERNAME", "admin"),
            Password = GetString(args, "--password", "V380_PASSWORD", ""),
            Ip = GetString(args, "--ip", "V380_IP", ""),
            Source = GetString(args, "--source", "V380_SOURCE", "lan"),
            Output = GetString(args, "--output", "V380_OUTPUT", "rtsp"),
            EnableOnvif = GetBoolean(args, "--enable-onvif", "V380_ENABLE_ONVIF", false),
            EnableApi = GetBoolean(args, "--enable-api", "V380_ENABLE_API", false),
            EnableMjpeg = GetBoolean(args, "--enable-mjpeg", "V380_ENABLE_MJPEG", false),
            RtspPort = GetInt(args, "--rtsp-port", "V380_RTSP_PORT", 8554),
            HttpPort = GetInt(args, "--http-port", "V380_HTTP_PORT", 8080),
            AudioDumpPath = GetString(args, "--audio-dump", "V380_AUDIO_DUMP", ""),
            Secure = GetBoolean(args, "--secure", "V380_SECURE", false),
            Debug = GetBoolean(args, "--debug", "V380_DEBUG", false),
            HasEnvironmentId = !string.IsNullOrWhiteSpace(rawEnvironmentId)
        };
    }

    private static int GetInt(
        string[] args, string option, string variable, int defaultValue, bool idMayBeEmpty = false)
    {
        if (HasOption(args, option))
            return ArgParser.GetArg(args, option, defaultValue);

        string value = Environment.GetEnvironmentVariable(variable);
        if (value == null || (idMayBeEmpty && string.IsNullOrWhiteSpace(value)))
            return defaultValue;
        if (int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed))
            return parsed;
        throw new ArgumentException($"Invalid environment variable {variable}");
    }

    private static bool GetBoolean(string[] args, string option, string variable, bool defaultValue)
    {
        if (HasOption(args, option))
            return ArgParser.GetArg(args, option, defaultValue);

        string value = Environment.GetEnvironmentVariable(variable);
        if (value == null) return defaultValue;
        switch (value.Trim().ToLowerInvariant())
        {
            case "true":
            case "1":
                return true;
            case "false":
            case "0":
                return false;
            default:
                throw new ArgumentException($"Invalid environment variable {variable}");
        }
    }

    private static string GetString(string[] args, string option, string variable, string defaultValue)
    {
        if (HasOption(args, option))
            return ArgParser.GetArg(args, option, defaultValue);
        string value = Environment.GetEnvironmentVariable(variable);
        return string.IsNullOrWhiteSpace(value) ? defaultValue : value;
    }

    private static bool HasOption(string[] args, string option) =>
        args.Any(argument => string.Equals(argument, option, StringComparison.OrdinalIgnoreCase));
}
