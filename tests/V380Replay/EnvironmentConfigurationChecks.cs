using V380Decoder.src;

static class EnvironmentConfigurationChecks
{
    private static readonly string[] VariableNames =
    [
        "V380_ID", "V380_USERNAME", "V380_PASSWORD", "V380_IP", "V380_PORT",
        "V380_SOURCE", "V380_OUTPUT", "V380_RTSP_PORT", "V380_HTTP_PORT",
        "V380_ENABLE_API", "V380_ENABLE_ONVIF", "V380_ENABLE_MJPEG",
        "V380_SECURE", "V380_DEBUG", "V380_AUDIO_DUMP"
    ];

    public static void Run()
    {
        DefaultsArePreserved();
        EnvironmentCanStartWithoutArguments();
        CommandLineOverridesEnvironment();
        BooleanValuesAreStrictAndCaseInsensitive();
        MalformedEnvironmentValuesAreSanitized();
        Console.WriteLine("environment configuration checks passed: defaults, zero-argument startup, CLI precedence and strict sanitized values");
    }

    private static void DefaultsArePreserved() => WithEnvironment([], () =>
    {
        AppOptions options = AppOptions.Parse([]);
        Assert(!options.HasEnvironmentId && options.Id == 0, "empty ID must not start the decoder");
        Assert(options.Port == 8800 && options.Username == "admin" && options.Password == "" &&
            options.Ip == "" && options.Source == "lan" && options.Output == "rtsp" &&
            !options.EnableOnvif && !options.EnableApi && !options.EnableMjpeg &&
            options.RtspPort == 8554 && options.HttpPort == 8080 &&
            options.AudioDumpPath == "" && !options.Secure && !options.Debug,
            "CLI defaults changed");
    });

    private static void EnvironmentCanStartWithoutArguments() => WithEnvironment(
        [("V380_ID", "123456"), ("V380_USERNAME", "operator"),
         ("V380_PASSWORD", "test-only-password"), ("V380_IP", "192.0.2.10"),
         ("V380_PORT", "8801"), ("V380_SOURCE", "lan"), ("V380_OUTPUT", "rtsp"),
         ("V380_RTSP_PORT", "8555"), ("V380_HTTP_PORT", "8081"),
         ("V380_ENABLE_API", "true"), ("V380_ENABLE_ONVIF", "1"),
         ("V380_ENABLE_MJPEG", "false"), ("V380_SECURE", "0"),
         ("V380_DEBUG", "FALSE"), ("V380_AUDIO_DUMP", "/tmp/audio.dump")],
        () =>
        {
            AppOptions options = AppOptions.Parse([]);
            Assert(options.HasEnvironmentId && options.Id == 123456 && options.Port == 8801 &&
                options.Username == "operator" && options.Password == "test-only-password" &&
                options.Ip == "192.0.2.10" && options.Source == "lan" && options.Output == "rtsp" &&
                options.RtspPort == 8555 && options.HttpPort == 8081 && options.EnableApi &&
                options.EnableOnvif && !options.EnableMjpeg && !options.Secure && !options.Debug &&
                options.AudioDumpPath == "/tmp/audio.dump",
                "zero-argument environment configuration was not applied");
        });

    private static void CommandLineOverridesEnvironment() => WithEnvironment(
        [("V380_ID", "invalid-env-id"), ("V380_PASSWORD", "environment-secret"),
         ("V380_PORT", "invalid-env-port"), ("V380_ENABLE_API", "invalid-env-bool")],
        () =>
        {
            AppOptions options = AppOptions.Parse(
                ["--id", "42", "--password", "cli-password", "--port", "9001", "--enable-api"]);
            Assert(options.Id == 42 && options.Password == "cli-password" &&
                options.Port == 9001 && options.EnableApi,
                "explicit CLI values did not override environment values");
        });

    private static void BooleanValuesAreStrictAndCaseInsensitive() => WithEnvironment(
        [("V380_ID", "1"), ("V380_ENABLE_API", "TrUe"), ("V380_ENABLE_ONVIF", "1"),
         ("V380_ENABLE_MJPEG", "FALSE"), ("V380_SECURE", "0"), ("V380_DEBUG", "false")],
        () =>
        {
            AppOptions options = AppOptions.Parse([]);
            Assert(options.EnableApi && options.EnableOnvif && !options.EnableMjpeg &&
                !options.Secure && !options.Debug,
                "supported boolean environment spellings were not parsed");
        });

    private static void MalformedEnvironmentValuesAreSanitized()
    {
        WithEnvironment([("V380_ID", "8"), ("V380_PORT", "port-private-marker")], () =>
            AssertOnlyVariableName("V380_PORT", "port-private-marker"));
        WithEnvironment([("V380_ID", "9"), ("V380_SECURE", "bool-private-marker")], () =>
            AssertOnlyVariableName("V380_SECURE", "bool-private-marker"));
        WithEnvironment([("V380_ID", "id-private-marker")], () =>
            AssertOnlyVariableName("V380_ID", "id-private-marker"));
    }

    private static void AssertOnlyVariableName(string expectedName, string forbiddenValue)
    {
        try
        {
            AppOptions.Parse([]);
        }
        catch (ArgumentException ex)
        {
            Assert(ex.Message == $"Invalid environment variable {expectedName}" &&
                !ex.Message.Contains(forbiddenValue, StringComparison.Ordinal),
                "environment parsing error leaked a value or named the wrong variable");
            return;
        }
        throw new InvalidDataException($"Malformed {expectedName} was accepted");
    }

    private static void WithEnvironment((string Name, string Value)[] values, Action test)
    {
        Dictionary<string, string?> previous = VariableNames.ToDictionary(
            name => name, Environment.GetEnvironmentVariable);
        try
        {
            foreach (string name in VariableNames)
                Environment.SetEnvironmentVariable(name, null);
            foreach ((string name, string value) in values)
                Environment.SetEnvironmentVariable(name, value);
            test();
        }
        finally
        {
            foreach ((string name, string? value) in previous)
                Environment.SetEnvironmentVariable(name, value);
        }
    }

    private static void Assert(bool condition, string message)
    {
        if (!condition) throw new InvalidDataException(message);
    }
}
