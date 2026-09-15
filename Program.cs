using V380Decoder.src;

if (args.Length > 0)
{
    if (args.Contains("--help") || args.Contains("-h") || args.Contains("/?"))
    {
        PrintHelp();
        return;
    }

    if (args.Contains("--discover") || args.Contains("-d"))
    {
        var discovery = new DeviceDiscovery();
        var devices = discovery.Discover();
        foreach (var dev in devices)
        {
            Console.Error.WriteLine($" ID:  {dev.DevId}\n IP:  {dev.Ip}\n MAC: {dev.Mac}\n");
        }
        return;
    }

    int id = ArgParser.GetArg(args, "--id", 0);
    int port = ArgParser.GetArg(args, "--port", 8800);
    string username = ArgParser.GetArg(args, "--username", "admin");
    string password = ArgParser.GetArg(args, "--password", "");
    string ip = ArgParser.GetArg(args, "--ip", "");
    string source = ArgParser.GetArg(args, "--source", "lan");
    string output = ArgParser.GetArg(args, "--output", "rtsp");
    bool enableOnvif = ArgParser.GetArg(args, "--enable-onvif", false);
    bool enableApi = ArgParser.GetArg(args, "--enable-api", false);
    bool enableMjpeg = ArgParser.GetArg(args, "--enable-mjpeg", false);
    int rtspPort = ArgParser.GetArg(args, "--rtsp-port", 8554);
    int httpPort = ArgParser.GetArg(args, "--http-port", 8080);
    bool secure = ArgParser.GetArg(args, "--secure", false);
    bool debug = ArgParser.GetArg(args, "--debug", false);

    if (source.Equals("lan", StringComparison.OrdinalIgnoreCase) && string.IsNullOrEmpty(ip))
    {
        Console.Error.WriteLine("Camera ip address not set");
        return;
    }
    if (id == 0)
    {
        Console.Error.WriteLine("Camera id not set");
        return;
    }
    if (string.IsNullOrEmpty(password))
    {
        Console.Error.WriteLine("Camera password not set");
        return;
    }
    if (debug)
    {
        LogUtils.enableDebug = true;
    }

    OutputMode outputMode = output switch
    {
        "audio" => OutputMode.Audio,
        "video" => OutputMode.Video,
        _ => OutputMode.Rtsp
    };

    SourceStream sourceStream = source switch
    {
        "cloud" => SourceStream.Cloud,
        _ => SourceStream.Lan
    };

    string relayIp = string.Empty;
    if (sourceStream == SourceStream.Cloud)
    {
        relayIp = await DispatchRelayServer.GetServerIPAsync(id);
        if (string.IsNullOrEmpty(relayIp))
        {
            Console.Error.WriteLine("[V380] failed to get relay server");
        }
        Console.Error.WriteLine($"[V380] using relay server {relayIp}");
    }


    bool enableWebServer = enableApi || enableOnvif;
    var cts = new CancellationTokenSource();
    var streamCatalog = new V380StreamCatalog();
    var client = new V380Client(
        sourceStream == SourceStream.Cloud ? relayIp : ip,
        port,
        (uint)id,
        username,
        password,
        sourceStream,
        outputMode,
        enableMjpeg
    );

    IMediaSink mediaSink = null;
    IMediaSink lowMediaSink = null;
    MediaMtxBridge mediaBridge = null;
    V380Client lowStreamClient = null;
    Task lowStreamTask = null;
    int lowStreamStarted = 0;
    WebServer webServer = null;
    if (outputMode == OutputMode.Rtsp)
    {
        mediaBridge = new(
            rtspPort,
            secure,
            username,
            password);
        mediaBridge.Start();
        mediaSink = mediaBridge.CreateSink("live");

        if (enableOnvif)
        {
            lowMediaSink = mediaBridge.CreateSink("live-low");
            lowStreamClient = new V380Client(
                sourceStream == SourceStream.Cloud ? relayIp : ip,
                port,
                (uint)id,
                username,
                password,
                sourceStream,
                outputMode,
                enableMjpeg: false,
                streamQuality: 0,
                streamPath: "live-low",
                enableSnapshots: false);

            lowStreamClient.StreamNegotiated = profile =>
            {
                bool accepted = streamCatalog.RegisterAdditional(profile);
                streamCatalog.MarkDiscoveryComplete();
                if (accepted && !string.IsNullOrEmpty(profile.Encoding))
                {
                    Console.Error.WriteLine(
                        $"[ONVIF] discovered {profile.DisplayName}: " +
                        $"{profile.Width}x{profile.Height}@{profile.FrameRate} " +
                        $"{profile.Encoding} -> /{profile.Path}");
                }
                return accepted;
            };
            lowStreamClient.StreamUnavailable = streamCatalog.MarkDiscoveryComplete;
        }

        client.StreamNegotiated = profile =>
        {
            bool accepted = streamCatalog.RegisterPrimary(profile);
            if (accepted && !string.IsNullOrEmpty(profile.Encoding))
            {
                Console.Error.WriteLine(
                    $"[ONVIF] discovered {profile.DisplayName}: " +
                    $"{profile.Width}x{profile.Height}@{profile.FrameRate} -> /{profile.Path}");
            }

            if (lowStreamClient != null && Interlocked.Exchange(ref lowStreamStarted, 1) == 0)
            {
                lowStreamTask = Task.Run(() => lowStreamClient.Run(lowMediaSink, cts.Token));
            }

            return accepted;
        };

        webServer = new(
            httpPort,
            rtspPort,
            client,
            enableApi,
            enableOnvif,
            enableMjpeg,
            secure,
            username,
            password,
            streamCatalog);
        webServer.Start();
    }

    OnvifDiscovery onvifDiscovery = null;
    if (enableOnvif)
    {
        onvifDiscovery = new(httpPort);
        onvifDiscovery.Start();
    }

    Console.CancelKeyPress += (sender, e) =>
     {
         e.Cancel = true;
         cts.Cancel();
     };

    AppDomain.CurrentDomain.ProcessExit += (sender, e) =>
    {
        cts.Cancel();
        Thread.Sleep(2000);
    };

    try
    {
        client.Run(mediaSink, cts.Token);
    }
    catch (OperationCanceledException)
    {
        Console.Error.WriteLine("[V380] Shutdown requested");
    }
    finally
    {
        Console.Error.WriteLine("[V380] Cleaning up...");
        cts.Cancel();
        webServer?.Stop();
        client.Dispose();
        lowStreamClient?.Dispose();
        try { lowStreamTask?.Wait(TimeSpan.FromSeconds(2)); } catch { }
        mediaBridge?.Dispose();
        onvifDiscovery?.Dispose();
    }

    Console.Error.WriteLine("[V380] Stopped");
}
else
{
    Console.Error.WriteLine("[V380] No arguments provided");
}

static void PrintHelp()
{
    Console.WriteLine(@"
╔══════════════════════════════════════════════════════════════╗
║                          V380Decoder                         ║
╚══════════════════════════════════════════════════════════════╝

DESCRIPTION:
  Decode and stream video/audio from V380 cameras. Supports local (LAN) and 
  cloud streaming, RTSP output, ONVIF, and web API control.

USAGE:
  V380Decoder [OPTIONS]

REQUIRED ARGUMENTS:
  --id <number>          Camera ID (required)
  --username <string>    Camera username (default: admin)
  --password <string>    Camera password (required)

CONNECTION OPTIONS:
  --ip <address>         Camera IP address for LAN mode
                         Example: --ip 192.168.1.100
  
  --port <number>        Camera port (default: 8800)
                         Example: --port 8800
  
  --source <mode>        Source type: 'lan' or 'cloud' (default: lan)
                         lan   - Direct connection to camera IP
                         cloud - Connection via relay server

OUTPUT OPTIONS:
  --output <type>        Output type: 'video', 'audio', or 'rtsp' (default: rtsp)
                         video - Raw H.264 video to stdout (pipe to ffplay)
                         audio - Raw G.711 audio to stdout (pipe to ffplay)
                         rtsp  - FFmpeg + MediaMTX RTSP stream (default)

  --rtsp-port <number>   RTSP server port when output=rtsp (default: 8554)
                         Example: --rtsp-port 8554

SERVER OPTIONS:
  --enable-api           Enable web API server (default: false)
                         Provides REST API and web UI for camera control
  
  --http-port <number>   Web API server port (default: 8080)
                         Example: --http-port 8080
  
  --enable-onvif         Enable ONVIF server (experimental) (default: false)
                         Works only with --output rtsp
                         Tested with Onvif Device Manager (ODM)

  --enable-mjpeg         Enable MJPEG stream for access through browser.

  --secure               Enable authentication for ONVIF, RTSP, and API.
                         Uses the same username and password as the V380 camera.                       

OTHER OPTIONS:
  --discover             Find camera devices on the local network
  --debug                Enable debug logging (default: false)
  --help                 Show this help message

EXAMPLES:

  1. RTSP streaming (LAN):
     V380Decoder --id 12345678 --username admin --password secret --ip 192.168.1.100
     
  2. Video to stdout (pipe to ffplay):
     V380Decoder --id 12345678 --username admin --password secret --ip 192.168.1.100 --output video | ffplay -f h264 -i pipe:0
     
  3. Audio to stdout:
     V380Decoder --id 12345678 --username admin --password secret --ip 192.168.1.100 --output audio | ffplay -f alaw -ar 8000 -ac 1 -i pipe:0
     
  4. Cloud streaming with web API:
     V380Decoder --id 12345678 --username admin --password secret --source cloud --enable-api
     
  5. Complete setup with ONVIF:
     V380Decoder --id 12345678 --username admin --password secret --ip 192.168.1.100 --enable-onvif --enable-api --http-port 8080

ACCESS POINTS (when API enabled):
  • Web UI:        http://localhost:8080
  • Snapshot:      http://localhost:8080/snapshot
  • RTSP stream:   rtsp://localhost:8554/live
  • ONVIF service: http://localhost:8080/onvif/device_service
  • REST API:      http://localhost:8080/api/{command}

API COMMANDS:
  PTZ Control:
    POST /api/ptz/up, /down, /left, /right
  
  Light Control:
    POST /api/light/on, /off, /auto
  
  Image Settings:
    POST /api/image/color, /bw, /auto, /flip

NOTES:
  • For cloud mode, IP address is auto-discovered from relay server
  • ONVIF is experimental and only tested with Onvif Device Manager
  • RTSP port can be accessed by any RTSP client (VLC, ffplay, etc.)

");
}
