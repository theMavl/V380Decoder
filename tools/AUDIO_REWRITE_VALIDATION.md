# Audio rewrite validation

Run the offline regression suite from the repository root with:

```sh
bash tools/check_audio_rewrite.sh
```

The command publishes the .NET application into `artifacts/audio-rewrite-validation`,
compiles the native helper against GStreamer, runs the ADPCM golden check, tests
the saved camera timestamp capture, exercises native audio/video overflow
policies, verifies H.264/H.265 plus PCM pipeline linking, and parses/replays the
saved V380 dump into a validation sink. The
replay check does not connect to a camera or require MediaMTX.

Requirements: the .NET 10 SDK, GCC, `pkg-config`, GStreamer development headers
for `gstreamer-1.0` and `gstreamer-app-1.0`, and the checked-in capture fixtures
listed in the script. The test projects are explicitly built and invoked by the
script; they are not part of the production application compile.
