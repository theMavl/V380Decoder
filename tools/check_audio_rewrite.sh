#!/usr/bin/env bash
# Reproducible offline validation for the V380 audio rewrite.
# One host command: bash tools/check_audio_rewrite.sh --docker
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [[ "${1:-}" == "--docker" ]]; then
    docker build -f Dockerfile.validation -t v380decoder:audio-validation "$ROOT"
    exec docker run --rm --network none v380decoder:audio-validation
fi
OUT="$ROOT/artifacts/audio-rewrite-validation"
FIXTURES="$OUT/fixtures"
CAPTURE="$FIXTURES/synthetic.v380dump"
TIMESTAMPS="$FIXTURES/timestamps.csv"
ADPCM="$FIXTURES/audio.adpcm"
REFERENCE="$FIXTURES/reference.pcm.wav"

for command in dotnet gcc pkg-config python3 ffmpeg; do
    command -v "$command" >/dev/null || {
        echo "missing required command: $command" >&2
        exit 1
    }
done
pkg-config --exists gstreamer-1.0 gstreamer-app-1.0 || {
    echo "GStreamer development packages are required (gstreamer-1.0 and gstreamer-app-1.0)" >&2
    exit 1
}
python3 tools/generate_validation_fixtures.py "$FIXTURES"

mkdir -p "$OUT/publish"
dotnet publish V380Decoder.csproj --configuration Release --output "$OUT/publish"
gcc -O2 -Wall -Wextra native/v380-gst-bridge.c -o "$OUT/v380-gst-bridge" \
    $(pkg-config --cflags --libs gstreamer-1.0 gstreamer-app-1.0)
cp "$OUT/v380-gst-bridge" "$OUT/publish/v380-gst-bridge"

dotnet build tests/ImaAdpcmChecks/ImaAdpcmChecks.csproj --configuration Release
dotnet build tests/V380Replay/V380Replay.csproj --configuration Release
"$OUT/v380-gst-bridge" --self-test-queues
"$OUT/v380-gst-bridge" --self-test-pipelines
dotnet run --project tests/ImaAdpcmChecks/ImaAdpcmChecks.csproj --configuration Release --no-build -- --clock
dotnet run --project tests/V380Replay/V380Replay.csproj --configuration Release --no-build -- --self-test
dotnet run --project tests/ImaAdpcmChecks/ImaAdpcmChecks.csproj --configuration Release --no-build -- \
    "$ADPCM" "$REFERENCE"
dotnet run --project tests/ImaAdpcmChecks/ImaAdpcmChecks.csproj --configuration Release --no-build -- \
    --timestamps "$TIMESTAMPS"
dotnet run --project tests/V380Replay/V380Replay.csproj --configuration Release --no-build -- \
    "$CAPTURE" --validate-only

# Optional private golden capture is mounted READ ONLY, never copied into image.
if [[ -n "${V380_LOCAL_FIXTURES:-}" ]]; then
    for fixture in v380-bad-audio.v380dump v380-bad-audio-analysis/audio-0x16-frames.csv \
        v380-bad-audio-analysis/audio-0x16-offset-16.adpcm \
        v380-bad-audio-analysis/audio-0x16-frame-reset.pcm.wav; do
        [[ -f "$V380_LOCAL_FIXTURES/$fixture" ]] || {
            echo "missing requested local fixture: $V380_LOCAL_FIXTURES/$fixture" >&2; exit 1;
        }
    done
    dotnet run --project tests/ImaAdpcmChecks/ImaAdpcmChecks.csproj -c Release --no-build -- \
        "$V380_LOCAL_FIXTURES/v380-bad-audio-analysis/audio-0x16-offset-16.adpcm" \
        "$V380_LOCAL_FIXTURES/v380-bad-audio-analysis/audio-0x16-frame-reset.pcm.wav"
    dotnet run --project tests/ImaAdpcmChecks/ImaAdpcmChecks.csproj -c Release --no-build -- \
        --timestamps "$V380_LOCAL_FIXTURES/v380-bad-audio-analysis/audio-0x16-frames.csv"
    dotnet run --project tests/V380Replay/V380Replay.csproj -c Release --no-build -- \
        "$V380_LOCAL_FIXTURES/v380-bad-audio.v380dump" --validate-only
else
    echo "private capture checks not requested; generated public fixtures passed"
fi

echo "audio rewrite checks passed"
