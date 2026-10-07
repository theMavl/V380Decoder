#!/usr/bin/env python3
"""Generate public, deterministic fixtures: no camera media or credentials.

The PCM reference is decoded independently by FFmpeg. Timestamps emulate the
documented pair batching; they are synthetic, not a sanitized real capture.
"""
import argparse
import csv
import json
from pathlib import Path
import random
import struct
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    rng = random.Random(380)
    blocks = []
    for i in range(650):
        blocks.append(struct.pack("<hBB", (i * 97) % 6000 - 3000, i % 56, 0)
                      + bytes(rng.randrange(256) for _ in range(252)))
    adpcm = b"".join(blocks)
    (args.output / "audio.adpcm").write_bytes(adpcm)
    fmt = struct.pack("<HHIIHHHH", 0x11, 1, 8000, 8000 * 256 // 505,
                      256, 4, 2, 505)
    chunks = b"fmt " + struct.pack("<I", len(fmt)) + fmt
    chunks += b"fact" + struct.pack("<II", 4, 650 * 505)
    chunks += b"data" + struct.pack("<I", len(adpcm)) + adpcm
    encoded = args.output / "audio.ima.wav"
    encoded.write_bytes(b"RIFF" + struct.pack("<I", len(chunks) + 4) + b"WAVE" + chunks)
    subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                    "-i", str(encoded), "-c:a", "pcm_s16le",
                    str(args.output / "reference.pcm.wav")], check=True)
    frames = []
    with (args.output / "timestamps.csv").open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(["ordinal", "kind", "size", "type", "unused", "frame_id",
                         "frame_type", "frame_rate", "timestamp_ms"])
        for i, block in enumerate(blocks):
            frame_id = 1 + i // 2 * 7 + (i % 2) * 3
            timestamp = 1000 + int(i // 2 * 126.25)
            writer.writerow([i, "audio", 272, 22, 0, frame_id, 0, 20, timestamp])
            frames.append((i * 63_125_000, 0x16, frame_id, timestamp, block))
    # Minimal Annex-B ingress fixtures test framing/clock/queues. The native
    # pipeline construction checks separately cover H.264/H.265 elements.
    for i in range(411):
        frames.append((i * 100_000_000, 0 if i % 40 == 0 else 1,
                       2 + i * 5, 1000 + i * 100, bytes([0, 0, 0, 1, 0x65 if i % 40 == 0 else 0x41, 0x80])))
    frames.sort(key=lambda item: (item[0], item[1]))
    metadata = json.dumps({"camera": {"device_version": 3},
                           "fixture": "generated-no-camera-data"}).encode()
    with (args.output / "synthetic.v380dump").open("wb") as output:
        output.write(b"V380DMP1" + struct.pack("<I", len(metadata)) + metadata)
        for arrival, kind, frame_id, timestamp, payload in frames:
            inner = struct.pack("<IHHQ", frame_id, 0, 20, timestamp) + payload
            wire = struct.pack("<BB B H H H 3x", 0x7f, kind, 0, 1, 0, len(inner)) + inner
            # Exercise TCP records that end midway through outer/inner headers.
            for part in (wire[:5], wire[5:19], wire[19:]):
                output.write(struct.pack("<QI", arrival, len(part)) + part)
    print("generated safe fixtures: video=411 audio=650 samples=328250")


if __name__ == "__main__":
    main()
