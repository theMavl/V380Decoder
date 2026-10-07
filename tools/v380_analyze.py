#!/usr/bin/env python3
"""Parse an offline V380 TCP dump and test audio extraction candidates."""

from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import statistics
import struct
import subprocess
import sys
import wave
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, Iterator


DUMP_MAGIC = b"V380DMP1"
RECORD_HEADER = struct.Struct("<QI")
AUDIO_TYPES = (0x16, 0x1A)
KNOWN_FRAME_TYPES = (0x00, 0x01, 0x16, 0x1A, 0x28, 0x29, 0x5B)


class DumpError(RuntimeError):
    pass


@dataclass
class Chunk:
    elapsed_ns: int
    data: bytes


@dataclass
class ParsedFragment:
    stream_offset: int
    elapsed_ns: int
    raw_header: bytes
    frame_type: int
    total: int
    current: int
    payload: bytes


@dataclass
class CompleteFrame:
    frame_type: int
    started_ns: int
    completed_ns: int
    payload: bytes
    fragments: int


@dataclass
class Assembly:
    total: int
    next_index: int
    started_ns: int
    parts: list[bytes] = field(default_factory=list)


class TimedByteReader:
    def __init__(self, chunks: Iterator[Chunk]):
        self._chunks = iter(chunks)
        self._buffer = bytearray()
        self._spans: list[list[int]] = []  # [remaining byte count, elapsed ns]
        self.offset = 0

    def _fill(self, size: int) -> bool:
        while len(self._buffer) < size:
            try:
                chunk = next(self._chunks)
            except StopIteration:
                return False
            if not chunk.data:
                continue
            self._buffer.extend(chunk.data)
            self._spans.append([len(chunk.data), chunk.elapsed_ns])
        return True

    def read_exact(self, size: int) -> tuple[bytes, int] | None:
        if not self._fill(size):
            return None
        timestamp = self._spans[0][1]
        data = bytes(self._buffer[:size])
        del self._buffer[:size]
        remaining = size
        while remaining:
            span = self._spans[0]
            consumed = min(remaining, span[0])
            span[0] -= consumed
            remaining -= consumed
            if span[0] == 0:
                self._spans.pop(0)
        self.offset += size
        return data, timestamp

    @property
    def buffered_bytes(self) -> int:
        return len(self._buffer)


def read_dump(path: Path) -> tuple[dict[str, object], Iterator[Chunk]]:
    source = path.open("rb")
    magic = source.read(len(DUMP_MAGIC))
    if magic != DUMP_MAGIC:
        source.close()
        raise DumpError(f"{path} is not a V380DMP1 file")
    length_raw = source.read(4)
    if len(length_raw) != 4:
        source.close()
        raise DumpError("truncated dump metadata length")
    metadata_length = struct.unpack("<I", length_raw)[0]
    if metadata_length > 16 * 1024 * 1024:
        source.close()
        raise DumpError("unreasonable dump metadata length")
    metadata_raw = source.read(metadata_length)
    if len(metadata_raw) != metadata_length:
        source.close()
        raise DumpError("truncated dump metadata")
    metadata = json.loads(metadata_raw.decode("utf-8"))

    def chunks() -> Iterator[Chunk]:
        try:
            while True:
                header = source.read(RECORD_HEADER.size)
                if not header:
                    return
                if len(header) != RECORD_HEADER.size:
                    print("warning: ignoring truncated final record header", file=sys.stderr)
                    return
                elapsed_ns, length = RECORD_HEADER.unpack(header)
                if length > 64 * 1024 * 1024:
                    raise DumpError(f"unreasonable recv chunk length: {length}")
                data = source.read(length)
                if len(data) != length:
                    print("warning: ignoring truncated final record", file=sys.stderr)
                    return
                yield Chunk(elapsed_ns, data)
        finally:
            source.close()

    return metadata, chunks()


def parse_fragments(chunks: Iterator[Chunk], errors: list[str]) -> Iterator[ParsedFragment]:
    reader = TimedByteReader(chunks)
    while True:
        record_offset = reader.offset
        header_result = reader.read_exact(12)
        if header_result is None:
            if reader.buffered_bytes:
                errors.append(f"truncated protocol header at stream offset {record_offset}")
            return
        header, timestamp = header_result
        if header[0] != 0x7F:
            errors.append(
                f"non-frame protocol record 0x{header[0]:02x} at stream offset {record_offset}; "
                f"12 bytes preserved but not interpreted"
            )
            continue

        frame_type = header[1]
        total, current, payload_length = struct.unpack_from("<HHH", header, 3)
        if not total or current >= total or not payload_length or payload_length > 65535:
            raise DumpError(
                f"invalid fragment header at stream offset {record_offset}: "
                f"type=0x{frame_type:02x} total={total} current={current} length={payload_length}"
            )
        payload_result = reader.read_exact(payload_length)
        if payload_result is None:
            errors.append(
                f"truncated payload at stream offset {record_offset}: expected {payload_length} bytes"
            )
            return
        payload, payload_timestamp = payload_result
        yield ParsedFragment(
            stream_offset=record_offset,
            elapsed_ns=min(timestamp, payload_timestamp),
            raw_header=header,
            frame_type=frame_type,
            total=total,
            current=current,
            payload=payload,
        )


def assemble_frames(
    fragments: Iterator[ParsedFragment], errors: list[str], counters: Counter
) -> Iterator[CompleteFrame]:
    active: dict[int, Assembly] = {}
    for fragment in fragments:
        counters[f"fragment_0x{fragment.frame_type:02x}"] += 1
        if fragment.frame_type not in KNOWN_FRAME_TYPES:
            counters["unknown_fragment_types"] += 1

        current = active.get(fragment.frame_type)
        if fragment.current == 0:
            if current is not None and current.next_index != current.total:
                errors.append(
                    f"type 0x{fragment.frame_type:02x}: new frame before previous completed "
                    f"({current.next_index}/{current.total})"
                )
                counters["incomplete_frames"] += 1
            current = Assembly(fragment.total, 0, fragment.elapsed_ns)
            active[fragment.frame_type] = current

        if current is None:
            errors.append(
                f"type 0x{fragment.frame_type:02x}: fragment {fragment.current}/{fragment.total} "
                "arrived without fragment 0"
            )
            counters["fragment_discontinuities"] += 1
            continue
        if fragment.total != current.total or fragment.current != current.next_index:
            errors.append(
                f"type 0x{fragment.frame_type:02x}: expected fragment "
                f"{current.next_index}/{current.total}, got {fragment.current}/{fragment.total}"
            )
            counters["fragment_discontinuities"] += 1
            active.pop(fragment.frame_type, None)
            continue

        current.parts.append(fragment.payload)
        current.next_index += 1
        if current.next_index != current.total:
            continue

        payload = b"".join(current.parts)
        active.pop(fragment.frame_type, None)
        counters[f"frame_0x{fragment.frame_type:02x}"] += 1
        yield CompleteFrame(
            frame_type=fragment.frame_type,
            started_ns=current.started_ns,
            completed_ns=fragment.elapsed_ns,
            payload=payload,
            fragments=current.total,
        )

    for frame_type, current in active.items():
        errors.append(
            f"type 0x{frame_type:02x}: dump ended during frame "
            f"({current.next_index}/{current.total})"
        )
        counters["incomplete_frames"] += 1


def describe_header(payload: bytes) -> dict[str, object]:
    padded = payload[:20].ljust(20, b"\0")
    return {
        "header_hex": payload[:20].hex(),
        "u32_0": struct.unpack_from("<I", padded, 0)[0],
        "u16_4": struct.unpack_from("<H", padded, 4)[0],
        "u16_6": struct.unpack_from("<H", padded, 6)[0],
        "u64_8": struct.unpack_from("<Q", padded, 8)[0],
        "u16_16": struct.unpack_from("<H", padded, 16)[0],
        "byte_18": padded[18],
        "byte_19": padded[19],
    }


def decode_original_ffmpeg(raw_path: Path, wav_path: Path, ffmpeg: str) -> tuple[bool, str]:
    # This intentionally matches the command documented by prsyahmi/v380.
    command = [
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-f",
        "s16le",
        "-ar",
        "8000",
        "-ac",
        "1",
        "-acodec",
        "adpcm_ima_ws",
        "-i",
        str(raw_path),
        "-c:a",
        "pcm_s16le",
        str(wav_path),
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    return result.returncode == 0, result.stderr.decode("utf-8", "replace").strip()


def write_ima_wav(blocks: list[bytes], output_path: Path, sample_rate: int = 8000) -> dict[str, int]:
    """Wrap camera IMA blocks in a standard Microsoft IMA ADPCM WAVE file."""
    if not blocks:
        raise DumpError("cannot write an IMA WAV without blocks")
    block_align = len(blocks[0])
    if block_align < 5 or any(len(block) != block_align for block in blocks):
        raise DumpError("IMA WAV blocks must have one constant size of at least 5 bytes")

    # Mono IMA WAV: a four-byte block header contains predictor, step index,
    # and reserved byte.  The predictor itself is the first decoded sample.
    samples_per_block = 1 + (block_align - 4) * 2
    average_bytes_per_second = sample_rate * block_align // samples_per_block
    sample_count = len(blocks) * samples_per_block
    data = b"".join(blocks)
    fmt = struct.pack(
        "<HHIIHHHH",
        0x0011,  # WAVE_FORMAT_IMA_ADPCM
        1,
        sample_rate,
        average_bytes_per_second,
        block_align,
        4,
        2,  # cbSize
        samples_per_block,
    )
    body = (
        b"WAVE"
        + b"fmt "
        + struct.pack("<I", len(fmt))
        + fmt
        + b"fact"
        + struct.pack("<II", 4, sample_count)
        + b"data"
        + struct.pack("<I", len(data))
        + data
    )
    output_path.write_bytes(b"RIFF" + struct.pack("<I", len(body)) + body)
    return {
        "block_align": block_align,
        "samples_per_block": samples_per_block,
        "sample_count": sample_count,
        "sample_rate": sample_rate,
    }


def decode_wave_ffmpeg(source_path: Path, wav_path: Path, ffmpeg: str) -> tuple[bool, str]:
    command = [
        ffmpeg,
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(source_path),
        "-c:a",
        "pcm_s16le",
        str(wav_path),
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    return result.returncode == 0, result.stderr.decode("utf-8", "replace").strip()


def pcm_metrics(wav_path: Path, window_seconds: float, csv_path: Path) -> dict[str, object]:
    with wave.open(str(wav_path), "rb") as source:
        channels = source.getnchannels()
        rate = source.getframerate()
        width = source.getsampwidth()
        frames = source.getnframes()
        if channels != 1 or width != 2:
            raise DumpError(f"unexpected WAV format in {wav_path}: channels={channels}, width={width}")
        pcm = source.readframes(frames)

    samples = struct.unpack(f"<{len(pcm) // 2}h", pcm) if pcm else ()
    window_size = max(1, round(rate * window_seconds))
    rows: list[dict[str, object]] = []
    previous = 0
    for start in range(0, len(samples), window_size):
        window = samples[start : start + window_size]
        if not window:
            continue
        squares = sum(sample * sample for sample in window)
        deltas = []
        last = previous if start else window[0]
        zero_crossings = 0
        clipped = 0
        for sample in window:
            deltas.append(abs(sample - last))
            if (sample < 0 <= last) or (last < 0 <= sample):
                zero_crossings += 1
            if abs(sample) >= 32760:
                clipped += 1
            last = sample
        previous = window[-1]
        rows.append(
            {
                "start_seconds": round(start / rate, 6),
                "samples": len(window),
                "rms": round(math.sqrt(squares / len(window)), 3),
                "peak": max(abs(sample) for sample in window),
                "max_delta": max(deltas),
                "mean_delta": round(statistics.fmean(deltas), 3),
                "zero_crossing_rate": round(zero_crossings / len(window), 6),
                "clipped_fraction": round(clipped / len(window), 8),
            }
        )

    with csv_path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]) if rows else ["start_seconds"])
        writer.writeheader()
        writer.writerows(rows)

    return {
        "sample_rate": rate,
        "samples": len(samples),
        "duration_seconds": round(len(samples) / rate, 6),
        "metric_windows": len(rows),
        "largest_sample_delta": max((int(row["max_delta"]) for row in rows), default=0),
        "largest_window_rms": max((float(row["rms"]) for row in rows), default=0),
    }


def analyze(args: argparse.Namespace) -> None:
    dump_path = Path(args.dump)
    output_dir = Path(args.output_dir) if args.output_dir else dump_path.with_suffix("").with_name(dump_path.stem + "-analysis")
    output_dir.mkdir(parents=True, exist_ok=True)
    offsets = sorted(set(args.offsets))
    if any(offset < 0 or offset > 256 for offset in offsets):
        raise DumpError("audio offsets must be between 0 and 256")

    metadata, chunks = read_dump(dump_path)
    errors: list[str] = []
    counters: Counter = Counter()
    frame_sizes: dict[int, Counter] = defaultdict(Counter)
    video_frames: list[CompleteFrame] = []
    audio16_frames: list[CompleteFrame] = []
    audio1a_frames: list[CompleteFrame] = []
    first_ns: int | None = None
    last_ns: int | None = None

    for frame in assemble_frames(parse_fragments(chunks, errors), errors, counters):
        first_ns = frame.started_ns if first_ns is None else min(first_ns, frame.started_ns)
        last_ns = frame.completed_ns if last_ns is None else max(last_ns, frame.completed_ns)
        frame_sizes[frame.frame_type][len(frame.payload)] += 1
        if frame.frame_type in (0x00, 0x01, 0x28, 0x29):
            video_frames.append(frame)
        elif frame.frame_type == 0x16:
            audio16_frames.append(frame)
        elif frame.frame_type == 0x1A:
            audio1a_frames.append(frame)

    capture_span = ((last_ns - first_ns) / 1_000_000_000) if first_ns is not None and last_ns is not None else 0.0
    headers_csv = output_dir / "audio-0x16-frames.csv"
    with headers_csv.open("w", newline="", encoding="utf-8") as output:
        fields = [
            "index", "arrival_seconds", "frame_bytes", "fragments", "header_hex",
            "u32_0", "u16_4", "u16_6", "u64_8", "u16_16", "byte_18", "byte_19",
        ]
        writer = csv.DictWriter(output, fieldnames=fields)
        writer.writeheader()
        origin = audio16_frames[0].started_ns if audio16_frames else 0
        for index, frame in enumerate(audio16_frames):
            writer.writerow(
                {
                    "index": index,
                    "arrival_seconds": f"{(frame.started_ns - origin) / 1_000_000_000:.9f}",
                    "frame_bytes": len(frame.payload),
                    "fragments": frame.fragments,
                    **describe_header(frame.payload),
                }
            )

    ffmpeg = shutil.which(args.ffmpeg)
    candidates: dict[str, object] = {}
    for offset in offsets:
        candidate_name = f"audio-0x16-offset-{offset}"
        raw_path = output_dir / f"{candidate_name}.adpcm"
        encoded_bytes = 0
        skipped_frames = 0
        with raw_path.open("wb") as output:
            for frame in audio16_frames:
                if len(frame.payload) <= offset:
                    skipped_frames += 1
                    continue
                payload = frame.payload[offset:]
                output.write(payload)
                encoded_bytes += len(payload)

        details: dict[str, object] = {
            "offset": offset,
            "raw_file": str(raw_path),
            "encoded_bytes": encoded_bytes,
            "skipped_frames": skipped_frames,
            "expected_pcm_samples_at_two_per_byte": encoded_bytes * 2,
            "implied_sample_rate_from_arrival_time": round(encoded_bytes * 2 / capture_span, 3) if capture_span else None,
        }
        if ffmpeg and encoded_bytes:
            wav_path = output_dir / f"{candidate_name}.wav"
            ok, message = decode_original_ffmpeg(raw_path, wav_path, ffmpeg)
            details["ffmpeg_ok"] = ok
            if message:
                details["ffmpeg_message"] = message
            if ok:
                metrics_csv = output_dir / f"{candidate_name}-metrics.csv"
                details["wav_file"] = str(wav_path)
                details["metrics_file"] = str(metrics_csv)
                details["pcm"] = pcm_metrics(wav_path, args.metric_window, metrics_csv)
        elif not ffmpeg:
            details["ffmpeg_ok"] = False
            details["ffmpeg_message"] = f"executable not found: {args.ffmpeg}"
        candidates[candidate_name] = details

    block_candidate: dict[str, object] = {
        "operation": (
            "remove the 16-byte V380 frame header; preserve the following "
            "4-byte IMA WAV state header; decode every complete block"
        )
    }
    valid_ima_blocks: list[bytes] = []
    invalid_ima_headers = 0
    block_sizes: Counter = Counter()
    predictors: list[int] = []
    step_indexes: list[int] = []
    reserved_values: Counter = Counter()
    for frame in audio16_frames:
        if len(frame.payload) < 20:
            invalid_ima_headers += 1
            continue
        block = frame.payload[16:]
        predictor, step_index, reserved = struct.unpack_from("<hBB", block, 0)
        block_sizes[len(block)] += 1
        predictors.append(predictor)
        step_indexes.append(step_index)
        reserved_values[reserved] += 1
        if step_index > 88 or reserved != 0:
            invalid_ima_headers += 1
            continue
        valid_ima_blocks.append(block)

    block_candidate.update(
        {
            "frames": len(audio16_frames),
            "valid_block_headers": len(valid_ima_blocks),
            "invalid_block_headers": invalid_ima_headers,
            "block_size_histogram": dict(sorted(block_sizes.items())),
            "predictor_min": min(predictors) if predictors else None,
            "predictor_max": max(predictors) if predictors else None,
            "step_index_min": min(step_indexes) if step_indexes else None,
            "step_index_max": max(step_indexes) if step_indexes else None,
            "reserved_value_histogram": dict(sorted(reserved_values.items())),
        }
    )
    if valid_ima_blocks and len(valid_ima_blocks) == len(audio16_frames) and len(block_sizes) == 1:
        encoded_wav = output_dir / "audio-0x16-frame-reset.ima.wav"
        wave_format = write_ima_wav(valid_ima_blocks, encoded_wav)
        block_candidate["encoded_wav_file"] = str(encoded_wav)
        block_candidate["wave_format"] = wave_format
        block_candidate["implied_sample_rate_from_arrival_time"] = (
            round(wave_format["sample_count"] / capture_span, 3) if capture_span else None
        )
        if ffmpeg:
            pcm_wav = output_dir / "audio-0x16-frame-reset.pcm.wav"
            ok, message = decode_wave_ffmpeg(encoded_wav, pcm_wav, ffmpeg)
            block_candidate["ffmpeg_ok"] = ok
            if message:
                block_candidate["ffmpeg_message"] = message
            if ok:
                metrics_csv = output_dir / "audio-0x16-frame-reset-metrics.csv"
                block_candidate["pcm_wav_file"] = str(pcm_wav)
                block_candidate["metrics_file"] = str(metrics_csv)
                block_candidate["pcm"] = pcm_metrics(pcm_wav, args.metric_window, metrics_csv)
        else:
            block_candidate["ffmpeg_ok"] = False
            block_candidate["ffmpeg_message"] = f"executable not found: {args.ffmpeg}"
    candidates["audio-0x16-frame-reset-ima-wav"] = block_candidate

    if audio1a_frames:
        alaw_path = output_dir / "audio-0x1a-offset-16.alaw"
        with alaw_path.open("wb") as output:
            for frame in audio1a_frames:
                if len(frame.payload) > 16:
                    output.write(frame.payload[16:])

    video_timestamps = [
        struct.unpack_from("<Q", frame.payload, 8)[0]
        for frame in video_frames
        if len(frame.payload) >= 16
    ]
    video_timestamp_span_ms = (
        video_timestamps[-1] - video_timestamps[0]
        if len(video_timestamps) > 1 and video_timestamps[-1] >= video_timestamps[0]
        else 0
    )
    login_fps = int(metadata.get("stream_profile", {}).get("fps", 0))
    detected_video_fps = (
        (len(video_timestamps) - 1) * 1000 / video_timestamp_span_ms
        if video_timestamp_span_ms
        else None
    )
    reset_wave = block_candidate.get("wave_format", {})
    audio_duration_seconds = (
        reset_wave.get("sample_count", 0) / reset_wave.get("sample_rate", 8000)
        if reset_wave
        else None
    )

    report: dict[str, object] = {
        "dump": str(dump_path),
        "metadata": metadata,
        "capture_frame_span_seconds": round(capture_span, 6),
        "counters": dict(sorted(counters.items())),
        "frame_size_histograms": {
            f"0x{frame_type:02x}": dict(sorted(sizes.items()))
            for frame_type, sizes in sorted(frame_sizes.items())
        },
        "audio_0x16_frames": len(audio16_frames),
        "audio_0x1a_frames": len(audio1a_frames),
        "audio_0x16_headers_csv": str(headers_csv),
        "timing": {
            "login_response_value_previously_treated_as_fps": login_fps,
            "video_frames": len(video_frames),
            "video_camera_timestamp_span_seconds": round(video_timestamp_span_ms / 1000, 6),
            "video_fps_from_camera_timestamps": round(detected_video_fps, 6) if detected_video_fps else None,
            "video_duration_if_login_value_were_used_seconds": (
                round(len(video_frames) / login_fps, 6) if login_fps else None
            ),
            "ima_wav_audio_duration_seconds": round(audio_duration_seconds, 6) if audio_duration_seconds else None,
        },
        "errors": errors,
        "candidates": candidates,
        "baseline": {
            "name": "prsyahmi-v380",
            "operation": "reassemble type 0x16, remove 20 bytes from every complete frame, concatenate, decode continuously with FFmpeg adpcm_ima_ws",
            "candidate": "audio-0x16-offset-20",
        },
        "detected_format": {
            "name": "V380 header followed by mono IMA WAV block",
            "operation": "remove 16 bytes, keep predictor/step-index/reserved block header, reset decoder state for every camera frame",
            "candidate": "audio-0x16-frame-reset-ima-wav",
        },
    }
    report_path = output_dir / "report.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"dump: {dump_path}")
    print(f"analysis: {output_dir}")
    print(f"frame span: {capture_span:.3f}s")
    print(f"audio frames: 0x16={len(audio16_frames)}, 0x1a={len(audio1a_frames)}")
    print(
        "fragment errors: "
        f"discontinuities={counters['fragment_discontinuities']}, "
        f"incomplete={counters['incomplete_frames']}, parser={len(errors)}"
    )
    for name, candidate in candidates.items():
        rate = candidate.get("implied_sample_rate_from_arrival_time")
        status = "decoded" if candidate.get("ffmpeg_ok") else "raw only"
        encoded_bytes = candidate.get("encoded_bytes", "IMA blocks")
        print(f"{name}: {encoded_bytes}, implied rate={rate}, {status}")
    print(f"report: {report_path}")
    print("baseline WAV: " + str(output_dir / "audio-0x16-offset-20.wav"))
    print("frame-reset WAV: " + str(output_dir / "audio-0x16-frame-reset.pcm.wav"))


def parse_offsets(value: str) -> list[int]:
    try:
        return [int(item.strip()) for item in value.split(",") if item.strip()]
    except ValueError as exc:
        raise argparse.ArgumentTypeError("offsets must be comma-separated integers") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze a v380_capture.py dump without connecting to the camera."
    )
    parser.add_argument("dump", help=".v380dump file created by v380_capture.py")
    parser.add_argument("--output-dir")
    parser.add_argument(
        "--offsets",
        type=parse_offsets,
        default=[0, 16, 18, 20],
        help="0x16 per-frame payload offsets to extract and decode (default: 0,16,18,20)",
    )
    parser.add_argument("--ffmpeg", default="ffmpeg")
    parser.add_argument("--metric-window", type=float, default=5.0)
    args = parser.parse_args()
    if args.metric_window <= 0:
        parser.error("--metric-window must be positive")
    return args


def main() -> int:
    try:
        analyze(parse_args())
        return 0
    except (OSError, ValueError, DumpError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
