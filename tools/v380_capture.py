#!/usr/bin/env python3
"""Capture the byte stream sent by a V380 camera after stream authorization.

The output is deliberately not parsed into video/audio frames.  Every recv()
chunk is stored verbatim with its monotonic arrival time, so framing and codec
assumptions can be changed later without reconnecting to the camera.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import secrets
import shutil
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path


DUMP_MAGIC = b"V380DMP1"
RECORD_HEADER = struct.Struct("<QI")  # elapsed monotonic ns, byte count
ALLOWED_RANDOM_KEY = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
STATIC_PASSWORD_KEY = b"macrovideo+*#!^@"


class ProtocolError(RuntimeError):
    pass


def aes_ecb_encrypt(key: bytes, data: bytes) -> bytes:
    """AES-128-ECB without padding, using an available local implementation."""
    if len(key) != 16 or len(data) % 16:
        raise ValueError("AES key must be 16 bytes and data must be block aligned")

    try:
        from Crypto.Cipher import AES  # type: ignore[import-not-found]

        return AES.new(key, AES.MODE_ECB).encrypt(data)
    except ImportError:
        pass

    try:
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes  # type: ignore[import-not-found]

        encryptor = Cipher(algorithms.AES(key), modes.ECB()).encryptor()
        return encryptor.update(data) + encryptor.finalize()
    except ImportError:
        pass

    openssl = shutil.which("openssl")
    if openssl is None:
        raise RuntimeError(
            "AES backend not found: install openssl, pycryptodome, or cryptography"
        )
    result = subprocess.run(
        [openssl, "enc", "-aes-128-ecb", "-nopad", "-nosalt", "-K", key.hex()],
        input=data,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "openssl AES failed: " + result.stderr.decode("utf-8", "replace").strip()
        )
    if len(result.stdout) != len(data):
        raise RuntimeError("openssl AES returned an unexpected byte count")
    return result.stdout


def encode_password(password: str) -> bytes:
    password_bytes = password.encode("ascii")
    if len(password_bytes) > 48:
        raise ValueError("V380 password must fit in 48 ASCII bytes")

    random_key = "".join(secrets.choice(ALLOWED_RANDOM_KEY) for _ in range(16)).encode("ascii")
    padded = password_bytes.ljust(48, b"\0")
    encrypted = aes_ecb_encrypt(STATIC_PASSWORD_KEY, padded)
    encrypted = aes_ecb_encrypt(random_key, encrypted)
    return random_key + encrypted


def put_bytes(target: bytearray, offset: int, value: bytes, limit: int) -> None:
    target[offset : offset + min(len(value), limit)] = value[:limit]


def recv_exact(sock: socket.socket, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        part = sock.recv(size - len(result))
        if not part:
            raise EOFError(f"connection closed after {len(result)}/{size} bytes")
        result.extend(part)
    return bytes(result)


def recv_response(sock: socket.socket, maximum: int, minimum: int) -> bytes:
    """Read a fixed protocol reply, tolerating shorter firmware variants."""
    result = bytearray()
    original_timeout = sock.gettimeout()
    try:
        while len(result) < minimum:
            part = sock.recv(maximum - len(result))
            if not part:
                break
            result.extend(part)
        sock.settimeout(0.15)
        while len(result) < maximum:
            try:
                part = sock.recv(maximum - len(result))
            except socket.timeout:
                break
            if not part:
                break
            result.extend(part)
    finally:
        sock.settimeout(original_timeout)
    if len(result) < minimum:
        raise ProtocolError(f"short response: {len(result)} bytes, expected at least {minimum}")
    return bytes(result)


def authenticate(args: argparse.Namespace, password: str) -> tuple[int, int, int, bytes]:
    request = bytearray(520)
    struct.pack_into("<I", request, 0, 1167)
    struct.pack_into("<I", request, 4, 120)
    request[8] = 31
    struct.pack_into("<I", request, 9, 1)
    struct.pack_into("<I", request, 13, args.id)
    put_bytes(request, 49, args.username.encode("ascii"), 32)
    put_bytes(request, 81, encode_password(password), 64)

    with socket.create_connection((args.ip, args.port), args.timeout) as sock:
        sock.settimeout(args.timeout)
        sock.sendall(request)
        response = recv_exact(sock, 256)

    command, result = struct.unpack_from("<II", response, 0)
    if command != 1168:
        raise ProtocolError(f"authentication response command is {command}, expected 1168")
    if result != 1001:
        meanings = {1011: "invalid username", 1012: "invalid password", 1018: "invalid device id"}
        raise ProtocolError(f"authentication failed: {result} ({meanings.get(result, 'unknown error')})")

    device_version = response[12]
    ticket = struct.unpack_from("<I", response, 13)[0]
    session = struct.unpack_from("<I", response, 17)[0]
    return ticket, session, device_version, response


def stream_login(
    args: argparse.Namespace, ticket: int
) -> tuple[socket.socket, dict[str, int], bytes]:
    sock = socket.create_connection((args.ip, args.port), args.timeout)
    try:
        sock.settimeout(args.timeout)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, args.receive_buffer)

        request = bytearray(256)
        struct.pack_into("<I", request, 0, 301)
        struct.pack_into("<I", request, 4, args.id)
        struct.pack_into("<I", request, 8, 0)
        struct.pack_into("<H", request, 12, args.fps)
        struct.pack_into("<I", request, 14, ticket)
        struct.pack_into("<I", request, 22, 4097)  # audio enabled
        struct.pack_into("<I", request, 26, args.quality)
        sock.sendall(request)

        response = recv_response(sock, 412, 18)
        command, result = struct.unpack_from("<II", response, 0)
        if command != 401:
            raise ProtocolError(f"stream response command is {command}, expected 401")
        if result not in (402, 1001):
            raise ProtocolError(f"stream login failed: {result}")

        profile = {
            "result": result,
            "fps": struct.unpack_from("<H", response, 8)[0],
            "width": struct.unpack_from("<I", response, 10)[0],
            "height": struct.unpack_from("<I", response, 14)[0],
        }
        if len(response) >= 25:
            profile.update(
                max_packet_size=struct.unpack_from("<I", response, 18)[0],
                audio_frequency=response[22],
                audio_bits=response[23],
                audio_channels=response[24],
            )

        start = bytearray(256)
        struct.pack_into("<I", start, 0, 303)
        # This matches the currently working LAN request and the original client.
        struct.pack_into("<H", start, 4, 0x3001)
        sock.sendall(start)
        return sock, profile, response
    except Exception:
        sock.close()
        raise


def write_dump_header(output, metadata: dict[str, object]) -> None:
    encoded = json.dumps(metadata, ensure_ascii=False, sort_keys=True).encode("utf-8")
    output.write(DUMP_MAGIC)
    output.write(struct.pack("<I", len(encoded)))
    output.write(encoded)


def capture(args: argparse.Namespace, password: str) -> None:
    ticket, session, device_version, auth_response = authenticate(args, password)
    print(
        f"[AUTH] success ticket={ticket} deviceVersion={device_version} session={session}",
        file=sys.stderr,
    )

    sock, profile, stream_response = stream_login(args, ticket)
    print(
        f"[STREAM] login OK quality={args.quality} "
        f"video={profile['width']}x{profile['height']}@{profile['fps']} audio=on",
        file=sys.stderr,
    )

    metadata: dict[str, object] = {
        "format": "v380-post-auth-tcp",
        "format_version": 1,
        "created_unix_ns": time.time_ns(),
        "camera": {
            "ip": args.ip,
            "port": args.port,
            "id": args.id,
            "username": args.username,
            "quality": args.quality,
            "requested_fps": args.fps,
            "device_version": device_version,
        },
        "stream_profile": profile,
        "auth_response_hex": auth_response.hex(),
        "stream_response_hex": stream_response.hex(),
        "record": "<uint64 elapsed_monotonic_ns><uint32 length><raw recv bytes>",
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic_ns()
    deadline = started + int(args.duration * 1_000_000_000) if args.duration else None
    chunks = 0
    byte_count = 0
    last_report = started
    last_flush = started

    try:
        with output_path.open("xb" if not args.overwrite else "wb", buffering=1024 * 1024) as output:
            write_dump_header(output, metadata)
            sock.settimeout(1.0)
            print(f"[CAPTURE] writing raw TCP stream to {output_path}", file=sys.stderr)
            print("[CAPTURE] stop with Ctrl-C", file=sys.stderr)

            while deadline is None or time.monotonic_ns() < deadline:
                try:
                    data = sock.recv(args.chunk_size)
                except socket.timeout:
                    continue
                if not data:
                    raise EOFError("camera closed the stream connection")

                now = time.monotonic_ns()
                output.write(RECORD_HEADER.pack(now - started, len(data)))
                output.write(data)
                chunks += 1
                byte_count += len(data)

                if now - last_flush >= 1_000_000_000:
                    output.flush()
                    last_flush = now
                if now - last_report >= 10_000_000_000:
                    elapsed = (now - started) / 1_000_000_000
                    print(
                        f"[CAPTURE] {elapsed:.1f}s chunks={chunks} bytes={byte_count}",
                        file=sys.stderr,
                    )
                    last_report = now
    except KeyboardInterrupt:
        print("\n[CAPTURE] interrupted; completed records remain valid", file=sys.stderr)
    finally:
        sock.close()

    elapsed = max((time.monotonic_ns() - started) / 1_000_000_000, 0.001)
    print(
        f"[CAPTURE] saved {byte_count} raw bytes in {chunks} chunks over {elapsed:.1f}s: {output_path}",
        file=sys.stderr,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Authenticate to a LAN V380 camera and capture its post-auth TCP byte stream verbatim."
    )
    parser.add_argument("--ip", required=True, help="camera LAN address")
    parser.add_argument("--id", required=True, type=int, help="numeric camera/device id")
    parser.add_argument("--port", type=int, default=8800)
    parser.add_argument("--username", default="admin")
    parser.add_argument(
        "--password-env",
        default="V380_PASSWORD",
        help="environment variable containing the password; prompt when unset",
    )
    parser.add_argument("--quality", type=int, choices=(0, 1), default=1)
    parser.add_argument("--fps", type=int, default=20)
    parser.add_argument("--duration", type=float, default=0, help="seconds; 0 means until Ctrl-C")
    parser.add_argument("--output", default="v380-raw.v380dump")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--receive-buffer", type=int, default=1024 * 1024)
    parser.add_argument("--chunk-size", type=int, default=65536)
    args = parser.parse_args()
    if not 1 <= args.id <= 0xFFFFFFFF:
        parser.error("--id must fit in uint32")
    if args.duration < 0:
        parser.error("--duration cannot be negative")
    if args.fps <= 0 or args.fps > 0xFFFF:
        parser.error("--fps must be between 1 and 65535")
    return args


def main() -> int:
    args = parse_args()
    password = os.environ.get(args.password_env)
    if password is None:
        password = getpass.getpass("Camera password: ")
    if not password:
        print("error: empty camera password", file=sys.stderr)
        return 2
    try:
        capture(args, password)
        return 0
    except (OSError, ProtocolError, RuntimeError, ValueError, EOFError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
