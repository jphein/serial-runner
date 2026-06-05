"""Delta watcher: stream new serial-log bytes on an interval.

Emits one JSON object per tick (NDJSON to stdout). Skips empty ticks.
Designed as the structured context feed for an LLM channel.
"""
import os, time, json, re, sys
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class Tick:
    t: str                 # wall-clock HH:MM:SS
    epoch: float
    bytes_added: int
    content: str           # cleaned (BEL/CR stripped) new bytes
    kernel_lines_dropped: int = 0


KERNEL_TS = re.compile(r"^\[\s*\d+\.\d+\]")

# Precomputed translation table: keep printable ASCII (0x20-0x7e) + LF + CR,
# map everything else to '.'. Used by ``_clean`` via ``bytes.translate`` (C-fast).
_CLEAN_TABLE = bytes(
    b if (0x20 <= b <= 0x7e) or b in (0x0a, 0x0d) else 0x2e for b in range(256)
)


def _clean(buf: bytes, drop_kernel_ts: bool, clean_bytes: bool = False) -> tuple[str, int]:
    """Decode + strip BEL/CR. Optionally drop kernel-timestamp lines.

    When ``clean_bytes`` is True, also drop NUL bytes and map any byte outside
    printable ASCII (0x20-0x7e) plus LF/CR to '.' before decoding. This makes
    output from noisy USB-serial adapters (e.g. CH340 bit-error RX) readable
    while preserving line structure.
    """
    buf = buf.replace(b"\x07", b"")
    if clean_bytes:
        buf = buf.replace(b"\x00", b"")
        # Map any byte outside printable ASCII + LF + CR to '.' (C-implemented)
        buf = buf.translate(_CLEAN_TABLE)
    text = buf.replace(b"\r", b"").decode("utf-8", "replace")
    if not drop_kernel_ts:
        return text, 0
    out_lines, dropped = [], 0
    for line in text.split("\n"):
        if KERNEL_TS.match(line):
            dropped += 1
            continue
        out_lines.append(line)
    return "\n".join(out_lines), dropped


def _split_content(data: bytes, max_bytes: int) -> list[str]:
    """Split already-encoded UTF-8 bytes into chunks of at most max_bytes.

    Prefers to split on the nearest newline boundary at or before max_bytes;
    falls back to a hard byte-split if no newline is available in the window.
    Operates on encoded bytes so the byte-size guarantee holds; decoding back
    to str is safe because newline ('\\n' = 0x0A) is never part of a multi-byte
    UTF-8 sequence, and the hard-split fallback only triggers on chunks with
    no newline (e.g., binary-ish blobs), where 'utf-8','replace' decoding will
    repair any boundary-straddling byte sequences.
    """
    chunks: list[str] = []
    i = 0
    n = len(data)
    while i < n:
        end = min(i + max_bytes, n)
        if end < n:
            # look for the last newline in data[i:end]
            nl = data.rfind(b"\n", i, end)
            if nl != -1 and nl >= i:
                end = nl + 1  # include the newline
        chunks.append(data[i:end].decode("utf-8", "replace"))
        i = end
    return chunks or [""]


def watch(
    log_path: str,
    interval_s: float = 5.0,
    drop_kernel_ts: bool = False,
    from_end: bool = True,
    clean_bytes: bool = False,
    max_bytes_per_tick: "Optional[int]" = None,
) -> None:
    """Poll the log file, emit a JSON line per tick when new bytes appear.

    If max_bytes_per_tick is set and a tick's cleaned content exceeds it, the
    tick is split across multiple JSON lines (each tagged with chunk_index /
    chunk_total) so downstream consumers aren't flooded by huge bursts.
    """
    if max_bytes_per_tick is not None and max_bytes_per_tick <= 0:
        raise ValueError("max_bytes_per_tick must be greater than 0")
    prev_size = os.path.getsize(log_path) if (os.path.exists(log_path) and from_end) else 0
    while True:
        try:
            cur_size = os.path.getsize(log_path)
        except FileNotFoundError:
            time.sleep(interval_s); continue
        if cur_size > prev_size:
            delta = cur_size - prev_size
            with open(log_path, "rb") as f:
                f.seek(prev_size)
                buf = f.read(delta)
            content, dropped = _clean(buf, drop_kernel_ts, clean_bytes)
            t_str = time.strftime("%H:%M:%S")
            epoch = time.time()
            if max_bytes_per_tick is not None and len(content_bytes := content.encode("utf-8")) > max_bytes_per_tick:
                pieces = _split_content(content_bytes, max_bytes_per_tick)
                total = len(pieces)
                for idx, piece in enumerate(pieces):
                    obj = {
                        "t": t_str,
                        "epoch": epoch,
                        "bytes_added": len(piece.encode("utf-8")),
                        "content": piece,
                        "kernel_lines_dropped": dropped if idx == 0 else 0,
                        "chunk_index": idx,
                        "chunk_total": total,
                    }
                    sys.stdout.write(json.dumps(obj) + "\n")
                    sys.stdout.flush()
            else:
                tick = Tick(
                    t=t_str,
                    epoch=epoch,
                    bytes_added=delta,
                    content=content,
                    kernel_lines_dropped=dropped,
                )
                sys.stdout.write(json.dumps(asdict(tick)) + "\n")
                sys.stdout.flush()
            prev_size = cur_size
        elif cur_size < prev_size:
            # log rotated/truncated — restart from current end
            prev_size = cur_size
        time.sleep(interval_s)
