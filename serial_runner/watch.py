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


def _clean(buf: bytes, drop_kernel_ts: bool) -> tuple[str, int]:
    """Decode + strip BEL/CR. Optionally drop kernel-timestamp lines."""
    text = buf.replace(b"\x07", b"").replace(b"\r", b"").decode("utf-8", "replace")
    if not drop_kernel_ts:
        return text, 0
    out_lines, dropped = [], 0
    for line in text.split("\n"):
        if KERNEL_TS.match(line):
            dropped += 1
            continue
        out_lines.append(line)
    return "\n".join(out_lines), dropped


def watch(
    log_path: str,
    interval_s: float = 5.0,
    drop_kernel_ts: bool = False,
    from_end: bool = True,
) -> None:
    """Poll the log file, emit a JSON line per tick when new bytes appear."""
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
            content, dropped = _clean(buf, drop_kernel_ts)
            tick = Tick(
                t=time.strftime("%H:%M:%S"),
                epoch=time.time(),
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
