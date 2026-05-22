"""serial-runner daemon: owns one serial port, logs to disk, accepts input via FIFO,
runs trigger engine with byte-level pattern detection."""
import serial, os, sys, time, threading, re
from dataclasses import dataclass, field
from typing import Callable, Optional, Pattern, Union


@dataclass
class Trigger:
    """One trigger: when `pattern` appears in the byte stream, run `action`."""
    name: str
    pattern: Union[bytes, Pattern[bytes]]
    action: Callable[[], None]
    debounce_s: float = 30.0
    enabled: bool = True
    last_fired: float = 0.0

    def matches(self, buf: bytes) -> bool:
        if isinstance(self.pattern, bytes):
            return self.pattern in buf
        return self.pattern.search(buf) is not None


class Daemon:
    """Single-owner serial port wrapper with trigger engine + FIFO input."""

    DET_BUF_MAX = 256

    def __init__(
        self,
        port: str = "/dev/ttyUSB0",
        baud: int = 115200,
        log_path: str = None,
        fifo_path: str = None,
        state_dir: str = None,
    ):
        self.port = port
        self.baud = baud
        self.state_dir = state_dir or os.path.expanduser("~/.serial-runner")
        os.makedirs(self.state_dir, exist_ok=True)
        self.log_path = log_path or os.path.join(self.state_dir, "serial.log")
        self.fifo_path = fifo_path or os.path.join(self.state_dir, "input.fifo")
        self.ser: Optional[serial.Serial] = None
        self.logf = None
        self.det_buf = bytearray()
        self.lock = threading.Lock()
        self.triggers: list[Trigger] = []
        self._running = False

    def add_trigger(self, t: Trigger) -> None:
        self.triggers.append(t)

    def is_disabled(self, name: str) -> bool:
        return os.path.exists(os.path.join(self.state_dir, f"disable_{name.lower()}"))

    def disable_trigger(self, name: str) -> None:
        open(os.path.join(self.state_dir, f"disable_{name.lower()}"), "w").close()

    def enable_trigger(self, name: str) -> None:
        f = os.path.join(self.state_dir, f"disable_{name.lower()}")
        if os.path.exists(f):
            os.unlink(f)

    def send(self, data: Union[bytes, str]) -> None:
        if isinstance(data, str):
            data = data.encode()
        self.ser.write(data); self.ser.flush()

    def type_chars(self, s: str, delay: float = 0.10, end: str = "\r") -> None:
        """Char-by-char with delay — for prompts that drop chars at fast input."""
        for c in s:
            self.send(c); time.sleep(delay)
        if end:
            self.send(end)

    def _reader(self) -> None:
        while self._running:
            data = self.ser.read(4096)
            if not data:
                continue
            sys.stdout.buffer.write(data); sys.stdout.buffer.flush()
            self.logf.write(data)
            with self.lock:
                self.det_buf.extend(data)
                if len(self.det_buf) > self.DET_BUF_MAX:
                    del self.det_buf[: -self.DET_BUF_MAX]
                snap = bytes(self.det_buf)
            now = time.time()
            for t in self.triggers:
                if self.is_disabled(t.name):
                    continue
                if now - t.last_fired < t.debounce_s:
                    continue
                if t.matches(snap):
                    t.last_fired = now
                    with self.lock:
                        self.det_buf.clear()
                    threading.Thread(target=self._fire, args=(t,), daemon=True).start()

    def _fire(self, t: Trigger) -> None:
        print(f"[daemon] TRIGGER {t.name} fired", flush=True)
        self.logf.write(f"\n[daemon] AUTO {t.name} @ {time.strftime('%H:%M:%S')}\n".encode())
        try:
            t.action()
        except Exception as e:
            print(f"[daemon] TRIGGER {t.name} action error: {e}", flush=True)

    def _from_fifo(self) -> None:
        while self._running:
            try:
                fd = os.open(self.fifo_path, os.O_RDONLY)
                while self._running:
                    data = os.read(fd, 4096)
                    if not data:
                        os.close(fd); break
                    self.ser.write(data); self.ser.flush()
            except Exception as e:
                print(f"[daemon] fifo err: {e}", flush=True)
                time.sleep(0.5)

    def start(self) -> None:
        # Open serial
        self.ser = serial.Serial(
            self.port, self.baud,
            bytesize=8, parity="N", stopbits=1, timeout=0.05,
        )
        self.logf = open(self.log_path, "ab", buffering=0)
        # Create FIFO
        if os.path.exists(self.fifo_path):
            os.unlink(self.fifo_path)
        os.mkfifo(self.fifo_path)
        os.chmod(self.fifo_path, 0o666)

        print(f"[daemon] {self.port}@{self.baud}", flush=True)
        print(f"[daemon] log {self.log_path}", flush=True)
        print(f"[daemon] fifo {self.fifo_path}", flush=True)
        print(f"[daemon] triggers: {', '.join(t.name for t in self.triggers)}", flush=True)

        self._running = True
        threading.Thread(target=self._reader, daemon=True).start()
        threading.Thread(target=self._from_fifo, daemon=True).start()
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            self._running = False
            print("\n[daemon] stopped", flush=True)
