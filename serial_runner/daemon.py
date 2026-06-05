"""serial-runner daemon: owns one serial port, logs to disk, accepts input via FIFO,
runs trigger engine with byte-level pattern detection."""
import serial, os, sys, time, threading, re, stat, glob, signal
from dataclasses import dataclass, field
from types import SimpleNamespace
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
        auto_fallback_port: bool = False,
        out_port: Optional[str] = None,
    ):
        self.port = port
        self.baud = baud
        self.auto_fallback_port = auto_fallback_port
        # Optional separate TX port. When set, bytes from the FIFO and trigger
        # `send()` calls go to this port instead of the main `port`. Use case:
        # asymmetric wiring where a clean RX path and a clean TX path live on
        # different physical ports (e.g., TTL header for RX, RS-232 for TX).
        self.out_port = out_port
        self.ser_out = None
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
        self.connected_event = threading.Event()
        # Path to plugin YAML, set by the CLI when --plugin is given.
        # Enables SIGHUP-driven hot-reload of triggers without restarting the daemon.
        self.plugin_path: Optional[str] = None

    def wait_connected(self, timeout: Optional[float] = None) -> bool:
        """Block until the serial port is open (or timeout). Returns True if connected."""
        return self.connected_event.wait(timeout)

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
        tx = self.ser_out if self.ser_out is not None else self.ser
        if tx is None:
            print("[daemon] cannot send: no active serial port", flush=True)
            return
        try:
            tx.write(data); tx.flush()
        except (serial.SerialException, OSError) as e:
            # If the out-port fails mid-flight, log + drop it and try the main
            # port. We never fall back the other direction (main port failures
            # are handled by the reader's reopen loop).
            if tx is self.ser_out:
                print(f"[daemon] out-port write failed: {e} — falling back to main port", flush=True)
                self.ser_out = None
                if self.ser is not None:
                    try:
                        self.ser.write(data); self.ser.flush()
                    except (serial.SerialException, OSError) as fb:
                        print(f"[daemon] fallback write also failed: {fb}", flush=True)
            else:
                print(f"[daemon] write failed: {e}", flush=True)

    def type_chars(self, s: str, delay: float = 0.10, end: str = "\r") -> None:
        """Char-by-char with delay — for prompts that drop chars at fast input."""
        for c in s:
            self.send(c); time.sleep(delay)
        if end:
            self.send(end)

    def _reader(self) -> None:
        while self._running:
            try:
                data = self.ser.read(4096)
            except (serial.SerialException, OSError) as e:
                print(f"[daemon] serial read err: {e} — reopening", flush=True)
                with self.lock:
                    self.logf.write(f"\n[daemon] PORT LOST: {e}\n".encode())
                self._reopen_serial()
                continue
            if not data:
                continue
            sys.stdout.buffer.write(data); sys.stdout.buffer.flush()
            with self.lock:
                self.logf.write(data)
                self.det_buf.extend(data)
                if len(self.det_buf) > self.DET_BUF_MAX:
                    del self.det_buf[: -self.DET_BUF_MAX]
                snap = bytes(self.det_buf)
                # Snapshot triggers under the lock so a concurrent reload
                # (which atomically reassigns self.triggers) can't race us.
                triggers = list(self.triggers)
            now = time.time()
            for t in triggers:
                if self.is_disabled(t.name):
                    continue
                if now - t.last_fired < t.debounce_s:
                    continue
                if t.matches(snap):
                    t.last_fired = now
                    with self.lock:
                        self.det_buf.clear()
                    threading.Thread(target=self._fire, args=(t,), daemon=True).start()

    def _find_port(self):
        """Return the configured port if it exists.
        If auto_fallback_port is set, fall back to any /dev/ttyUSB*/ttyACM*."""
        if os.path.exists(self.port):
            return self.port
        if not self.auto_fallback_port:
            return None
        candidates = sorted(glob.glob("/dev/ttyUSB*") + glob.glob("/dev/ttyACM*"))
        return candidates[0] if candidates else None

    def _open_serial(self):
        """Try to open the serial port. True on success."""
        port = self._find_port()
        if not port:
            return False
        try:
            self.ser = serial.Serial(port, self.baud, bytesize=8, parity="N", stopbits=1, timeout=0.05)
            if port != self.port:
                print(f"[daemon] WARN: port {self.port} unavailable, falling back to {port} (auto-fallback enabled)", flush=True)
                with self.lock:
                    self.logf.write(f"\n[daemon] PORT REMAPPED: {self.port} -> {port}\n".encode())
            self.connected_event.set()
            return True
        except (serial.SerialException, OSError) as e:
            print(f"[daemon] open {port} failed: {e}", flush=True)
            return False

    def _reopen_serial(self):
        """Block-with-backoff until the port comes back."""
        self.connected_event.clear()
        if self.ser is not None:
            try:
                self.ser.close()
            except Exception:
                pass
        backoff = 0.5
        while self._running:
            if self._open_serial():
                print(f"[daemon] reconnected to {self.ser.port}", flush=True)
                with self.lock:
                    self.logf.write(f"\n[daemon] PORT RECONNECTED: {self.ser.port}\n".encode())
                return
            time.sleep(backoff)
            backoff = min(backoff * 1.5, 5.0)

    def _fire(self, t: Trigger) -> None:
        print(f"[daemon] TRIGGER {t.name} fired", flush=True)
        with self.lock:
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
                    # Route through self.send() so the FIFO path gets the same
                    # port selection + error handling as trigger actions.
                    self.send(data)
            except Exception as e:
                print(f"[daemon] fifo err: {e}", flush=True)
                time.sleep(0.5)

    def _send_break(self, duration: float = 0.25) -> None:
        """Drive a serial BREAK condition on the TX port.

        Useful for entering kernel Magic SysRq mode, interrupting some U-Boot
        variants, and recovering stuck remote consoles. Triggered via SIGUSR1
        — see start()."""
        tx = self.ser_out if self.ser_out is not None else self.ser
        if tx is None:
            print("[daemon] BREAK requested but no TX port is open", flush=True)
            return
        try:
            tx.send_break(duration=duration)
            with self.lock:
                self.logf.write(f"\n[daemon] BREAK sent ({duration:.2f}s) on {tx.port}\n".encode())
            print(f"[daemon] BREAK sent ({duration:.2f}s) on {tx.port}", flush=True)
        except Exception as e:
            print(f"[daemon] BREAK error: {e}", flush=True)

    def _reload_plugin(self) -> None:
        """Re-read the YAML at self.plugin_path and atomically swap triggers.

        Bad YAML/missing file: log and keep the old triggers running.
        Triggered via SIGHUP — see start()."""
        if self.plugin_path is None:
            print("[daemon] reload requested but no plugin path set; ignoring", flush=True)
            return
        try:
            from . import runbook as rb
            book = rb.load(self.plugin_path)
            if not isinstance(book, dict):
                print(f"[daemon] reload error: plugin YAML is not a mapping: {type(book).__name__}", flush=True)
                return
            ctx = rb.RunbookContext(daemon=self, vars=dict(book.get("vars", {})))
            # Build the new trigger list into a throwaway holder first.  If
            # install_triggers raises (bad action, invalid pattern, etc.) the
            # live self.triggers stays untouched and the daemon keeps running
            # on the previous config.
            holder = SimpleNamespace(triggers=[])
            holder.add_trigger = holder.triggers.append
            rb.install_triggers(book, holder, ctx)
            with self.lock:
                self.triggers = holder.triggers
            print(f"[daemon] plugin reloaded: {len(holder.triggers)} triggers", flush=True)
        except Exception as e:
            print(f"[daemon] reload error ({type(e).__name__}): {e} — keeping existing triggers", flush=True)

    def start(self) -> None:
        # Open log first so reconnection messages have a destination
        self.logf = open(self.log_path, "ab", buffering=0)
        # Open serial (wait for it if not present yet)
        if not self._open_serial():
            print(f"port {self.port} not present — waiting...", flush=True)
            self._running = True
            self._reopen_serial()
        # Open the optional separate TX port, if configured. Asymmetric wiring:
        # the main port handles reads (board → us) and `out_port` handles writes
        # (us → board). Useful when only one direction is clean on a given path.
        if self.out_port:
            try:
                self.ser_out = serial.Serial(
                    self.out_port, self.baud,
                    bytesize=8, parity="N", stopbits=1, timeout=0.05,
                    rtscts=False, xonxoff=False,
                )
                print(f"[daemon] tx port: {self.out_port}@{self.baud}", flush=True)
            except (serial.SerialException, OSError) as e:
                print(f"[daemon] WARN: opening out-port {self.out_port} failed: {e} — falling back to main port for TX", flush=True)
                self.ser_out = None
        # Create FIFO if missing. CRUCIAL: don't unlink an existing FIFO —
        # keys.py readers may already have it open by inode; unlinking creates
        # an inode race where they write into an orphaned pipe nobody reads.
        # Only recreate if the path exists but isn't a FIFO.
        try:
            st = os.lstat(self.fifo_path)
            if not stat.S_ISFIFO(st.st_mode):
                os.unlink(self.fifo_path)
                raise FileNotFoundError
        except FileNotFoundError:
            os.mkfifo(self.fifo_path)
            os.chmod(self.fifo_path, 0o666)

        print(f"[daemon] {self.port}@{self.baud}", flush=True)
        print(f"[daemon] log {self.log_path}", flush=True)
        print(f"[daemon] fifo {self.fifo_path}", flush=True)
        print(f"[daemon] triggers: {', '.join(t.name for t in self.triggers)}", flush=True)

        # SIGHUP → reload the plugin YAML and swap triggers. Threaded so the
        # signal handler returns immediately (no I/O in the handler itself).
        # SIGHUP does not exist on Windows; guard the registration so the
        # daemon still starts there (just without hot-reload).
        if hasattr(signal, "SIGHUP"):
            signal.signal(signal.SIGHUP, lambda *_: threading.Thread(target=self._reload_plugin, daemon=True).start())
        else:
            print("[daemon] SIGHUP not available on this platform; plugin hot-reload disabled", flush=True)

        # SIGUSR1 → drive a serial BREAK on the TX port. Handler returns
        # immediately because send_break blocks for `duration` seconds.
        if hasattr(signal, "SIGUSR1"):
            signal.signal(signal.SIGUSR1, lambda *_: threading.Thread(target=self._send_break, daemon=True).start())

        self._running = True
        threading.Thread(target=self._reader, daemon=True).start()
        threading.Thread(target=self._from_fifo, daemon=True).start()
        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            self._running = False
            print("\n[daemon] stopped", flush=True)
