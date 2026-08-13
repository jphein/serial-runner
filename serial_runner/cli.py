"""serial-runner CLI."""
import argparse, os, re, shlex, sys, subprocess, threading, time
from . import runbook as rb
from .daemon import Daemon


def cmd_daemon(args):
    """Run the daemon. If --plugin is given, install its triggers (skip steps).
    The plugin path is remembered so SIGHUP can hot-reload the YAML without restarting."""
    d = Daemon(
        port=args.port,
        baud=args.baud,
        state_dir=args.state_dir,
        auto_fallback_port=getattr(args, "auto_fallback_port", False),
        out_port=getattr(args, "out_port", None),
    )
    plugin_path = getattr(args, "plugin", None)
    if plugin_path:
        plugin_path = _resolve_plugin(plugin_path)
        book = rb.load(plugin_path)
        if not isinstance(book, dict):
            raise ValueError(f"plugin {plugin_path!r} did not parse to a dict (got {type(book).__name__})")
        ctx = rb.RunbookContext(daemon=d, vars=dict(book.get("vars", {})))
        rb.install_triggers(book, d, ctx)
        # Remember the path so SIGHUP can re-read the YAML.
        d.plugin_path = plugin_path
        print(f"[cli] loaded plugin: {book.get('name')} from {plugin_path}", flush=True)
    d.start()


def cmd_watch(args):
    """Stream new serial-log bytes as NDJSON deltas every `--interval`s."""
    from .watch import watch
    log_path = args.log or os.path.join(args.state_dir, "serial.log")
    watch(
        log_path=log_path,
        interval_s=args.interval,
        drop_kernel_ts=args.drop_kernel_timestamps,
        from_end=not args.from_start,
        clean_bytes=args.clean,
        max_bytes_per_tick=args.max_bytes_per_tick,
    )


# Kernel timestamp regex matched against raw bytes (no decode needed).
_KERNEL_TS_RE = re.compile(rb"^\[\s*\d+\.\d+\]")
# Precomputed translation table: printable ASCII + \n + \r kept, everything else -> '.'.
_CLEAN_TABLE = bytes(
    b if (0x20 <= b <= 0x7e) or b in (0x0a, 0x0d) else 0x2e
    for b in range(256)
)


def _open_tail(log_path, from_end):
    """Open log file and seek per from_end; return (file, inode, size)."""
    f = open(log_path, "rb")
    st = os.fstat(f.fileno())
    if from_end:
        f.seek(0, 2)
    else:
        f.seek(0)
    return f, st.st_ino, st.st_size


def cmd_tail(args):
    """Follow the serial log in real time with optional garble-cleaning."""
    log_path = args.log or os.path.join(args.state_dir, "serial.log")

    # Wait briefly for the log file to exist.
    waited = 0.0
    while not os.path.exists(log_path) and waited < 10.0:
        time.sleep(0.5)
        waited += 0.5
    if not os.path.exists(log_path):
        print(f"[tail] log file not found: {log_path}", file=sys.stderr)
        return 1

    partial = b""
    last_data_t = time.monotonic()
    flush_after = args.flush_partial
    try:
        f, cur_ino, _ = _open_tail(log_path, from_end=(args.from_ == "end"))
        try:
            while True:
                chunk = f.read(8192)
                if not chunk:
                    # EOF: check for rotation/truncation before sleeping.
                    try:
                        st_disk = os.stat(log_path)
                        pos = f.tell()
                        rotated = st_disk.st_ino != cur_ino
                        truncated = st_disk.st_size < pos
                        if rotated or truncated:
                            # Flush any pending partial before reopening.
                            if partial and args.drop_kernel_timestamps:
                                sys.stdout.buffer.write(partial)
                                sys.stdout.buffer.flush()
                                partial = b""
                            f.close()
                            f, cur_ino, _ = _open_tail(log_path, from_end=False)
                            last_data_t = time.monotonic()
                            continue
                    except FileNotFoundError:
                        # Log briefly gone (mid-rotate); just wait and retry.
                        pass
                    # Idle: flush stale partial buffer so interactive prompts
                    # (login:, password:) that lack a trailing newline get shown.
                    if (
                        args.drop_kernel_timestamps
                        and partial
                        and (time.monotonic() - last_data_t) >= flush_after
                    ):
                        sys.stdout.buffer.write(partial)
                        sys.stdout.buffer.flush()
                        partial = b""
                    time.sleep(args.poll)
                    continue
                last_data_t = time.monotonic()
                # Apply byte-clean translation BEFORE the kernel-ts filter so
                # the partial buffer is already clean.
                if not args.raw:
                    chunk = chunk.replace(b"\x00", b"").replace(b"\x07", b"")
                    chunk = chunk.translate(_CLEAN_TABLE)
                if args.drop_kernel_timestamps:
                    partial += chunk
                    lines = partial.split(b"\n")
                    partial = lines[-1]
                    complete = lines[:-1]
                    out = b""
                    for line in complete:
                        # Strip trailing \r for matching but preserve in output.
                        test = line.rstrip(b"\r")
                        if _KERNEL_TS_RE.match(test):
                            continue
                        out += line + b"\n"
                    if out:
                        sys.stdout.buffer.write(out)
                        sys.stdout.buffer.flush()
                else:
                    sys.stdout.buffer.write(chunk)
                    sys.stdout.buffer.flush()
        finally:
            f.close()
    except KeyboardInterrupt:
        return 0


def cmd_ai(args):
    """Read NDJSON serial deltas from stdin; emit LLM suggestions on triggers."""
    from . import llm
    runbook = _resolve_plugin(args.plugin) if args.plugin else None
    llm.run(
        runbook_path=runbook,
        state_dir=args.state_dir,
        mode=args.mode,
        model=args.model,
        effort=args.effort,
        max_tokens=args.max_tokens,
        min_interval_s=args.min_interval,
        idle_threshold=args.idle_threshold,
        trigger_regex=args.trigger_regex,
        buffer_ticks=args.buffer_ticks,
        out_path=args.out,
    )


def cmd_keys(args):
    """Run the keystroke relay attached to the daemon's FIFO."""
    from . import keys
    return keys.main(args.fifo)


def cmd_send(args):
    """One-shot 'paste': write TEXT to the daemon's input FIFO (→ serial TX).

    The non-interactive complement to `keys`. Appends a carriage return by
    default (most device consoles — U-Boot, RT-Thread, BusyBox — expect CR,
    not LF). Use --hex for control bytes ('03' = Ctrl-C, '0d' = bare CR),
    --no-newline to omit the ending, or --end to override it."""
    fifo_path = args.fifo
    if not os.path.exists(fifo_path):
        print(f"[send] FIFO {fifo_path} missing — is the daemon up? (serial-runner up)",
              file=sys.stderr)
        return 1
    if args.hex:
        try:
            payload = bytes.fromhex(args.text.replace(" ", ""))
        except ValueError as e:
            print(f"[send] bad --hex value {args.text!r}: {e}", file=sys.stderr)
            return 2
    else:
        payload = args.text.encode("utf-8", "replace")
        if not args.no_newline:
            payload += args.end.encode()
    # Non-blocking open so a FIFO with no reader (daemon down) fails loudly
    # with ENXIO instead of hanging forever waiting for one.
    try:
        fd = os.open(fifo_path, os.O_WRONLY | os.O_NONBLOCK)
    except OSError as e:
        print(f"[send] cannot open FIFO ({e}) — no daemon reading it?", file=sys.stderr)
        return 1
    try:
        os.write(fd, payload)
    finally:
        os.close(fd)
    return 0


def cmd_scan(args):
    """Sweep candidate baud rates on a FREE port; rank by printable-ASCII ratio.

    A pad-and-baud finder for board bring-up. Stop any running daemon first
    (it owns the port). A rate that yields mostly-printable bytes with newlines
    is the console baud; if NO rate decodes, the pad is probably not a UART TX
    (a continuous clock/audio/data line reads as garbage at every baud)."""
    import serial as _serial
    if args.rates:
        rates = [int(r) for r in args.rates.replace(" ", "").split(",") if r]
    else:
        rates = [115200, 921600, 460800, 230400, 256000, 74880,
                 57600, 38400, 19200, 9600, 1500000, 2000000]

    def score(data: bytes) -> float:
        if not data:
            return 0.0
        return sum(1 for c in data if c in (9, 10, 13) or 32 <= c <= 126) / len(data)

    def samp(data: bytes, n: int = 90) -> str:
        return "".join(chr(c) if 32 <= c <= 126 else "." for c in data[:n])

    results = []
    for b in rates:
        try:
            s = _serial.Serial(args.port, b, timeout=0.3)
        except Exception as e:
            print(f"{b:>8}: OPEN ERR {e}", file=sys.stderr)
            continue
        try:
            s.reset_input_buffer()
            buf = b""
            t0 = time.time()
            while time.time() - t0 < args.dwell:
                buf += s.read(512)
        finally:
            s.close()
        r = score(buf)
        results.append((r, len(buf), b, samp(buf)))
        print(f"{b:>8}: {len(buf):>5}B  printable={r:0.2f}  |{samp(buf)}|", flush=True)

    results.sort(reverse=True)
    print("\n== ranked (most printable first) ==")
    for r, n, b, sp in results[:5]:
        flag = "   <-- looks like TEXT" if (r > 0.75 and n > 8) else ""
        print(f"  {b:>8} baud  printable={r:0.2f}  {n}B{flag}")
    if results and results[0][0] > 0.75 and results[0][1] > 8:
        print(f"\n[scan] likely console baud: {results[0][2]}  "
              f"(bring the daemon up with --baud {results[0][2]})")
    else:
        print("\n[scan] no rate produced clean text. Likely NOT a UART TX pad "
              "(continuous clock/audio/data reads as garbage at every baud), or the "
              "line only emits a boot log at reset — rescan while power-cycling the board.")
    return 0


def cmd_break(args):
    """Tell a running daemon to drive a serial BREAK on its TX port.

    Useful for entering kernel Magic SysRq mode, interrupting certain
    bootloaders, and recovering stuck remote consoles."""
    import signal as _signal
    import subprocess as _sp
    # Find the daemon PID — pgrep is reliable and avoids us having to parse /proc.
    pid_out = _sp.run(
        ["pgrep", "-f", "serial_runner.cli daemon"],
        capture_output=True, text=True,
    )
    pids = [int(p) for p in pid_out.stdout.split() if p.isdigit()]
    if not pids:
        print("[break] no daemon process found", file=sys.stderr)
        return 1
    if len(pids) > 1:
        print(f"[break] multiple daemon processes found ({pids}); sending to all", file=sys.stderr)
    for pid in pids:
        try:
            os.kill(pid, _signal.SIGUSR1)
            print(f"[break] SIGUSR1 → pid {pid}")
        except ProcessLookupError:
            print(f"[break] pid {pid} gone", file=sys.stderr)
    return 0


def cmd_run(args):
    """Load a runbook plugin and execute it, with the daemon serving alongside."""
    plugin_path = _resolve_plugin(args.plugin)
    book = rb.load(plugin_path)
    print(f"[cli] loaded plugin: {book.get('name')} from {plugin_path}", flush=True)

    d = Daemon(
        port=args.port,
        baud=args.baud,
        state_dir=args.state_dir,
        auto_fallback_port=args.auto_fallback_port,
        out_port=getattr(args, "out_port", None),
    )
    ctx = rb.RunbookContext(daemon=d, vars=dict(book.get("vars", {})))
    # Allow CLI overrides: --var key=val
    for kv in args.var or []:
        k, _, v = kv.partition("=")
        ctx.vars[k] = v

    rb.install_triggers(book, d, ctx)

    # Start daemon in a thread so we can run steps in main
    t = threading.Thread(target=d.start, daemon=True)
    t.start()
    # Wait until the serial port is actually open before issuing writes
    if not d.wait_connected(timeout=30.0):
        print("[cli] daemon failed to connect to serial port within 30s", flush=True)
        sys.exit(2)

    try:
        ok = rb.execute_steps(book, ctx)
        sys.exit(0 if ok else 2)
    except KeyboardInterrupt:
        print("\n[cli] interrupted", flush=True)
        sys.exit(130)


def cmd_up(args):
    """Launch daemon + tmux UI: top=serial-tail, bottom=keys.py.
    If --plugin is given, also runs the runbook in a background pane.

    Panes are targeted by their stable tmux pane-id (%N), captured at
    creation via `-P -F '#{pane_id}'`, rather than positional
    session:window.pane indices. Positional targets like `:0.1` break
    whenever the user's tmux sets `base-index`/`pane-base-index` to
    anything but 0; pane-ids are unaffected."""
    state_dir = args.state_dir or os.path.expanduser("~/.serial-runner")
    os.makedirs(state_dir, exist_ok=True)
    log_path = os.path.join(state_dir, "serial.log")
    fifo_path = os.path.join(state_dir, "input.fifo")

    session = args.session
    # Kill prior session if --force
    if args.force:
        subprocess.run(["tmux", "kill-session", "-t", session], capture_output=True)

    def tmux(*a, capture=False):
        """Run a tmux subcommand; return stripped stdout when capture=True."""
        r = subprocess.run(["tmux", *a], check=True,
                           capture_output=capture, text=True)
        return r.stdout.strip() if capture else None

    # Daemon command. Run as `sudo` only if requested.
    # If --plugin is set, the daemon installs its triggers itself — no
    # separate `run` pane needed (which would try to open the same serial port).
    py = sys.executable  # works under pipx, venv, or system python
    plugin_arg = f" --plugin {shlex.quote(args.plugin)}" if args.plugin else ""
    daemon_cmd = (
        ("sudo " if args.sudo else "")
        + f"{shlex.quote(py)} -m serial_runner.cli daemon --port {shlex.quote(args.port)} --baud {args.baud} --state-dir {shlex.quote(state_dir)}{plugin_arg}"
    )

    # tmux layout (by role, targeted via captured pane-ids):
    #   p_tail   (top-left):     tail -F serial.log
    #   p_daemon (top-right):    daemon
    #   p_info   (bottom-right): runbook info (only with --plugin)
    #   p_keys   (bottom-left):  keystroke relay
    # Use serial-runner's own garble-cleaning tail (non-printable bytes -> '.')
    # rather than raw `tail -F`, so noisy/garbage serial (wrong baud, non-UART
    # pad, boot noise) can't scramble the pane with stray control sequences.
    tail_cmd = (
        f"{shlex.quote(py)} -m serial_runner.cli tail "
        f"--state-dir {shlex.quote(state_dir)} --from end"
    )
    p_tail = tmux("new-session", "-d", "-s", session, "-P", "-F", "#{pane_id}",
                  tail_cmd, capture=True)
    p_daemon = tmux("split-window", "-h", "-t", p_tail, "-l", "60",
                    "-P", "-F", "#{pane_id}", daemon_cmd, capture=True)
    if args.plugin:
        # Triggers already installed in the daemon above; this pane just shows
        # plugin info / any future runbook steps if invoked manually.
        info_cmd = f'echo "plugin {shlex.quote(args.plugin)} loaded in daemon"; echo "run steps manually with: serial-runner run --plugin {shlex.quote(args.plugin)}"; exec bash'
        tmux("split-window", "-v", "-t", p_daemon, "-P", "-F", "#{pane_id}", info_cmd, capture=True)
    keys_loop = (
        f"while true; do {shlex.quote(py)} -m serial_runner.cli keys "
        f"--fifo {shlex.quote(fifo_path)}; echo '[keys.py exited — restarting]'; sleep 1; done"
    )
    p_keys = tmux("split-window", "-v", "-t", p_tail, "-l", "8",
                  "-P", "-F", "#{pane_id}", keys_loop, capture=True)
    tmux("set-option", "-t", session, "history-limit", "1000000")
    tmux("set-option", "-t", session, "mouse", "on")
    tmux("select-pane", "-t", p_keys)

    print(f"[cli] tmux session '{session}' up. attach: tmux attach -t {session}")
    print(f"[cli]   {p_tail} (top-left): serial tail")
    print(f"[cli]   {p_daemon} (top-right): daemon")
    if args.plugin:
        print(f"[cli]   (bottom-right): runbook progress")
    print(f"[cli]   {p_keys} (bottom-left): keystroke relay (focused)")


def _resolve_plugin(name_or_path: str) -> str:
    if os.path.isfile(name_or_path):
        return name_or_path
    # Built-in plugins dir
    pkg_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    p = os.path.join(pkg_dir, "plugins", f"{name_or_path}.yaml")
    if os.path.isfile(p):
        return p
    # User plugins dir
    user_p = os.path.expanduser(f"~/.serial-runner/plugins/{name_or_path}.yaml")
    if os.path.isfile(user_p):
        return user_p
    raise FileNotFoundError(f"plugin not found: {name_or_path}")


def main():
    ap = argparse.ArgumentParser(prog="serial-runner")
    sub = ap.add_subparsers(dest="cmd", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--port", default="/dev/ttyUSB0")
    common.add_argument("--baud", type=int, default=115200)
    common.add_argument("--state-dir", default=os.path.expanduser("~/.serial-runner"))
    common.add_argument(
        "--auto-fallback-port",
        action="store_true",
        help="if --port is missing, fall back to any /dev/ttyUSB*/ttyACM* (risky: may pick wrong device)",
    )
    common.add_argument(
        "--out-port",
        default=None,
        help="optional separate TX port. When set, FIFO writes and trigger send()s go here while --port is used only for reads. For asymmetric wiring (e.g. clean TTL header for RX, RS-232 for TX).",
    )

    p_daemon = sub.add_parser("daemon", parents=[common], help="run daemon only")
    p_daemon.add_argument("--plugin", help="runbook YAML to install triggers from (hot-reloadable via SIGHUP)")
    p_daemon.set_defaults(func=cmd_daemon)

    p_keys = sub.add_parser("keys", help="run keystroke relay (attach to running daemon)")
    p_keys.add_argument("--fifo", default=os.path.expanduser("~/.serial-runner/input.fifo"))
    p_keys.set_defaults(func=cmd_keys)

    p_send = sub.add_parser("send", help="one-shot paste: write TEXT to the daemon FIFO (→ serial TX)")
    p_send.add_argument("text", help="text to send (or hex bytes with --hex)")
    p_send.add_argument("--fifo", default=os.path.expanduser("~/.serial-runner/input.fifo"))
    p_send.add_argument("--end", default="\r",
                        help="line ending appended after TEXT (default: CR '\\r')")
    p_send.add_argument("--no-newline", action="store_true",
                        help="do not append any line ending")
    p_send.add_argument("--hex", action="store_true",
                        help="interpret TEXT as hex bytes, e.g. '03'=Ctrl-C, '0d'=CR")
    p_send.set_defaults(func=cmd_send)

    p_break = sub.add_parser("break", help="tell running daemon to drive a serial BREAK on its TX port (for sysrq, bootloader interrupt, etc.)")
    p_break.set_defaults(func=cmd_break)

    p_scan = sub.add_parser("scan", help="sweep baud rates on a FREE port, rank by printable-text ratio (pad/baud finder — stop the daemon first)")
    p_scan.add_argument("--port", default="/dev/ttyUSB0")
    p_scan.add_argument("--dwell", type=float, default=1.2, help="seconds to listen per rate")
    p_scan.add_argument("--rates", default=None, help="comma-separated baud rates to try (default: common set)")
    p_scan.set_defaults(func=cmd_scan)

    p_run = sub.add_parser("run", parents=[common], help="execute a runbook plugin")
    p_run.add_argument("--plugin", required=True)
    p_run.add_argument("--var", action="append", help="VAR=VALUE (repeatable)")
    p_run.set_defaults(func=cmd_run)

    p_watch = sub.add_parser("watch", parents=[common], help="emit NDJSON deltas of new serial bytes on an interval")
    p_watch.add_argument("--log", default=None, help="log file path (default: state_dir/serial.log)")
    p_watch.add_argument("--interval", type=float, default=5.0)
    p_watch.add_argument("--drop-kernel-timestamps", action="store_true",
                         help="filter out lines starting with [N.NNNNNN] kernel timestamps")
    p_watch.add_argument("--from-start", action="store_true",
                         help="start emitting from the beginning of the log (default: from current end)")
    p_watch.add_argument("--clean", action="store_true",
                         help="Strip NUL/BEL and map non-printable bytes to '.' (for noisy adapters like CH340).")
    p_watch.add_argument("--max-bytes-per-tick", type=int, default=None,
                         help="Split ticks larger than N bytes into multiple JSON lines (avoids overwhelming downstream consumers). Default: no split.")
    p_watch.set_defaults(func=cmd_watch)

    p_ai = sub.add_parser("ai", parents=[common], help="LLM channel — read NDJSON serial deltas, stream narration or trigger-based suggestions")
    p_ai.add_argument("--plugin", help="runbook YAML (cached as system context)")
    p_ai.add_argument("--mode", default="narrate", choices=["narrate", "trigger"],
                      help="narrate: react to every tick with content (live narration); trigger: only on ERROR/IDLE/ASK")
    p_ai.add_argument("--model", default="claude-sonnet-4-6")
    p_ai.add_argument("--effort", default="low", choices=["low", "medium", "high", "max"])
    p_ai.add_argument("--max-tokens", type=int, default=512)
    p_ai.add_argument("--min-interval", type=float, default=3.0, help="seconds between Claude calls (debounce)")
    p_ai.add_argument("--idle-threshold", type=int, default=6, help="consecutive empty ticks before IDLE fires")
    p_ai.add_argument("--trigger-regex", default=r"(?i)(error|fail|panic|denied|refused|wrong image format|kernel panic|cannot|could not|aborted)")
    p_ai.add_argument("--buffer-ticks", type=int, default=12, help="rolling buffer size in NDJSON ticks")
    p_ai.add_argument("--out", default=None, help="also append narration to this file")
    p_ai.set_defaults(func=cmd_ai)

    p_tail = sub.add_parser("tail", help="follow the serial log in real time with optional garble-cleaning (alternative to `tail -F | tr ...`)")
    p_tail.add_argument("--log", default=None, help="log file path (default: state_dir/serial.log)")
    p_tail.add_argument("--state-dir", default=os.path.expanduser("~/.serial-runner"))
    p_tail.add_argument("--from", dest="from_", choices=["end", "start"], default="end",
                        help="start from end (default, like tail -F) or start of file")
    p_tail.add_argument("--poll", type=float, default=0.2, help="polling interval seconds")
    p_tail.add_argument("--raw", action="store_true", help="disable garble byte-class mapping (output exact bytes)")
    p_tail.add_argument("--drop-kernel-timestamps", action="store_true",
                        help="drop lines starting with kernel timestamp [N.NNNNNN]")
    p_tail.add_argument("--flush-partial", type=float, default=1.0,
                        help="seconds of inactivity before flushing a partial (no-newline) line buffer when --drop-kernel-timestamps is set (default: 1.0)")
    p_tail.set_defaults(func=cmd_tail)

    p_up = sub.add_parser("up", parents=[common], help="launch tmux UI + daemon (+ optional plugin)")
    p_up.add_argument("--plugin")
    p_up.add_argument("--session", default="serial-runner")
    p_up.add_argument("--sudo", action="store_true", help="run daemon under sudo (needed for /dev/ttyUSB0 without dialout)")
    p_up.add_argument("--force", action="store_true", help="kill prior session first")
    p_up.set_defaults(func=cmd_up)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
