"""serial-runner CLI."""
import argparse, os, sys, subprocess, threading, time
from . import runbook as rb
from .daemon import Daemon


def cmd_daemon(args):
    """Run the daemon (no runbook) — useful for raw serial access."""
    d = Daemon(port=args.port, baud=args.baud, state_dir=args.state_dir)
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
    )


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


def cmd_run(args):
    """Load a runbook plugin and execute it, with the daemon serving alongside."""
    plugin_path = _resolve_plugin(args.plugin)
    book = rb.load(plugin_path)
    print(f"[cli] loaded plugin: {book.get('name')} from {plugin_path}", flush=True)

    d = Daemon(port=args.port, baud=args.baud, state_dir=args.state_dir)
    ctx = rb.RunbookContext(daemon=d, vars=dict(book.get("vars", {})))
    # Allow CLI overrides: --var key=val
    for kv in args.var or []:
        k, _, v = kv.partition("=")
        ctx.vars[k] = v

    rb.install_triggers(book, d, ctx)

    # Start daemon in a thread so we can run steps in main
    t = threading.Thread(target=d.start, daemon=True)
    t.start()
    time.sleep(1)  # let daemon initialize

    try:
        ok = rb.execute_steps(book, ctx)
        sys.exit(0 if ok else 2)
    except KeyboardInterrupt:
        print("\n[cli] interrupted", flush=True)
        sys.exit(130)


def cmd_up(args):
    """Launch daemon + tmux UI: top=serial-tail, bottom=keys.py.
    If --plugin is given, also runs the runbook in a background pane."""
    state_dir = args.state_dir or os.path.expanduser("~/.serial-runner")
    os.makedirs(state_dir, exist_ok=True)
    log_path = os.path.join(state_dir, "serial.log")
    fifo_path = os.path.join(state_dir, "input.fifo")

    session = args.session
    # Kill prior session if --force
    if args.force:
        subprocess.run(["tmux", "kill-session", "-t", session], capture_output=True)

    # Daemon command. Run as `sudo` only if requested.
    daemon_cmd = (
        ("sudo " if args.sudo else "")
        + f"python3 -m serial_runner.cli daemon --port {args.port} --baud {args.baud} --state-dir {state_dir}"
    )

    # tmux layout:
    #   pane 0 (top): tail -F serial.log
    #   pane 1 (right side, top): daemon
    #   pane 2 (right side, bottom): runbook output (if --plugin) or shell
    #   pane 3 (bottom): keys relay
    subprocess.run([
        "tmux", "new-session", "-d", "-s", session,
        f"tail -F {log_path} | tr -d '\\007'",
    ], check=True)
    subprocess.run([
        "tmux", "split-window", "-h", "-t", f"{session}:0", "-l", "60", daemon_cmd,
    ], check=True)
    if args.plugin:
        run_cmd = f"sleep 2; python3 -m serial_runner.cli run --plugin {args.plugin} --port {args.port} --baud {args.baud} --state-dir {state_dir}"
        subprocess.run([
            "tmux", "split-window", "-v", "-t", f"{session}:0.1", run_cmd,
        ], check=True)
    subprocess.run([
        "tmux", "split-window", "-v", "-t", f"{session}:0.0", "-l", "8",
        f"while true; do python3 -m serial_runner.cli keys --fifo {fifo_path}; echo '[keys.py exited — restarting]'; sleep 1; done",
    ], check=True)
    subprocess.run(["tmux", "set-option", "-t", session, "history-limit", "1000000"], check=True)
    subprocess.run(["tmux", "set-option", "-t", session, "mouse", "on"], check=True)
    subprocess.run(["tmux", "select-pane", "-t", f"{session}:0.3"], check=False)

    print(f"[cli] tmux session '{session}' up. attach: tmux attach -t {session}")
    print(f"[cli]   pane 0 (top-left): serial tail")
    print(f"[cli]   pane 1 (top-right): daemon")
    if args.plugin:
        print(f"[cli]   pane 2 (bottom-right): runbook progress")
    print(f"[cli]   pane 3 (bottom-left): keystroke relay (focused)")


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

    p_daemon = sub.add_parser("daemon", parents=[common], help="run daemon only")
    p_daemon.set_defaults(func=cmd_daemon)

    p_keys = sub.add_parser("keys", help="run keystroke relay (attach to running daemon)")
    p_keys.add_argument("--fifo", default=os.path.expanduser("~/.serial-runner/input.fifo"))
    p_keys.set_defaults(func=cmd_keys)

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
