"""YAML runbook loader + executor.

A runbook is a list of (1) triggers (always-on reactions to serial patterns) and
(2) steps (sequential phases of a flash/recovery procedure).

Triggers run continuously in the daemon's reader thread.
Steps run sequentially in the runbook executor thread.
"""
import yaml, time, re, os, subprocess
from dataclasses import dataclass
from typing import Optional
from .daemon import Daemon, Trigger


@dataclass
class RunbookContext:
    """Mutable runtime context shared across steps. YAML can reference vars."""
    daemon: Daemon
    vars: dict


def _interp(s: str, ctx: RunbookContext) -> str:
    """Cheap ${var} interpolation against ctx.vars."""
    for k, v in ctx.vars.items():
        s = s.replace(f"${{{k}}}", str(v))
    return s


# --- Trigger action handlers (yaml `action:` types) ---

def _make_action(spec: dict, daemon: Daemon, ctx: RunbookContext):
    """Compile a YAML action spec into a no-arg callable."""
    if "send" in spec:
        s = spec["send"]
        return lambda: daemon.send(_interp(s, ctx).encode())
    if "type" in spec:
        text = spec["type"]
        delay = spec.get("delay", 0.10)
        wait_before = spec.get("wait", 0.0)
        end = spec.get("end", "\r")
        def _act():
            if wait_before: time.sleep(wait_before)
            daemon.type_chars(_interp(text, ctx), delay=delay, end=end)
        return _act
    if "noop" in spec:
        return lambda: None
    raise ValueError(f"unknown action spec: {spec}")


# --- Step handlers ---

def _wait_for_serial(pattern: str, timeout: float, daemon: Daemon) -> bool:
    rx = re.compile(pattern.encode() if isinstance(pattern, str) else pattern)
    end = time.time() + timeout
    while time.time() < end:
        with open(daemon.log_path, "rb") as f:
            f.seek(0, 2); sz = f.tell()
            f.seek(max(0, sz - 4000))
            buf = f.read().replace(b"\x07", b"")
        if rx.search(buf):
            return True
        time.sleep(0.5)
    return False


def _ssh(host: str, password: str, cmd: str, timeout: float = 120) -> subprocess.CompletedProcess:
    return subprocess.run([
        "sshpass", "-p", password,
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "PreferredAuthentications=password",
        "-o", "ConnectTimeout=4",
        f"root@{host}", cmd,
    ], capture_output=True, text=True, timeout=timeout)


def _scp(host: str, password: str, local: str, remote: str) -> subprocess.CompletedProcess:
    subprocess.run(["ssh-keygen", "-R", host], capture_output=True)
    return subprocess.run([
        "sshpass", "-p", password,
        "scp", "-O",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "PreferredAuthentications=password",
        local, f"root@{host}:{remote}",
    ], capture_output=True, text=True, timeout=180)


def _ping(host: str) -> bool:
    return subprocess.run(["ping", "-c", "1", "-W", "1", host],
                          capture_output=True, timeout=3).returncode == 0


def _run_step(step: dict, ctx: RunbookContext) -> bool:
    """Execute one step. Returns True on success."""
    d = ctx.daemon
    sid = step.get("id", "?")
    print(f"[runbook] STEP {sid}", flush=True)

    if "send" in step:
        d.send(_interp(step["send"], ctx).encode())

    if "type" in step:
        d.type_chars(_interp(step["type"], ctx), delay=step.get("delay", 0.10))

    if "send_lines" in step:
        # Send multiple lines, separated by \n in YAML
        for line in step["send_lines"].splitlines():
            line = line.strip()
            if not line:
                continue
            d.type_chars(_interp(line, ctx), delay=0.02)
            time.sleep(step.get("between_lines", 1.5))

    if "disable_trigger" in step:
        d.disable_trigger(step["disable_trigger"])

    if "enable_trigger" in step:
        d.enable_trigger(step["enable_trigger"])

    if "wait" in step:
        time.sleep(step["wait"])

    if "wait_for" in step:
        pat = _interp(step["wait_for"], ctx)
        if not _wait_for_serial(pat, step.get("timeout", 60), d):
            print(f"[runbook] STEP {sid}: timeout waiting for {pat!r}", flush=True)
            return False

    if "wait_ssh" in step:
        host = _interp(step["wait_ssh"].get("host", "192.168.1.1"), ctx)
        timeout = step["wait_ssh"].get("timeout", 240)
        end = time.time() + timeout
        while time.time() < end:
            if _ping(host):
                r = _ssh(host, ctx.vars.get("ssh_password", ""), "echo ok", timeout=8)
                if r.returncode == 0 and "ok" in r.stdout:
                    break
            time.sleep(3)
        else:
            print(f"[runbook] STEP {sid}: ssh never came up at {host}", flush=True)
            return False

    if "scp" in step:
        host = _interp(step["scp"].get("host", "192.168.1.1"), ctx)
        local = _interp(step["scp"]["local"], ctx)
        remote = _interp(step["scp"]["remote"], ctx)
        r = _scp(host, ctx.vars.get("ssh_password", ""), local, remote)
        if r.returncode != 0:
            print(f"[runbook] STEP {sid}: scp failed: {r.stderr}", flush=True)
            return False

    if "ssh" in step:
        host = _interp(step["ssh"].get("host", "192.168.1.1"), ctx)
        cmd = _interp(step["ssh"]["cmd"], ctx)
        r = _ssh(host, ctx.vars.get("ssh_password", ""), cmd, timeout=step["ssh"].get("timeout", 60))
        print(f"[runbook] STEP {sid}: ssh exit={r.returncode}", flush=True)

    if "poll_load_until" in step:
        target = step["poll_load_until"]
        host = _interp(step.get("host", "192.168.1.1"), ctx)
        end = time.time() + step.get("timeout", 600)
        while time.time() < end:
            r = _ssh(host, ctx.vars.get("ssh_password", ""), "cat /proc/loadavg | cut -d. -f1", timeout=8)
            load = r.stdout.strip()
            print(f"[runbook] STEP {sid}: load={load}", flush=True)
            if load == str(target):
                break
            time.sleep(15)

    if "require_user" in step:
        prompt = _interp(step["require_user"], ctx)
        wait_for_pattern = step.get("until")
        print(f"[runbook] STEP {sid}: USER ACTION REQUIRED — {prompt}", flush=True)
        if wait_for_pattern:
            if not _wait_for_serial(_interp(wait_for_pattern, ctx),
                                    step.get("timeout", 300), d):
                print(f"[runbook] STEP {sid}: user action timeout", flush=True)
                return False

    if "assert_ssh" in step:
        host = _interp(step.get("host", "192.168.1.1"), ctx)
        cmd = _interp(step["assert_ssh"], ctx)
        r = _ssh(host, ctx.vars.get("ssh_password", ""), cmd, timeout=8)
        if r.returncode != 0:
            print(f"[runbook] STEP {sid}: assertion failed: {cmd}", flush=True)
            return False

    print(f"[runbook] STEP {sid} OK", flush=True)
    return True


def load(yaml_path: str) -> dict:
    with open(yaml_path) as f:
        return yaml.safe_load(f)


def install_triggers(rb: dict, daemon: Daemon, ctx: RunbookContext) -> None:
    """Register all `triggers:` entries with the daemon."""
    for t in rb.get("triggers", []):
        action = _make_action(t["action"], daemon, ctx)
        trig = Trigger(
            name=t["id"],
            pattern=t["pattern"].encode() if isinstance(t["pattern"], str) else t["pattern"],
            action=action,
            debounce_s=t.get("debounce", 30.0),
        )
        daemon.add_trigger(trig)


def execute_steps(rb: dict, ctx: RunbookContext) -> bool:
    """Run all `steps:` sequentially. Returns True if all passed."""
    for step in rb.get("steps", []):
        ok = _run_step(step, ctx)
        if not ok:
            print(f"[runbook] aborted at step {step.get('id')}", flush=True)
            return False
    print(f"[runbook] all steps completed", flush=True)
    return True
