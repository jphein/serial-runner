"""LLM channel: reads NDJSON serial deltas from stdin, calls Claude, streams
text output to stdout (and optionally a file).

Two modes:
- ``narrate`` (default): on every tick with content, ask Claude to narrate what
  just happened and flag anything concerning. Output streams token-by-token to
  stdout — pipe to a tmux pane for live narration.
- ``trigger``: only consult on ERROR / IDLE / ASK triggers. Lower cost; good
  for unattended runs.

Both modes use prompt caching: the system prompt + runbook YAML are cached
with ephemeral cache_control. Only the rolling serial buffer is uncached per
call. Cache hit visibility appears in each emission (`read:N write:N`).
"""
import json, os, re, sys, time
from collections import deque
from typing import Optional


DEFAULT_TRIGGER_REGEX = r"(?i)(error|fail|panic|denied|refused|wrong image format|kernel panic|cannot|could not|aborted)"


def _read_runbook(path: Optional[str]) -> Optional[str]:
    if not path or not os.path.isfile(path):
        return None
    with open(path) as f:
        return f.read()


def _system_blocks(mode: str, runbook_yaml: Optional[str], extra_context: Optional[str]) -> list[dict]:
    """Frozen system prompt blocks with ephemeral cache_control on the last."""
    if mode == "narrate":
        role = (
            "You are narrating a live serial console during a hardware "
            "bring-up / recovery / flashing procedure. The user is watching "
            "your output in a tmux pane and uses it to keep tabs on the "
            "device without staring at raw serial. "
            "Each request gives you a runbook plugin (YAML) describing the "
            "expected procedure plus a snapshot of recent serial bytes. "
            "Narrate WHAT JUST HAPPENED in 1-3 short lines. Be concrete: "
            "name boot stages (U-Boot, kernel, init, login, sysupgrade), "
            "tools invoked, partitions written. If something looks off, "
            "say so on its own line prefixed with '⚠ '. If activity is "
            "uneventful (just kernel boot noise progressing normally), say "
            "one terse line like 'kernel boot in progress'. Never repeat "
            "yourself — assume the user saw your prior narration."
        )
    else:  # trigger
        role = (
            "You are an operations assistant watching a live serial console. "
            "You only see input when something interesting happens (an error "
            "marker, an idle stall, or an explicit ASK). For each request, "
            "diagnose and suggest the next concrete action — a command to "
            "send, a key to press, a step to retry. Be terse (2-5 lines). "
            "Quote serial evidence. If everything looks fine, say "
            "'NO ACTION NEEDED' and nothing else."
        )
    parts = [role]
    if runbook_yaml:
        parts.append(f"Active runbook plugin:\n\n```yaml\n{runbook_yaml}\n```")
    if extra_context:
        parts.append(extra_context)
    blocks = [{"type": "text", "text": p} for p in parts]
    blocks[-1]["cache_control"] = {"type": "ephemeral"}
    return blocks


def _content_buffer(ticks: deque[dict], max_chars: int = 8000) -> str:
    """Concatenate the rolling buffer of NDJSON ticks into a serial transcript."""
    text = ""
    for t in ticks:
        text += f"\n=== {t.get('t','??')} (+{t.get('bytes_added',0)}B) ===\n{t.get('content','')}"
    if len(text) > max_chars:
        text = "...[earlier history truncated]...\n" + text[-max_chars:]
    return text.strip()


def _stream_call(client, model: str, system: list[dict], serial_text: str,
                 user_prefix: str, effort: str, max_tokens: int,
                 on_token, on_done) -> dict:
    """Stream a Claude call, calling on_token(text) per delta, on_done(text, usage)."""
    full = []
    with client.messages.stream(
        model=model,
        max_tokens=max_tokens,
        output_config={"effort": effort},
        system=system,
        messages=[{
            "role": "user",
            "content": user_prefix + "\n\n" + serial_text,
        }],
    ) as stream:
        for delta in stream.text_stream:
            full.append(delta)
            on_token(delta)
        final = stream.get_final_message()
    usage = {
        "input": final.usage.input_tokens,
        "cache_read": final.usage.cache_read_input_tokens,
        "cache_write": final.usage.cache_creation_input_tokens,
        "output": final.usage.output_tokens,
    }
    on_done("".join(full), usage)
    return usage


def run(
    runbook_path: Optional[str],
    state_dir: str,
    mode: str = "narrate",
    model: str = "claude-sonnet-4-6",
    effort: str = "low",
    max_tokens: int = 512,
    min_interval_s: float = 3.0,
    idle_threshold: int = 6,
    trigger_regex: str = DEFAULT_TRIGGER_REGEX,
    buffer_ticks: int = 12,
    extra_context: Optional[str] = None,
    out_path: Optional[str] = None,
) -> None:
    """Main loop. NDJSON in on stdin, narration/suggestions out on stdout."""
    try:
        import anthropic
    except ImportError:
        print("[ai] anthropic SDK not installed: pip install 'serial-runner[ai]'", file=sys.stderr)
        sys.exit(1)

    client = anthropic.Anthropic()
    system = _system_blocks(mode, _read_runbook(runbook_path), extra_context)
    rx = re.compile(trigger_regex)

    ticks: deque[dict] = deque(maxlen=buffer_ticks)
    last_call_at = 0.0
    empty_streak = 0
    ask_flag = os.path.join(state_dir, "ai_ask")
    out_f = open(out_path, "a", buffering=1) if out_path else None

    def write(s: str) -> None:
        sys.stdout.write(s); sys.stdout.flush()
        if out_f: out_f.write(s)

    def header(reasons: list[str]) -> None:
        ts = time.strftime("%H:%M:%S")
        write(f"\n[ai {ts}] ({','.join(reasons)}) ")

    def footer(usage: dict) -> None:
        write(f"\n[ai usage] in:{usage['input']} read:{usage['cache_read']} write:{usage['cache_write']} out:{usage['output']}\n")

    write(f"[ai start] mode={mode} model={model} effort={effort} runbook={runbook_path or '(none)'}\n")

    user_prefix = (
        "Latest serial activity since your last narration:"
        if mode == "narrate"
        else "Recent serial console output. What (if anything) should the operator do next?"
    )

    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            tick = json.loads(raw)
        except json.JSONDecodeError:
            continue
        ticks.append(tick)

        reasons = []
        content = tick.get("content", "")
        if content.strip():
            empty_streak = 0
            if mode == "narrate":
                reasons.append("NEW")
            if rx.search(content):
                reasons.append("ERROR")
        else:
            empty_streak += 1
            if empty_streak >= idle_threshold:
                reasons.append("IDLE")
                empty_streak = 0
        if os.path.exists(ask_flag):
            reasons.append("ASK")
            try: os.unlink(ask_flag)
            except OSError: pass

        if not reasons:
            continue
        now = time.time()
        if now - last_call_at < min_interval_s and "ASK" not in reasons:
            continue
        last_call_at = now

        header(reasons)
        try:
            _stream_call(
                client, model, system,
                serial_text=_content_buffer(ticks),
                user_prefix=user_prefix,
                effort=effort, max_tokens=max_tokens,
                on_token=write,
                on_done=lambda text, usage: footer(usage),
            )
        except Exception as e:
            write(f"\n[ai err] {type(e).__name__}: {e}\n")
            continue

    if out_f:
        out_f.close()
