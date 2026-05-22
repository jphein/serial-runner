# serial-runner

AI-assisted serial terminal with hardware-specific runbook plugins. tmux is the UI.

## Quick start

```bash
pip install -e .
sudo usermod -aG dialout $USER       # for /dev/ttyUSB* without sudo (logout/login)
serial-runner up --plugin ws-ap3825i # launches daemon + tmux UI + plugin
tmux attach -t serial-runner
```

Inside tmux:
- **top-left**: live serial output (tail -F)
- **top-right**: daemon log
- **bottom-right**: runbook progress (if `--plugin`)
- **bottom-left**: keystroke relay — type and your keys go to the AP

Toggle a trigger off without restart:
```bash
touch ~/.serial-runner/disable_autoboot   # AP can autoboot through to stock OS
rm    ~/.serial-runner/disable_autoboot   # re-arm
```

## Subcommands

| cmd | purpose |
|---|---|
| `serial-runner up [--plugin X]` | launch daemon + tmux UI |
| `serial-runner daemon` | run daemon only (no UI) |
| `serial-runner keys` | run keystroke relay attached to a running daemon |
| `serial-runner run --plugin X` | execute a plugin against a running serial port (no UI) |
| `serial-runner watch [--interval 5] [--drop-kernel-timestamps]` | NDJSON delta feed of new serial bytes per tick. Designed as the LLM context channel. |
| `serial-runner ai --plugin X [--model claude-sonnet-4-6] [--out file.log]` | Read `watch`'s NDJSON on stdin; consult Claude on triggers (ERROR / IDLE / ASK), suggest the next action. Cache the runbook+system prompt so only the new serial bytes are uncached per call. |

### LLM channel (v0.2)

```bash
pip install -e ".[ai]"                            # adds the anthropic SDK
export ANTHROPIC_API_KEY=sk-ant-...

# Pipe the delta stream into the AI channel
# narrate mode (default): live narration of every tick, streamed token-by-token
serial-runner watch | serial-runner ai --plugin ws-ap3825i --out ~/serial-ai.log

# trigger mode: quieter, only consults on ERROR/IDLE/ASK
serial-runner watch | serial-runner ai --plugin ws-ap3825i --mode trigger --min-interval 30
```

**Modes:**
- **narrate** (default): every tick with content fires a streaming Claude call. Output streams to stdout token-by-token — pipe to a tmux pane for live narration. Use `--min-interval` (default 3s) to debounce.
- **trigger**: only consults on ERROR/IDLE/ASK. Use `--min-interval 30` or higher for unattended runs.

**Triggers** (any fires a call, all subject to `--min-interval` debounce — ASK bypasses debounce):
- **NEW** *(narrate only)*: tick has content
- **ERROR**: new bytes match `--trigger-regex` (default catches `error|fail|panic|kernel panic|wrong image format|cannot|aborted` etc.)
- **IDLE**: `--idle-threshold` consecutive empty ticks (default 6 × 5s = 30s of silence)
- **ASK**: `touch ~/.serial-runner/ai_ask` for an explicit consultation (one-shot, file is consumed)

The runbook YAML is cached in the system prompt via `cache_control: {"type": "ephemeral"}` — repeated calls only pay full input price for the rolling serial buffer. Verify with the `read:N write:N` numbers in each AI emission (`[ai usage] in:N read:N write:N out:N`).

## Plugin contract

YAML at `plugins/<name>.yaml`. Two sections:

- `triggers:` — always-on byte-pattern reactions (autoboot, login, console-activate).
  Daemon's byte-level reader fires these without log-file lag.
- `steps:` — sequential phases of the procedure. Built-in step types:
  `send`, `type`, `send_lines`, `wait`, `wait_for`, `wait_ssh`, `scp`, `ssh`,
  `poll_load_until`, `require_user`, `disable_trigger`, `enable_trigger`,
  `assert_ssh`.

See `plugins/ws-ap3825i.yaml` for the reference.

## Status

v0.1 — works for the WS-AP3825i recipe. No LLM integration yet (v0.3). No
plugin registry (v0.2). Single serial port (v0.4 = multi-port).

## Design notes

See `serial-ai-terminal-design.md` in the parent `openwrt` project.

## Why byte-level triggers

U-Boot autoboot prints `Hit 'd' for diagnostics: 2 \b 1 \b 0` — entire
countdown on one line via backspace overwrites. A `tail -F` matcher only
sees the line AFTER autoboot has completed and emitted `\n`. By that time
your keystroke is too late. Reading bytes inline catches it.

## Why mtd -r write not sysupgrade

OpenWrt 23.05.5 on some WS-AP3825i units crashes procd's `ujail` during
sysupgrade stage2, silently aborting the flash. `mtd -r write` writes the
FIT image directly and reboots, avoiding the broken stage2 entirely. The
WS-AP3825i sysupgrade.bin is a raw FIT image, so it can be written verbatim.
