"""Per-keystroke relay: stdin → daemon FIFO. Resilient to EOF/Ctrl-D."""
import os, sys, time


def main(fifo_path: str = None) -> int:
    fifo_path = fifo_path or os.path.expanduser("~/.serial-runner/input.fifo")
    # Wait briefly for FIFO to exist (allows starting before daemon)
    for _ in range(30):
        if os.path.exists(fifo_path):
            break
        time.sleep(0.5)
    else:
        print(f"FIFO {fifo_path} missing — start daemon first", flush=True)
        return 1

    print("=" * 60, flush=True)
    print(">>> serial-runner keys ACTIVE.  Type → serial.  Ctrl-] = quit. <<<", flush=True)
    print("=" * 60, flush=True)

    fifo = open(fifo_path, "wb", buffering=0)
    fd_in = sys.stdin.fileno()

    old = None
    try:
        import tty, termios
        old = termios.tcgetattr(fd_in)
        tty.setraw(fd_in)
    except Exception as e:
        print(f"[keys] raw mode unavailable ({e}); line mode", flush=True)

    try:
        while True:
            try:
                ch = os.read(fd_in, 1)
            except OSError:
                continue
            if not ch:
                time.sleep(0.1); continue
            if ch == b"\x1d":  # Ctrl-]
                break
            if ch == b"\x04":  # ignore Ctrl-D — accidental exit guard
                continue
            try:
                fifo.write(ch)
            except Exception as e:
                print(f"[keys] write err {e} — reopening", flush=True)
                try: fifo.close()
                except: pass
                time.sleep(0.5)
                fifo = open(fifo_path, "wb", buffering=0)
    finally:
        if old is not None:
            try:
                import termios
                termios.tcsetattr(fd_in, termios.TCSADRAIN, old)
            except: pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
