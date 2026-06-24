"""Cross-platform command helpers for the test suite.

The original tests drove real subprocesses with POSIX shell snippets
(``echo``/``sleep``/``exit``, SIGTERM traps).  Windows has no ``/bin/sh``, so
these helpers express the same behavior as **list-form** commands that run
the test interpreter (``sys.executable -c ...``); a list command bypasses the
shell entirely and behaves identically on every platform.

Captured output is written through ``sys.stdout.buffer`` /
``sys.stderr.buffer`` so the bytes carry ``\\n`` line endings everywhere
(``print()`` would translate to ``\\r\\n`` on Windows and break exact-output
assertions).

``yaml_command`` renders an argv list as a YAML ``command:`` block.  Each item
is JSON-encoded, which is always a valid double-quoted YAML scalar -- so a
Windows interpreter path (backslashes) and arbitrary Python source embed
cleanly.
"""

import json
import sys

PYTHON = sys.executable


def yaml_command(argv, indent=4, key="command"):
    """Render ``argv`` as a YAML ``<key>:`` block sequence at ``indent``."""
    pad = " " * indent
    lines = ["{}{}:".format(pad, key)]
    lines += ["{}  - {}".format(pad, json.dumps(arg)) for arg in argv]
    return "\n".join(lines)


def _py(code):
    return [PYTHON, "-c", code]


def cmd_print(out=None, err=None, code=0):
    """argv that writes a line to stdout/stderr then exits ``code``.

    The Python analog of ``echo "<out>" [1>&2] ; exit <code>``.  ``out`` /
    ``err`` are line contents *without* the trailing newline (it is appended,
    like echo).
    """
    parts = ["import sys"]
    if out is not None:
        parts.append("sys.stdout.buffer.write({}.encode())".format(
            json.dumps(out + "\n")
        ))
        parts.append("sys.stdout.buffer.flush()")
    if err is not None:
        parts.append("sys.stderr.buffer.write({}.encode())".format(
            json.dumps(err + "\n")
        ))
        parts.append("sys.stderr.buffer.flush()")
    parts.append("sys.exit({})".format(code))
    return _py("; ".join(parts))


def cmd_sleep(seconds):
    """argv that sleeps for ``seconds`` (the Python analog of ``sleep``)."""
    return _py("import time; time.sleep({})".format(seconds))


def cmd_print_sleep_print(first, seconds, second):
    """Write ``first``, sleep, then write ``second`` (for executionTimeout)."""
    code = (
        "import sys, time; "
        "sys.stdout.buffer.write({}.encode()); sys.stdout.buffer.flush(); "
        "time.sleep({}); "
        "sys.stdout.buffer.write({}.encode()); sys.stdout.buffer.flush()"
    ).format(json.dumps(first + "\n"), seconds, json.dumps(second + "\n"))
    return _py(code)


def cmd_hang(first, seconds):
    """Write ``first``, ignore SIGTERM, then sleep ``seconds``.

    On POSIX the SIG_IGN makes the graceful ``terminate()`` (SIGTERM) a no-op
    so the killTimeout -> SIGKILL path is exercised.  On Windows SIGTERM is
    never delivered (TerminateProcess kills unconditionally), so ignoring it is
    a harmless no-op and the timeout path still fires.
    """
    code = (
        "import sys, time, signal; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "sys.stdout.buffer.write({}.encode()); sys.stdout.buffer.flush(); "
        "time.sleep({})"
    ).format(json.dumps(first + "\n"), seconds)
    return _py(code)


def cmd_write_env(path):
    """argv that appends the YACRON2_* report env vars to ``path``.

    Replaces the shell reporter's ``echo "$YACRON2_... " >> file``; verifies
    the same thing (the reporter exports those variables to its command).
    """
    code = (
        "import os; "
        "open({}, 'a', encoding='utf-8').write("
        "'{{}} - {{}} - {{}} - Error code {{}}'.format("
        "os.environ['YACRON2_JOB_NAME'], "
        "os.environ['YACRON2_JOB_COMMAND'], "
        "os.environ['YACRON2_JOB_SCHEDULE'], "
        "os.environ['YACRON2_RETCODE']))"
    ).format(json.dumps(path))
    return _py(code)
