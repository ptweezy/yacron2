"""Best-effort secret scrubbing for archived job output.

Captured stdout/stderr routinely carries credentials -- a connection string, a
bearer token, an API key echoed by a misbehaving script -- so before cronstable
writes a run's output to a durable store (see
:meth:`cronstable.cron.Cron._archive_output`) it runs each line through
:func:`redact_secrets` (or, for a whole run's output, :func:`redact_lines`,
which additionally tracks multi-line PEM blocks across lines).

This is a *defence in depth* pass, deliberately conservative: it errs toward
redacting a bit too much rather than leaking, and it is not a guarantee that no
secret survives.  It replaces only the sensitive span (keeping the surrounding
key/label for context), so an archived log stays readable.  Redaction is on by
default and can be turned off per job with ``redactArchivedSecrets: false``.
"""

import re
from typing import Callable, Iterable, List, Optional, Tuple, Union

#: What a redacted span is replaced with.
REDACTED = "***REDACTED***"

_Repl = Union[str, Callable[[re.Match], str]]


# (compiled pattern, replacement) applied in order.  Replacements that need to
# keep surrounding context (a key name, a URL host) use a callable; the rest
# replace the whole match, which is itself the secret.
_PATTERNS: List[Tuple[re.Pattern, _Repl]] = [
    # key = value / key: value where the key names a secret.  Keeps the key
    # and separator, redacts the value.  Deliberately loose around the key:
    #
    # * the key may be a SUFFIX of a compound name -- `(?<![a-z0-9])` (not
    #   ``\b``) so an underscore prefix still matches: ``MY_PASSWORD=`` and
    #   ``AWS_SECRET_ACCESS_KEY=`` are the single most common shapes real job
    #   output leaks, and ``\b`` never fires between ``_`` and a letter;
    # * the key may be quoted (JSON bodies): an optional closing quote is
    #   allowed between the key and the ``=``/``:`` separator;
    # * the value may be quoted: a quoted value is redacted to its closing
    #   quote (preserving any trailing structure, e.g. the rest of a JSON
    #   object); a BARE value is redacted to end of line, because a secret
    #   containing spaces ("correct horse battery staple") has no reliable
    #   delimiter and a first-word-only redaction leaks the tail while the
    #   archive is stamped redacted -- over-redaction is this module's
    #   documented bias.
    # The quoted alternatives admit backslash escapes: a JSON-encoded value
    # inside a JSON log line ("password": "{\"inner\":\"s3cret\"}") would
    # otherwise terminate the match at the first embedded \" and leak the
    # tail of the secret while the archive is stamped redacted.
    # Bare values: a value ending at a JSON/`key=value`-list delimiter
    # (comma, closing brace/bracket) is redacted only up to it, preserving
    # the surrounding structure; anything else redacts to end of line (a
    # multi-word passphrase has no reliable delimiter).
    (
        re.compile(
            r"(?i)(?<![a-z0-9])("
            r"password|passwd|pwd|secret|token|api[_-]?key|apikey|"
            r"access[_-]?key|secret[_-]?key|auth[_-]?token|credential|"
            r"private[_-]?key"
            r")(s?)([\"']?\s*[=:]\s*)"
            r"(\"(?:[^\"\\]|\\.)*\"|'(?:[^'\\]|\\.)*'"
            r"|[^\s,}\]]+(?=\s*[,}\]])|[^\r\n]+)"
        ),
        lambda m: f"{m.group(1)}{m.group(2)}{m.group(3)}{REDACTED}",
    ),
    # credentials embedded in a URL: scheme://user:PASSWORD@host (redact pass).
    # The username is OPTIONAL (``*`` not ``+``): the credential-only form
    # ``scheme://:PASSWORD@host`` -- how redis/mongodb/amqp connection strings
    # carry a password with no user -- has an empty username, and requiring a
    # username here leaked those passwords verbatim.  The ``@`` anchor keeps a
    # plain ``host:port`` URL (no userinfo) from matching.
    (
        re.compile(r"([a-zA-Z][a-zA-Z0-9+.\-]*://[^:/\s@]*:)([^@/\s]+)(@)"),
        lambda m: f"{m.group(1)}{REDACTED}{m.group(3)}",
    ),
    # Authorization: Bearer <token> / Basic <base64 user:pass>.  The Basic
    # form is anchored to the header name: "basic" is an ordinary English
    # word, and an unanchored pattern redacted innocent text like
    # "basic understanding" wholesale.
    (
        re.compile(r"(?i)(bearer\s+)([A-Za-z0-9._\-]{8,})"),
        lambda m: m.group(1) + REDACTED,
    ),
    (
        re.compile(r"(?i)(authorization\s*:?\s+basic\s+)([A-Za-z0-9+/=]{8,})"),
        lambda m: m.group(1) + REDACTED,
    ),
    # Recognisable cloud/service token formats (the whole match is the secret).
    (re.compile(r"AKIA[0-9A-Z]{16}"), REDACTED),
    (re.compile(r"\bxox[baprs]-[0-9A-Za-z-]{8,}"), REDACTED),
    (re.compile(r"\bgh[posur]_[0-9A-Za-z]{20,}"), REDACTED),
    # GitHub fine-grained personal access tokens.
    (re.compile(r"\bgithub_pat_[0-9A-Za-z_]{20,}"), REDACTED),
    # OpenAI/Anthropic-style bare keys and Stripe live/test keys.
    (re.compile(r"\bsk-[A-Za-z0-9_\-]{16,}"), REDACTED),
    (re.compile(r"\b[sr]k_(?:live|test)_[0-9A-Za-z]{16,}"), REDACTED),
    # JWTs (three base64url segments joined by dots).
    (
        re.compile(
            r"\beyJ[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}"
        ),
        REDACTED,
    ),
    # A PEM private-key header line and anything after it on that line.  The
    # BODY of a multi-line PEM block is handled statefully by redact_lines --
    # per-line patterns cannot see that the base64 lines following the header
    # ARE the key material.
    (re.compile(r"(?i)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*"), REDACTED),
]

# Cheap prefilter, parallel to _PATTERNS (same order). Each entry is a literal
# that MUST be present in a line for the corresponding pattern to have any
# chance of matching, or None for the always-run patterns (the case-insensitive
# ones, and the key=value pattern, which have no single required literal).
# redact_secrets skips any pattern whose literal is absent: since that sub()
# could only be a no-op, the elision keeps the output byte-identical while
# dropping a typical no-secret line from 12 regex passes to the 3 always-on
# ones. "***REDACTED***" contains none of these literals, so an earlier
# redaction can never spuriously trip a later gate. Kept in lockstep with
# _PATTERNS by the check below -- a plain `if`, deliberately not an `assert`,
# because the release binary runs under -OO, which strips asserts.
_PATTERN_GATES: Tuple[Optional[str], ...] = (
    None,  # 1. key = value / key: value (case-insensitive keywords)
    "://",  # 2. scheme://user:PASSWORD@host
    None,  # 3. Bearer <token> (case-insensitive)
    None,  # 4. Authorization: Basic <base64> (case-insensitive)
    "AKIA",  # 5. AWS access key id
    "xox",  # 6. Slack tokens
    "gh",  # 7. GitHub ghp_/gho_/ghs_/ghu_/ghr_ tokens
    "github_pat_",  # 8. GitHub fine-grained PAT
    "sk-",  # 9. OpenAI/Anthropic-style keys
    "k_",  # 10. Stripe [sr]k_live_/_test_ keys
    "eyJ",  # 11. JWT (base64url of the opening '{"')
    "-----",  # 12. PEM -----BEGIN ... PRIVATE KEY----- header
)
if len(_PATTERN_GATES) != len(_PATTERNS):  # pragma: no cover - dev invariant
    raise RuntimeError("redact: _PATTERN_GATES is out of step with _PATTERNS")

_PEM_BEGIN = re.compile(r"(?i)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")
_PEM_END = re.compile(r"(?i)-----END [A-Z0-9 ]*PRIVATE KEY-----")


def redact_secrets(text: str) -> str:
    """Return ``text`` with recognisable secrets replaced by :data:`REDACTED`.

    Conservative and best-effort (see the module docstring): applies each known
    pattern in turn.  Safe on any input and never raises.  Stateless: for a
    *sequence* of lines that may contain a multi-line PEM block, use
    :func:`redact_lines`, which redacts the block's body, not just its header.
    """
    for (pattern, repl), required in zip(
        _PATTERNS, _PATTERN_GATES, strict=True
    ):
        # Skip a pattern whose mandatory literal is absent from the line: its
        # sub() would be a guaranteed no-op, so eliding it leaves the output
        # byte-identical while sparing most lines all but the always-on passes.
        if required is not None and required not in text:
            continue
        text = pattern.sub(repl, text)
    return text


def _pem_state_after(line: str, in_pem: bool) -> bool:
    """Whether a PEM block is still open after ``line``.

    Walks the BEGIN/END markers in POSITION order, so a line carrying both
    (two PEM files concatenated without a trailing newline, a log line
    quoting ``END`` before ``BEGIN``) transitions correctly.  Judging by mere
    marker *presence* mis-ordered exactly those lines and leaked the second
    key's whole base64 body.
    """
    pos = 0
    while True:
        marker = _PEM_END if in_pem else _PEM_BEGIN
        match = marker.search(line, pos)
        if match is None:
            return in_pem
        in_pem = not in_pem
        pos = match.end()


def _starts_mid_pem(lines: List[str]) -> bool:
    """Whether ``lines`` begins INSIDE a PEM block whose BEGIN was truncated.

    ``True`` iff the first PEM marker in the batch (in position order) is an
    ``END`` with no ``BEGIN`` before it -- the fingerprint of a private key
    whose header line was evicted from the bounded live-log ring before
    archiving, leaving only its base64 body and the ``END``.  Seeding
    :func:`redact_lines` with this scrubs the leading body instead of leaking
    it.  In untruncated output a ``BEGIN`` always precedes its ``END``, so this
    is ``False`` and the output is byte-identical to the unseeded walk.
    """
    for line in lines:
        if "-----" not in line:  # cheap gate, parallel to redact_secrets
            continue
        begin = _PEM_BEGIN.search(line)
        end = _PEM_END.search(line)
        if begin is None and end is None:
            continue
        return end is not None and (
            begin is None or end.start() < begin.start()
        )
    return False


def redact_lines(lines: Iterable[str]) -> List[str]:
    """Redact an ordered sequence of output lines, tracking PEM blocks.

    Applies :func:`redact_secrets` to each line, and additionally replaces
    every line inside a ``-----BEGIN ... PRIVATE KEY----- / -----END ...-----``
    block (inclusive) with :data:`REDACTED`: the base64 body lines *are* the
    key material, and no per-line pattern can recognise them in isolation.  A
    block left unterminated (truncated output) stays redacted to the end --
    erring toward over-redaction, per the module contract.

    The batch may also start MID-block: when a private key is printed early and
    the run then emits enough further output that the bounded live-log ring
    evicts the ``BEGIN`` header before archiving, the archived tail opens with
    the key's base64 body.  :func:`_starts_mid_pem` detects that (a leading
    ``END`` with no preceding ``BEGIN``) and seeds the walk ``in_pem`` so the
    orphaned body is redacted rather than passed through -- the symmetric case
    to a truncated trailing ``END``.
    """
    materialised = list(lines)
    out: List[str] = []
    in_pem = _starts_mid_pem(materialised)
    for line in materialised:
        if in_pem:
            out.append(REDACTED)
        else:
            # a line that OPENS a block still gets the per-line pass (the
            # header pattern redacts from the marker to end of line).
            out.append(redact_secrets(line))
        in_pem = _pem_state_after(line, in_pem)
    return out
