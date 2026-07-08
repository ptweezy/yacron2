"""Secret scrubbing for archived job output (cronstable.redact)."""

from cronstable.redact import REDACTED, redact_lines, redact_secrets


def test_key_value_secrets_redacted():
    assert redact_secrets("password=hunter2") == "password=" + REDACTED
    assert redact_secrets("API_KEY: abc123xyz") == "API_KEY: " + REDACTED
    # a quoted value is redacted whole, quotes included.
    assert redact_secrets('secret="s3cr3t"') == "secret=" + REDACTED


def test_underscore_prefixed_key_names_redacted():
    # `\b` never fires between `_` and a letter, so these -- the single most
    # common shapes in real job output -- used to pass through untouched.
    assert redact_secrets("MY_PASSWORD=hunter2") == "MY_PASSWORD=" + REDACTED
    out = redact_secrets("AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG")
    assert "wJalrXUtnFEMI" not in out
    assert REDACTED in out
    out = redact_secrets("DB_PASSWORD: s3cret")
    assert "s3cret" not in out


def test_json_body_secrets_redacted():
    # the key's closing quote sits between the key and the separator.
    out = redact_secrets('{"api_key": "abc123", "user": "bob"}')
    assert "abc123" not in out
    assert '"user": "bob"' in out  # quoted value: trailing structure kept


def test_multiword_secret_values_do_not_leak_their_tail():
    out = redact_secrets("Password: correct horse battery staple")
    assert "battery" not in out
    assert out == "Password: " + REDACTED
    # quoted multi-word values are bounded by the closing quote.
    out = redact_secrets("passphrase secret='two words' trailing=kept")
    assert "two words" not in out


def test_more_token_formats_redacted():
    assert REDACTED in redact_secrets("github_pat_" + "a1" * 15)
    assert REDACTED in redact_secrets("sk-" + "a" * 24)
    assert REDACTED in redact_secrets("sk_live_" + "a1B2" * 6)
    out = redact_secrets("Authorization: Basic dXNlcjpwYXNzd29yZA==")
    assert "dXNlcjpwYXNzd29yZA" not in out


def test_escaped_quotes_in_quoted_values_do_not_leak_tail():
    # a JSON-encoded value inside a JSON log line: the quoted-value match
    # must honor backslash escapes or everything after the first embedded
    # \" leaks while the archive is stamped redacted.
    out = redact_secrets(
        '{"password": "{\\"inner\\":\\"supersecretvalue\\"}"}'
    )
    assert "supersecretvalue" not in out
    out = redact_secrets('password="ab\\"cd"')
    assert "cd" not in out


def test_pem_markers_sharing_one_line_do_not_leak_second_block():
    # cat key1.pem key2.pem without a trailing newline puts END and BEGIN on
    # one line; presence-based state tracking exited PEM mode there and
    # archived the second key's whole base64 body in cleartext.
    out = redact_lines(
        [
            "-----BEGIN RSA PRIVATE KEY-----",
            "block1AAAA",
            "-----END RSA PRIVATE KEY----------BEGIN RSA PRIVATE KEY-----",
            "block2BBBB",
            "-----END RSA PRIVATE KEY-----",
            "after",
        ]
    )
    assert "block2BBBB" not in out
    assert out[-1] == "after"
    # an END quoted BEFORE a BEGIN on a non-PEM line must still open the
    # block for the following body lines.
    out = redact_lines(
        [
            "log: -----END RSA PRIVATE KEY----- then "
            "-----BEGIN RSA PRIVATE KEY-----",
            "bodyAAAA",
            "-----END RSA PRIVATE KEY-----",
            "tail",
        ]
    )
    assert "bodyAAAA" not in out
    assert out[-1] == "tail"
    # a self-contained single-line block exits cleanly.
    out = redact_lines(
        [
            "x -----BEGIN EC PRIVATE KEY----- k -----END EC PRIVATE KEY-----",
            "normal line",
        ]
    )
    assert out[1] == "normal line"


def test_pem_block_body_redacted_across_lines():
    lines = [
        "before",
        "-----BEGIN RSA PRIVATE KEY-----",
        "MIIEpAIBAAKCAQEA7demo",
        "sTc3Jk8demo=",
        "-----END RSA PRIVATE KEY-----",
        "after",
    ]
    out = redact_lines(lines)
    assert out[0] == "before"
    assert out[-1] == "after"
    # header, body and trailer are all gone -- the base64 body IS the key.
    assert all(REDACTED == line for line in out[1:-1])
    # an unterminated block redacts to the end (truncated output).
    out = redact_lines(["-----BEGIN PRIVATE KEY-----", "MIIabc", "MIIdef"])
    assert out[1:] == [REDACTED, REDACTED]


def test_non_secret_keys_untouched():
    assert redact_secrets("count=5") == "count=5"
    assert redact_secrets("status: running") == "status: running"


def test_url_credentials_redacted():
    out = redact_secrets("postgres://user:s3cret@db:5432/app")
    assert "s3cret" not in out
    assert "user:" + REDACTED + "@db" in out


def test_bearer_token_redacted():
    out = redact_secrets("Authorization: Bearer abcdef123456")
    assert "abcdef123456" not in out
    assert REDACTED in out


def test_cloud_and_service_tokens_redacted():
    assert redact_secrets("AKIAIOSFODNN7EXAMPLE") == REDACTED
    assert REDACTED in redact_secrets("xoxb-123456789012-abcdefghij")
    assert REDACTED in redact_secrets("ghp_" + "a" * 36)


def test_jwt_redacted():
    jwt = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSM12345"
    assert REDACTED in redact_secrets(jwt)


def test_private_key_marker_redacted():
    out = redact_secrets("-----BEGIN RSA PRIVATE KEY-----MIIabc")
    assert REDACTED in out
    assert "MIIabc" not in out


def test_plain_text_unchanged():
    assert redact_secrets("just a normal log line") == "just a normal log line"
    assert redact_secrets("") == ""


def test_never_raises_on_odd_input():
    # must be safe on anything a job might print.
    redact_secrets("%%%$$$### \x00 \t weird")
    redact_secrets("=:=:=:")


def test_basic_pattern_requires_authorization_context():
    # "basic" is an ordinary English word: an unanchored pattern redacted
    # innocent text like "basic understanding" wholesale.
    text = "a basic understanding of the system"
    assert redact_secrets(text) == text


def test_unquoted_json_values_do_not_swallow_trailing_structure():
    # an unquoted JSON scalar ends at the delimiter; redacting to end of
    # line here would swallow the rest of the object.
    out = redact_secrets('{"token": 12345, "user": "bob"}')
    assert "12345" not in out
    assert '"user": "bob"' in out


def test_private_key_kv_redacted():
    out = redact_secrets("PRIVATE_KEY=abc123def456")
    assert "abc123def456" not in out


def test_url_credentials_with_empty_username_redacted():
    # scheme://:PASSWORD@host -- how redis/mongodb/amqp connection strings
    # carry a password with no user.  Requiring a username here (the old
    # ``+``) leaked these verbatim while the archive was stamped redacted.
    for uri in (
        "redis://:SuperSecret123@cache:6379",
        "mongodb://:p4ssw0rd@db",
        "amqp://:hunter2@rabbit/vhost",
    ):
        out = redact_secrets(uri)
        assert REDACTED in out
    assert "SuperSecret123" not in redact_secrets(
        "redis://:SuperSecret123@cache:6379"
    )
    # a normal user:pass URL still redacts...
    assert "s3cret" not in redact_secrets("redis://user:s3cret@host")
    # ...and a plain host:port URL (no userinfo, no ``@``) is left untouched.
    assert redact_secrets("redis://cache:6379/0") == "redis://cache:6379/0"


def test_redact_lines_seeds_pem_when_begin_line_truncated():
    # the bounded live-log ring evicted the ``BEGIN`` header before archiving,
    # so the archived tail OPENS with the key's base64 body + END.  The body
    # must be redacted, not passed through, even with no BEGIN present.
    lines = [
        "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDktruncated",
        "AnotherBase64BodyLineWithKeyMaterialXYZ==",
        "-----END RSA PRIVATE KEY-----",
        "deploy finished ok",
    ]
    out = redact_lines(lines)
    assert out[0] == REDACTED
    assert out[1] == REDACTED
    assert out[2] == REDACTED
    assert "MIIEvQIBAD" not in "".join(out)
    # normal output AFTER the orphaned END passes through unredacted.
    assert out[3] == "deploy finished ok"


def test_redact_lines_complete_pem_block_unaffected_by_seed():
    # a self-contained block: BEGIN precedes END, so the mid-PEM seed is
    # False and the walk is byte-identical to before the fix.
    lines = [
        "starting",
        "-----BEGIN RSA PRIVATE KEY-----",
        "MIIEvQIBADANBgkqhkiG9w0BAQEF",
        "-----END RSA PRIVATE KEY-----",
        "done",
    ]
    out = redact_lines(lines)
    assert out[0] == "starting"
    assert all(line == REDACTED for line in out[1:4])
    assert out[4] == "done"
    assert "MIIEvQIBAD" not in "".join(out)
