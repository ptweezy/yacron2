"""Fleet-portable value contract for the durable-JSON seam (cronstable._json).

orjson (the optional ``speedups`` extra) and the stdlib disagree at the edges:
orjson silently rewrites NaN/Infinity to ``null`` and rejects >64-bit ints,
while the stdlib writes ``NaN``/``Infinity`` tokens orjson cannot parse and
widens ints without limit.  On a mixed fleet sharing one store that is silent
corruption or a permanently-unreadable record, so :func:`ensure_portable`
rejects those values UNIFORMLY -- the same accept/reject on every host, with or
without orjson -- and :func:`dumps_bytes` enforces it at every write.
"""

import importlib.util
import json as stdlib_json
import platform
import sys

import pytest

from cronstable import _json


@pytest.mark.parametrize(
    "bad",
    [
        float("inf"),
        float("-inf"),
        float("nan"),
        {"value": float("inf")},
        [1, 2, float("nan")],
        2**64,  # one past the unsigned-64 max orjson accepts
        -(2**63) - 1,  # one below the signed-64 min
        {"n": 2**70},
        {"deep": {"deeper": [float("inf")]}},
        "\ud800",  # lone high surrogate: stdlib writes it, orjson never can
        "\udfff",  # lone low surrogate
        {"value": "a\ud800b"},
        {"\udc00key": 1},  # surrogate hiding in an object KEY
        [1, ["x", "\ud9ab"]],
    ],
)
def test_ensure_portable_rejects_unportable(bad):
    with pytest.raises(_json.UnsupportedValue):
        _json.ensure_portable(bad)
    # dumps_bytes enforces the same contract on whichever backend is installed.
    with pytest.raises(_json.UnsupportedValue):
        _json.dumps_bytes(bad)


def test_ensure_portable_rejects_non_string_keys():
    with pytest.raises(_json.UnsupportedValue):
        _json.ensure_portable({1: "a"})


@pytest.mark.parametrize(
    "ok",
    [
        None,
        True,
        False,
        0,
        -(2**63),  # signed-64 min boundary is portable
        2**63,  # within unsigned-64: orjson accepts it
        2**64 - 1,  # unsigned-64 max boundary is portable
        1.5,
        -3.25,
        "a string",
        "café \U0001f600",  # well-formed non-ASCII incl. an astral pair
        {"k": [1, "two", 3.0, None, True]},
        {"nested": {"a": 1, "b": [False, "x"]}},
        [],
        {},
    ],
)
def test_portable_values_round_trip(ok):
    _json.ensure_portable(ok)  # no raise
    encoded = _json.dumps_bytes(ok)
    assert _json.loads(encoded) == ok
    # and the bytes are readable by the *other* backend too: the stdlib can
    # always parse what dumps_bytes emitted (no NaN/Infinity/oversized ints).
    assert stdlib_json.loads(encoded.decode("utf-8")) == ok


def test_bool_is_not_treated_as_out_of_range_int():
    # bool is an int subclass; ensure_portable must not trip on True/False.
    _json.ensure_portable({"flag": True, "other": False})
    assert _json.loads(_json.dumps_bytes({"flag": True})) == {"flag": True}


# ---------------------------------------------------------------------------
# The stdlib (no-orjson) flavour of the seam + the orjson re-raise arm.
# ---------------------------------------------------------------------------


def _load_json_without_orjson():
    """A fresh, independent copy of cronstable/_json.py with orjson absent.

    Forcing ``sys.modules['orjson'] = None`` makes the module's ``import
    orjson`` raise ImportError, so the ``else:`` (stdlib) flavour of
    dumps_bytes/loads/deepcopy_json is what gets defined. Loaded under a throw
    away name from the SAME source path, so the live ``cronstable._json`` (which
    has orjson) is left untouched.
    """
    saved = sys.modules.get("orjson")
    sys.modules["orjson"] = None
    try:
        spec = importlib.util.spec_from_file_location(
            "_json_no_orjson_copy", _json.__file__
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        if saved is None:
            del sys.modules["orjson"]
        else:
            sys.modules["orjson"] = saved
    return mod


def test_stdlib_flavour_round_trips_and_emits_compact_bytes():
    mod = _load_json_without_orjson()
    assert mod.orjson is None  # we really are on the stdlib path
    value = {"k": [1, "two", 3.0, None, True], "n": 2**63}
    encoded = mod.dumps_bytes(value)
    assert isinstance(encoded, bytes)
    # compact separators, no spaces
    assert b", " not in encoded and b": " not in encoded
    assert mod.loads(encoded) == value
    assert mod.loads(encoded.decode("utf-8")) == value


def test_stdlib_flavour_rejects_unportable_value():
    mod = _load_json_without_orjson()
    # the ensure_portable pre-walk runs on the untrusted stdlib path
    with pytest.raises(mod.UnsupportedValue):
        mod.dumps_bytes({"bad": float("inf")})
    with pytest.raises(mod.UnsupportedValue):
        mod.dumps_bytes(2**64)
    # a lone surrogate is the string-shaped split: the stdlib would happily
    # write b'{"value":"\\ud800"}' -- bytes no orjson node can parse or
    # rewrite -- so the gate must reject it here, before it is persisted.
    with pytest.raises(mod.UnsupportedValue):
        mod.dumps_bytes({"value": "\ud800"})


def test_stdlib_flavour_trusted_skips_walk_but_allow_nan_still_bites():
    mod = _load_json_without_orjson()
    # trusted skips ensure_portable, so an out-of-window int slips through the
    # walk and is widened by the stdlib (documented behaviour)
    assert mod.dumps_bytes(2**70, trusted=True) == str(2**70).encode("utf-8")
    # ...but allow_nan=False keeps a non-finite float an error even when trusted
    with pytest.raises(ValueError):
        mod.dumps_bytes(float("nan"), trusted=True)


def test_stdlib_flavour_deepcopy_json_is_a_distinct_equal_tree():
    mod = _load_json_without_orjson()
    original = {"a": [1, 2], "b": {"c": 3}}
    copy = mod.deepcopy_json(original)
    assert copy == original
    assert copy is not original
    copy["a"].append(99)
    assert original["a"] == [1, 2]  # the copy is independent


def test_orjson_is_installed_where_a_wheel_exists():
    # Guards the two tests below from going silently dead. They are
    # importorskip-guarded, and for a while orjson was in no test environment
    # at all, so both skipped in every CI cell and the accelerated JSON arm
    # shipped unexercised. requirements_dev.txt now installs orjson wherever a
    # wheel is reliably available; this fails loudly if that line is dropped
    # or its markers stop matching, instead of degrading back to a skip.
    if sys.platform == "win32" and platform.machine().upper() == "ARM64":
        pytest.skip("no orjson wheel for win-arm64; it builds only with Rust")
    if sys.version_info >= (3, 15):
        pytest.skip("orjson may not have built for this Python yet")
    assert importlib.util.find_spec("orjson") is not None, (
        "orjson is missing from this environment, so the orjson tests below "
        "silently skip. Check the orjson line in requirements_dev.txt."
    )


def test_orjson_flavour_reraises_unserializable_after_portable_check():
    # live module (orjson installed): a value orjson cannot encode but that is
    # NOT a portability violation (a set) makes ensure_portable pass, so the
    # original orjson error re-raises (the `raise` after the recheck).
    # orjson is the optional `speedups` extra. requirements_dev.txt installs
    # it for the cells that have wheels (see the guard test above); this skip
    # only covers the ones that genuinely cannot.
    pytest.importorskip("orjson")
    assert _json.orjson is not None
    with pytest.raises(TypeError):
        _json.dumps_bytes({"s": {1, 2, 3}})


def test_orjson_flavour_deepcopy_json_round_trips():
    pytest.importorskip("orjson")
    assert _json.orjson is not None
    original = {"x": [1, {"y": 2}]}
    copy = _json.deepcopy_json(original)
    assert copy == original and copy is not original


# ---------------------------------------------------------------------------
# fuzzing findings: the depth bound, and out-of-window integer LITERALS
# ---------------------------------------------------------------------------


def _nested_list(depth):
    root = current = []
    for _ in range(depth - 1):
        child = []
        current.append(child)
        current = child
    return root


@pytest.mark.parametrize("flavour", ["installed", "stdlib"])
def test_depth_bound_is_uniform_and_never_a_recursion_error(flavour):
    # orjson's encoder hard-fails at depth 256 while its parser reads to
    # 1024 and the stdlib reaches ~1000 both ways, so without a shared
    # bound a stdlib node could persist a document every orjson node could
    # READ but never WRITE BACK -- wedging every read-modify-write path.
    # MAX_DEPTH is enforced by an explicit counter in the gate (and in the
    # orjson pre-walk), identically on both backends.
    mod = _json if flavour == "installed" else _load_json_without_orjson()
    ok = _nested_list(mod.MAX_DEPTH)
    mod.ensure_portable(ok)
    assert mod.loads(mod.dumps_bytes(ok)) == ok
    for too_deep in (
        _nested_list(mod.MAX_DEPTH + 1),
        _nested_list(255),  # previously: stdlib-writable, orjson-unwritable
        _nested_list(100_000),  # far beyond the interpreter stack
    ):
        with pytest.raises(mod.UnsupportedValue):
            mod.ensure_portable(too_deep)
        with pytest.raises(mod.UnsupportedValue):
            mod.dumps_bytes(too_deep)
    # dict nesting counts too
    deep_dict = tip = {}
    for _ in range(mod.MAX_DEPTH):
        tip["k"] = {}
        tip = tip["k"]
    with pytest.raises(mod.UnsupportedValue):
        mod.ensure_portable(deep_dict)


def test_gate_on_a_parsed_deep_body_raises_unsupported_not_recursion():
    # a ~2KB request body parses fine (loads limit is deeper than the
    # gate's), and the gate must classify it cleanly: dagrun's fan-out
    # guard catches UnsupportedValue only, so a RecursionError here used to
    # crash the whole run advance.
    parsed = _json.loads(("[" * 1000 + "]" * 1000).encode())
    with pytest.raises(_json.UnsupportedValue):
        _json.ensure_portable(parsed)


@pytest.mark.parametrize("flavour", ["installed", "stdlib"])
def test_loads_rejects_out_of_window_integer_literals_uniformly(flavour):
    # the stdlib preserves 2**64 as an exact (unportable) int the gate then
    # rejects, while orjson silently narrowed it to a lossy FLOAT the gate
    # happily accepted -- so a mapped fan-out saw different data depending
    # on which host parsed the bytes.  loads() now rejects such literals
    # identically on both backends.
    mod = _json if flavour == "installed" else _load_json_without_orjson()
    for blob in (
        b"[18446744073709551616]",  # 2**64, one past the unsigned max
        b'{"v": -9223372036854775809}',  # one below the signed min
        b"[99999999999999999999999999]",
        '{"v": 18446744073709551616}',  # str input too
    ):
        with pytest.raises(mod.UnsupportedValue):
            mod.loads(blob)
    # boundary and near-boundary literals still parse EXACTLY
    assert mod.loads(b"[9223372036854775807]") == [2**63 - 1]
    assert mod.loads(b"[-9223372036854775808]") == [-(2**63)]
    assert mod.loads(b"[18446744073709551615]") == [2**64 - 1]
    # 19+ digit runs inside strings or float literals are not integer
    # literals: the prescan may trip, the verification must not reject
    assert mod.loads(b'{"id": "12345678901234567890123"}') == {
        "id": "12345678901234567890123"
    }
    assert mod.loads(b"[1234567890123456789.5]") == [1234567890123456789.5]
    assert mod.loads(b'{"t": 1752900000000000000}') == {
        "t": 1752900000000000000  # 19-digit epoch-nanos, in-window
    }
