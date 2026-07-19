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
