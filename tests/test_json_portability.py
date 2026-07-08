"""Fleet-portable value contract for the durable-JSON seam (cronstable._json).

orjson (the optional ``speedups`` extra) and the stdlib disagree at the edges:
orjson silently rewrites NaN/Infinity to ``null`` and rejects >64-bit ints,
while the stdlib writes ``NaN``/``Infinity`` tokens orjson cannot parse and
widens ints without limit.  On a mixed fleet sharing one store that is silent
corruption or a permanently-unreadable record, so :func:`ensure_portable`
rejects those values UNIFORMLY -- the same accept/reject on every host, with or
without orjson -- and :func:`dumps_bytes` enforces it at every write.
"""

import json as stdlib_json

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
