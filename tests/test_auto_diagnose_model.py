"""auto_diagnose.analyze() against the Opus 5.5 response shape, with a fake SDK."""
import sys
import types
from types import SimpleNamespace

import pytest

import auto_diagnose


def _install_fake_anthropic(monkeypatch, response):
    calls = []

    class _Messages:
        def create(self, **kwargs):
            calls.append(kwargs)
            return response

    class Anthropic:
        def __init__(self, **kwargs):
            self.beta = SimpleNamespace(messages=_Messages())

    monkeypatch.setitem(sys.modules, "anthropic", types.SimpleNamespace(Anthropic=Anthropic))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    return calls


def _msg(stop_reason, *blocks):
    return SimpleNamespace(stop_reason=stop_reason, content=list(blocks))


def test_reads_text_after_a_leading_thinking_block(monkeypatch):
    thinking = SimpleNamespace(type="thinking", thinking="", signature="sig")
    text = SimpleNamespace(
        type="text",
        text='```json\n{"root_cause": "x", "severity": "low", "safe_auto_fixes": [], '
             '"manual_actions": [], "diagnosis_summary": "ok"}\n```',
    )
    calls = _install_fake_anthropic(monkeypatch, _msg("end_turn", thinking, text))

    out = auto_diagnose.analyze({}, {"alert": "test"})

    assert out["root_cause"] == "x"
    assert calls[0]["model"] == auto_diagnose.DIAGNOSE_MODEL
    assert calls[0]["fallbacks"] == "default"
    assert "temperature" not in calls[0]


@pytest.mark.parametrize("stop_reason", ["refusal", "max_tokens"])
def test_an_unfinished_reply_takes_no_auto_action(monkeypatch, stop_reason):
    _install_fake_anthropic(monkeypatch, _msg(stop_reason))

    out = auto_diagnose.analyze({}, {"alert": "test"})

    assert out["safe_auto_fixes"] == []
    assert stop_reason in out["root_cause"]
