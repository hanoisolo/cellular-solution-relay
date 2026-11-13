"""End-to-end test for the ConversationRelay server (no Twilio, no Mistral).

Run the server first with MOCK_LLM=1, then run this script:

    MOCK_LLM=1 .venv/bin/python server.py        # terminal 1
    .venv/bin/python test_ws.py                  # terminal 2

It connects to ws://localhost:8000/ws, sends a fake `setup` + `prompt`,
prints every message the server sends back, and asserts a `text` token
with last=true arrives. It then exercises the interrupt path and checks
the Twilio signature validator against an independently computed HMAC.

Set WS_URL to point at another host, e.g. WS_URL=ws://localhost:8000/ws.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import sys

import websockets

WS_URL = os.environ.get("WS_URL", "ws://localhost:8000/ws")

SETUP = {
    "type": "setup",
    "sessionId": "VXtest0000000000000000000000000000",
    "accountSid": "ACtest0000000000000000000000000000",
    "callSid": "CAtest0000000000000000000000000000",
    "from": "+14165550100",
    "to": "+14374754742",
    "callType": "PSTN",
    "direction": "inbound",
    "callStatus": "RINGING",
    "customParameters": {},
}

PROMPT = {
    "type": "prompt",
    "voicePrompt": (
        "Hi, I'm looking for a Bell deal for my business. "
        "My name is Don and my number is 416-555-0100, "
        "can someone call me back?"
    ),
    "lang": "en-CA",
    "last": True,
}


async def collect_until_last(ws, timeout=20.0):
    """Collect messages until a text token with last=true, or timeout."""
    received = []
    try:
        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
            msg = json.loads(raw)
            received.append(msg)
            print("RECV:", json.dumps(msg)[:300])
            if msg.get("type") == "text" and msg.get("last") is True:
                return received
    except asyncio.TimeoutError:
        print("TIMEOUT waiting for last=true token")
        return received


def check_truncation():
    """_truncate_to_utterance must cut a spoken reply to what was heard."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from server import _truncate_to_utterance

    spoken = (
        "Sure, the EPP discount is up to thirty percent off "
        "and there are no activation fees."
    )
    assert _truncate_to_utterance(spoken, "Sure, the EPP") == "Sure, the EPP"
    # utterance truncated mid-word by the interrupt
    assert _truncate_to_utterance(spoken, "Sure, the EPP disc") == "Sure, the EPP disc"
    # empty utterance: keep the whole reply
    assert _truncate_to_utterance(spoken, "") == spoken
    # utterance not in the reply at all: caller heard none of it
    assert _truncate_to_utterance(spoken, "zzz not there") == ""
    print("TRUNCATION CHECKS: pass")


def check_recording_gate():
    """should_disclose_recording gates the outbound recording notice. A live
    call opened with "Hello.", which the old affirmative-only gate rejected,
    so no notice played for 77 seconds and then it landed on "Uh, okay.
    That's it." on the way out."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from server import should_disclose_recording

    # First caller turn: disclose unless the caller clearly declined.
    for text in (
        "Hello.",
        "Yes.",
        "yeah, go ahead",
        "sure, I have a minute",
        "Who is this?",
        "What?",
        "Uh, okay.",
    ):
        assert should_disclose_recording(text, 1), text
    for text in ("No, I'm busy", "not now", "nope", ""):
        assert not should_disclose_recording(text, 1), text

    # Second caller turn: disclose regardless, so a call that keeps going can
    # never run without the notice.
    for text in ("No, I'm busy", "not now", ""):
        assert should_disclose_recording(text, 2), text
    print("RECORDING GATE CHECKS: pass")


def check_dead_end_guard():
    """is_dead_end catches the single-word replies a live call produced
    ("Okay." / "Alright.") after the caller declined the callback."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from server import is_dead_end, CLOSING_RE, DEAD_END_RECOVERIES

    for dead in ("Okay.", "Alright.", "Sure.", "Got it.", "", "Thanks."):
        assert is_dead_end(dead), dead
    for alive in (
        "Would you like the store to call you back?",
        "No problem. Is there anything else about EPP I can explain before we finish?",
        "The store is open Monday to Friday, 9am to 5pm.",
        "EPP gives employees of select partner companies up to 30 percent off "
        "Bell mobility plans and no activation fees.",
    ):
        assert not is_dead_end(alive), alive

    # A caller wrapping up must not be answered with another question.
    for closing in (
        "Uh, okay. That's it.",
        "That's all, thanks.",
        "Goodbye.",
        "Nothing else.",
        "I'm done.",
        "No thanks.",
    ):
        assert CLOSING_RE.search(closing), closing
    for not_closing in ("No.", "Hello.", "Tell me more.", "What are the fees?"):
        assert not CLOSING_RE.search(not_closing), not_closing

    # The recovery must not itself be a dead end, and the two variants must
    # differ so the guard cannot repeat itself.
    for recovery in DEAD_END_RECOVERIES:
        assert not is_dead_end(recovery), recovery
    assert DEAD_END_RECOVERIES[0] != DEAD_END_RECOVERIES[1], DEAD_END_RECOVERIES
    print("DEAD END GUARD CHECKS: pass")


def check_repeat_guard():
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from server import is_repeat, REPEAT_FALLBACKS, OUTBOUND_SYSTEM_PROMPT, SYSTEM_PROMPT

    assert is_repeat("Do you have a minute?", "Do you have a minute?")
    assert is_repeat("Do you  have a minute?", "do you have a minute?")
    assert not is_repeat("Do you have a minute?", "Sure, go ahead.")
    assert not is_repeat("", "anything")

    # Near-duplicates. These are the four pitches a live call
    # (CA106db13e99260b9376d66230bb1c2c3c) spoke back to back. Only the
    # opening words changed, so exact matching let the loop run to the end of
    # the call.
    pitch_a = (
        "Bell's Employee Purchase Program, or EPP, gives employees of select "
        "partner companies up to 30 percent off Bell mobility plans and waives "
        "activation fees. To check if your company is eligible, visit cellular "
        "solution dot c a, click Employee Offers, and type in your company "
        "name. If your company comes up, you're eligible. Proof of employment "
        "is shown after ordering. Would you like me to check your eligibility "
        "on the website, or should I arrange a callback from the store so they "
        "can confirm it for you?"
    )
    pitch_b = (
        "Great. EPP gives employees of select partner companies up to 30 "
        "percent off Bell mobility plans and waives activation fees. To check "
        "if your company is eligible, visit cellular solution dot c a, click "
        "Employee Offers, and type in your company name. If your company comes "
        "up, you're eligible. Proof of employment is shown after ordering. "
        "Would you like me to check your eligibility on the website, or should "
        "I arrange a callback from the store so they can confirm it for you?"
    )
    pitch_c = (
        "EPP gives employees of select partner companies up to 30 percent off "
        "Bell mobility plans and waives activation fees. To check if your "
        "company is eligible, visit cellular solution dot c a, click Employee "
        "Offers, and type in your company name. If your company comes up, "
        "you're eligible. Would you like me to check your eligibility on the "
        "website, or should I arrange a callback from the store so they can "
        "confirm it for you?"
    )
    pitch_d = (
        "Bell's Employee Purchase Program, or EPP, gives employees of select "
        "partner companies up to 30 percent off Bell mobility plans and waives "
        "activation fees. To check if your company is eligible, visit cellular "
        "solution dot c a, click Employee Offers, and type in your company "
        "name. If your company comes up, you're eligible. Would you like me to "
        "check your eligibility on the website, or should I arrange a callback "
        "from the store so they can confirm it for you?"
    )
    for prev, cur, label in (
        (pitch_a, pitch_b, "a->b"),
        (pitch_b, pitch_c, "b->c"),
        (pitch_c, pitch_d, "c->d"),
        (pitch_a, pitch_d, "a->d"),
    ):
        assert is_repeat(prev, cur), (
            f"near-duplicate pitch not caught ({label}): the model would loop"
        )

    # Legitimate consecutive replies must never be flagged: a false positive
    # makes the assistant say "sorry, I didn't catch that" to a caller it
    # understood perfectly.
    for cur, label in (
        ("Would you like the store to call you back with the details?", "callback offer"),
        ("Great, and what is your name?", "name ask"),
        (
            "The iPhone 17 256GB on SmartPay Ultra is 62 dollars 91 a month "
            "with tax. Would you like the store to confirm the other sizes?",
            "price answer",
        ),
        (
            "The store is open Monday to Friday, 9am to 5pm. Would you like me "
            "to arrange a callback?",
            "store hours",
        ),
        (
            "You can check at cellular solution dot c a, click Employee "
            "Offers, and type in your company name. Would you like the store "
            "to call you back?",
            "website directions",
        ),
        (
            "No problem at all. If you change your mind, the store is at "
            "905-264-2228. Have a good day.",
            "decline",
        ),
    ):
        assert not is_repeat(pitch_c, cur), (
            f"legitimate reply flagged as a repeat ({label}): {cur!r}"
        )
    assert not is_repeat("Sure.", "Go ahead."), "short acks flagged as a repeat"

    # The guard must not answer a loop with a loop of its own.
    assert len(REPEAT_FALLBACKS) == 2, REPEAT_FALLBACKS
    assert REPEAT_FALLBACKS[0] != REPEAT_FALLBACKS[1], REPEAT_FALLBACKS

    # The prompt has to stop the loop at the source too: the guard only
    # rewrites a reply after the model has already produced it.
    assert "Give the pitch ONCE" in OUTBOUND_SYSTEM_PROMPT, (
        "outbound prompt lost the say-the-pitch-once rule"
    )
    for name, prompt in (("outbound", OUTBOUND_SYSTEM_PROMPT), ("inbound", SYSTEM_PROMPT)):
        assert "Never repeat a previous reply" in prompt, (
            f"{name} prompt lost the never-repeat-a-reply rule"
        )
    print("REPEAT GUARD CHECKS: pass")


def check_pitch_phase():
    """The outbound prompt must change phase once the pitch has been spoken.

    A live call (CA253b72be5b160cbbf661c88739a2dbad) re-spoke the same 316-char
    pitch after the caller said "Hello." again. The pre-speech gate missed it
    because the turn in between was a short fallback, and the prompt itself was
    the real cause: it said "give the pitch ONCE" AND "if the first reply is
    just a greeting like 'hello', go straight into the pitch". The second rule
    won every time. The fix removes the trigger by swapping the system prompt
    after the pitch, so the phase-2 prompt must not contain it.
    """
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from server import (
        OUTBOUND_SYSTEM_PROMPT,
        OUTBOUND_SYSTEM_PROMPT_AFTER_PITCH,
        RECENT_REPLIES_KEPT,
        _pitch_delivered,
        _swap_to_after_pitch,
        new_call_state,
    )

    pitch = (
        "Bell's Employee Purchase Program, or EPP, gives employees of select "
        "partner companies up to 30 percent off Bell mobility plans and waives "
        "activation fees. Would you like the store to call you back?"
    )
    assert _pitch_delivered(pitch), "the pitch itself was not recognised as a pitch"
    assert not _pitch_delivered("Sure."), "a short ack was read as the pitch"
    assert not _pitch_delivered(
        "The store is open Monday to Friday, 9am to 5pm and they can confirm "
        "the exact price for the model you want. Would you like a callback?"
    ), "a long non-pitch reply was read as the pitch"
    # The model slips into written form sometimes. A miss here leaves the
    # pre-pitch prompt in place after the pitch, which is the whole bug.
    for written, label in (
        (
            "Bell's Employee Purchase Program gives employees of select "
            "partner companies 30% off Bell mobility plans and waives "
            "activation fees. Would you like the store to call you back?",
            "30%",
        ),
        (
            "Bell's Employee Purchase Program gives employees of select "
            "partner companies 30 % off Bell mobility plans. Would you like "
            "the store to call you back?",
            "30 %",
        ),
    ):
        assert _pitch_delivered(written), (
            f"pitch written as {label} was not recognised: the pre-pitch "
            "prompt would survive the pitch"
        )

    # Phase 1 keeps the trigger; phase 2 must not have it, or an unclear turn
    # resurrects the pitch script.
    assert "straight into the pitch" in OUTBOUND_SYSTEM_PROMPT.lower(), (
        "phase-1 prompt lost the pitch trigger it needs on a 'yes'"
    )
    assert "straight into the pitch" not in OUTBOUND_SYSTEM_PROMPT_AFTER_PITCH.lower(), (
        "phase-2 prompt still tells the model to go straight into the pitch: "
        "an unclear turn will re-pitch"
    )
    assert "ALREADY given the EPP pitch" in OUTBOUND_SYSTEM_PROMPT_AFTER_PITCH, (
        "phase-2 prompt does not tell the model the pitch is already given"
    )
    assert "Never repeat a previous reply" in OUTBOUND_SYSTEM_PROMPT_AFTER_PITCH, (
        "phase-2 prompt lost the never-repeat-a-reply rule"
    )
    # The late-pickup risk: a caller who only starts listening after the pitch
    # must still be able to learn what the call is about.
    assert "what the call is about" in OUTBOUND_SYSTEM_PROMPT_AFTER_PITCH, (
        "phase-2 prompt has no answer for 'what is this call about'"
    )

    # The swap itself: idempotent, and it replaces history[0] in place.
    state = new_call_state()
    assert state["pitch_delivered"] is False, state
    assert state["recent_replies"] == [], state
    history = [{"role": "system", "content": OUTBOUND_SYSTEM_PROMPT}]
    _swap_to_after_pitch(history, state, "CAtest")
    assert state["pitch_delivered"] is True, "the swap did not flag the phase"
    assert history[0]["role"] == "system", history[0]
    assert "ALREADY given the EPP pitch" in history[0]["content"], (
        "the swap did not install the phase-2 prompt"
    )
    history[0]["content"] = "sentinel"
    _swap_to_after_pitch(history, state, "CAtest")
    assert history[0]["content"] == "sentinel", "the swap is not idempotent"

    # The gate compares against a ring, not one reply: the turn between two
    # pitch attempts is often a short fallback.
    assert RECENT_REPLIES_KEPT >= 3, RECENT_REPLIES_KEPT
    print("PITCH PHASE CHECKS: pass — the pitch trigger is gone after the pitch")


def check_name_extraction():
    """extract_name must not read an "I'm <state>" reply as a name. A live
    call captured "Listening" from "I'm listening." and logged a
    CALLBACK_REQUEST for a person who does not exist."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from server import extract_name

    for utterance in (
        "I'm listening.",
        "I'm still here.",
        "I'm waiting.",
        "I'm good.",
        "I'm fine, thanks.",
        "I'm just checking.",
        "I'm not sure.",
        "I'm here.",
    ):
        assert extract_name(utterance) is None, (
            f"extract_name read a state reply as a name: {utterance!r} -> "
            f"{extract_name(utterance)!r}"
        )
    assert extract_name("My name is Don") == "Don"
    assert extract_name("I'm Don") == "Don"
    assert extract_name("This is Jonathan") == "Jonathan"
    print("NAME EXTRACTION CHECKS: pass")


def check_signature_validator():
    """Verify server.validate against an independently computed HMAC-SHA1."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from server import twilio_signature_valid

    token = "test-token-123"
    url = "https://example.com/twiml"
    params = {"CallSid": "CA123", "From": "+14165550100", "To": "+14374754742"}
    data = url + "".join(k + v for k, v in sorted(params.items()))
    digest = hmac.new(token.encode(), data.encode(), hashlib.sha1).digest()
    good_sig = base64.b64encode(digest).decode()

    assert twilio_signature_valid(url, params, good_sig, token), "good sig rejected"
    assert not twilio_signature_valid(url, params, "bogus", token), (
        "bad sig accepted"
    )
    assert not twilio_signature_valid(url, params, good_sig, "wrong-token"), (
        "wrong token accepted"
    )
    # WebSocket upgrade: no POST params, signature over the URL alone.
    ws_url = "wss://relay.example.com/ws"
    ws_data = ws_url
    ws_digest = hmac.new(token.encode(), ws_data.encode(), hashlib.sha1).digest()
    ws_sig = base64.b64encode(ws_digest).decode()
    assert twilio_signature_valid(ws_url, {}, ws_sig, token), (
        "ws upgrade sig rejected"
    )
    print("SIGNATURE CHECKS: pass")


def check_epp_offers_ws():
    """EPP offers blocks: with_offers() unit checks plus WS setup injection for
    inbound and outbound directions, via an in-process TestClient websocket.
    Only `setup` is sent, so no LLM call happens and no live server is needed."""
    import time

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import server as srv
    from fastapi.testclient import TestClient

    keys = ("RELAY_EPP_HARDWARE_OFFERS", "RELAY_EPP_PLAN_OFFERS")
    saved = {k: os.environ.get(k) for k in keys}
    try:
        for k in keys:
            os.environ.pop(k, None)
        assert srv.with_offers(srv.SYSTEM_PROMPT) == srv.SYSTEM_PROMPT, (
            "unset vars must leave the base prompt unchanged"
        )
        hw = ("HARDWARE OFFERS (Bell EPP, as of 2026-11-10) — Ontario (ON): "
              "Ahlo 1 128GB SmartPay Ultra: $1.13/mo with tax")
        pl = ("RATE PLAN OFFERS (Bell EPP, as of 2026-11-10) — Ontario (ON): "
              "Essential 50GB: $45.00/mo")
        os.environ["RELAY_EPP_HARDWARE_OFFERS"] = hw
        out = srv.with_offers(srv.SYSTEM_PROMPT)
        assert srv.SYSTEM_PROMPT in out and srv.EPP_OFFERS_GUARDRAIL in out
        assert f"VERIFIED HARDWARE OFFERS:\n{hw}" in out
        assert "VERIFIED RATE PLAN OFFERS" not in out
        os.environ["RELAY_EPP_PLAN_OFFERS"] = pl
        out = srv.with_offers(srv.OUTBOUND_SYSTEM_PROMPT)
        assert srv.OUTBOUND_SYSTEM_PROMPT in out and srv.EPP_OFFERS_GUARDRAIL in out
        assert f"VERIFIED HARDWARE OFFERS:\n{hw}" in out
        assert f"VERIFIED RATE PLAN OFFERS:\n{pl}" in out

        tc = TestClient(srv.app)
        srv.SESSIONS.clear()

        def ws_setup_and_check(call_sid, direction, expect_prompt):
            setup = dict(SETUP)
            setup["callSid"] = call_sid
            setup["direction"] = direction
            with tc.websocket_connect("/ws") as ws:
                ws.send_json(setup)
                for _ in range(100):
                    if call_sid in srv.SESSIONS:
                        break
                    time.sleep(0.05)
            assert call_sid in srv.SESSIONS, f"session not created for {call_sid}"
            assert srv.SESSIONS[call_sid][0]["role"] == "system"
            content = srv.SESSIONS[call_sid][0]["content"]
            assert expect_prompt in content, f"base prompt missing for {direction}"
            assert srv.EPP_OFFERS_GUARDRAIL in content
            assert f"VERIFIED HARDWARE OFFERS:\n{hw}" in content
            assert f"VERIFIED RATE PLAN OFFERS:\n{pl}" in content

        ws_setup_and_check("CAepp000000000000000000000000001", "inbound", srv.SYSTEM_PROMPT)
        ws_setup_and_check("CAepp000000000000000000000000002", "outbound-api", srv.OUTBOUND_SYSTEM_PROMPT)

        # Rollback: unset vars -> bare base prompt again.
        for k in keys:
            os.environ.pop(k, None)
        setup = dict(SETUP)
        setup["callSid"] = "CAepp000000000000000000000000003"
        with tc.websocket_connect("/ws") as ws:
            ws.send_json(setup)
            for _ in range(100):
                if setup["callSid"] in srv.SESSIONS:
                    break
                time.sleep(0.05)
        content = srv.SESSIONS["CAepp000000000000000000000000003"][0]["content"]
        assert content == srv.SYSTEM_PROMPT and "VERIFIED OFFERS" not in content, (
            "unset vars must restore the bare prompt"
        )
        print("EPP OFFERS WS CHECKS: pass")
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


async def check_idle_prompt():
    """A caller who goes quiet mid-call must be prompted, not left in dead
    air. Runs against the live server with its configured idle delay, so the
    wait is cancellable and a regression fails the gate instead of hanging
    it."""
    idle_after = float(os.environ.get("RELAY_IDLE_AFTER", "8") or 0)
    if idle_after <= 0:
        print("IDLE PROMPT: skipped — RELAY_IDLE_AFTER=0 disables it")
        return
    idle_text = os.environ.get("RELAY_IDLE_TEXT") or "Are you still there?"
    setup = dict(SETUP)
    setup["callSid"] = "CAidle000000000000000000000000001"
    async with websockets.connect(WS_URL, max_size=2**20) as ws:
        await ws.send(json.dumps(setup))
        for _ in range(3):  # greeting, disclosure, question
            await asyncio.wait_for(ws.recv(), timeout=10)
        await ws.send(
            json.dumps(
                {"type": "prompt", "voicePrompt": "What are your hours?", "last": True}
            )
        )
        await collect_until_last(ws)
        # Now go quiet: the idle prompt must arrive on its own.
        raw = await asyncio.wait_for(ws.recv(), timeout=idle_after + 8)
        msg = json.loads(raw)
        print("RECV (idle):", json.dumps(msg)[:200])
        assert msg.get("type") == "text" and msg.get("last") is True, (
            f"idle prompt must be a final text token: {msg}"
        )
        assert msg.get("token") == idle_text, (
            f"unexpected idle prompt: {msg.get('token')!r}"
        )
        assert msg.get("interruptible") is True, (
            f"idle prompt must be interruptible: {msg}"
        )
    print("IDLE PROMPT: pass — caller silence is answered, not left dead")


async def check_reconnect_state():
    """A dropped WebSocket must not lose the callback details the caller has
    already dictated. They live in CALL_STATE, not in the conversation
    history, so a reconnect has to restore them — otherwise the handoff
    payload arrives at the store with no name and no number."""
    sid = "CAtest0000000000000000000000000009"
    setup = dict(SETUP)
    setup["callSid"] = sid
    async with websockets.connect(WS_URL, max_size=2**20) as ws:
        await ws.send(json.dumps(setup))
        for _ in range(3):  # greeting, disclosure, question
            await asyncio.wait_for(ws.recv(), timeout=10)
        await ws.send(json.dumps(PROMPT))
        await collect_until_last(ws)
    # Socket dropped mid-call. Twilio reconnects the same call.
    setup_r = dict(setup)
    setup_r["connectionType"] = "reconnect"
    async with websockets.connect(WS_URL, max_size=2**20) as ws:
        await ws.send(json.dumps(setup_r))
        # A reconnect must not re-speak the opening.
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            raise AssertionError(f"reconnect re-spoke the opening: {raw}")
        except asyncio.TimeoutError:
            pass
        await ws.send(
            json.dumps(
                {
                    "type": "prompt",
                    "voicePrompt": "Actually, let me talk to a representative.",
                    "last": True,
                }
            )
        )
        end_msg = None
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
                m = json.loads(raw)
                if m.get("type") == "end":
                    end_msg = m
                    break
        except asyncio.TimeoutError:
            pass
        assert end_msg is not None, "no end message after reconnect handoff"
        hd = json.loads(end_msg.get("handoffData") or "{}")
        cb = hd.get("callback") or {}
        assert cb.get("name") == "Don" and cb.get("phone") == "+14165550100", (
            f"reconnect lost the callback details: {hd}"
        )
    print("RECONNECT STATE: pass — callback details survive a dropped socket")


async def check_pitch_phase_ws():
    """End-to-end: after the pitch is spoken, the model must be handed the
    phase-2 prompt. The mock reports which phase it sees, so this fails if the
    swap never happens — the exact condition that let a live call re-pitch."""
    setup = dict(SETUP)
    setup["callSid"] = "CAtest0000000000000000000000000010"
    setup["direction"] = "outbound-api"

    async def say(ws, text):
        await ws.send(json.dumps({"type": "prompt", "voicePrompt": text, "last": True}))
        acks = {"Sure.", "Go ahead.", "Of course."}
        tokens = []
        deadline = asyncio.get_event_loop().time() + 10
        while asyncio.get_event_loop().time() < deadline:
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
            m = json.loads(raw)
            if m.get("type") != "text":
                continue
            if m.get("interruptible") is False:
                continue  # the outbound greeting and the recording notice
            tok = m.get("token") or ""
            if tok.strip() in acks:
                continue  # the interrupt acknowledgement, spoken on its own
            tokens.append(tok)
            if m.get("last") is True:
                break
        return "".join(tokens).strip()

    async def speak_until(ws, marker):
        """Read the reply tokens until `marker` has been spoken, then return
        what was said. Timing-free: the interrupt lands exactly when the
        caller would have heard the marker, whatever the machine speed."""
        spoken = ""
        deadline = asyncio.get_event_loop().time() + 20
        while marker not in spoken and asyncio.get_event_loop().time() < deadline:
            raw = await asyncio.wait_for(ws.recv(), timeout=20)
            m = json.loads(raw)
            if m.get("type") != "text" or m.get("interruptible") is False:
                continue
            spoken += m.get("token") or ""
        assert marker in spoken, f"the reply never reached {marker!r}: {spoken!r}"
        return spoken

    async with websockets.connect(WS_URL, max_size=2**20) as ws:
        await ws.send(json.dumps(setup))
        await asyncio.sleep(0.5)
        before = await say(ws, "pitchref")
        assert "pre-pitch prompt is still in place" in before, (
            f"the call did not start in the pre-pitch phase: {before!r}"
        )
        pitch = await say(ws, "pitchme")
        assert "Employee Purchase Program" in pitch, f"no pitch spoken: {pitch!r}"
        after = await say(ws, "pitchref")
        assert "post-pitch prompt is in place" in after, (
            f"the system prompt was not swapped after the pitch: {after!r}"
        )
    print("PITCH PHASE SWAP: pass — phase-2 prompt installed after the pitch")

    # An interrupted pitch still counts as delivered: the caller heard the
    # pitch, so the pre-pitch prompt must not come back on the next turn.
    setup2 = dict(SETUP)
    setup2["callSid"] = "CAtest0000000000000000000000000011"
    setup2["direction"] = "outbound-api"
    async with websockets.connect(WS_URL, max_size=2**20) as ws:
        await ws.send(json.dumps(setup2))
        await asyncio.sleep(0.5)
        await ws.send(
            json.dumps({"type": "prompt", "voicePrompt": "pitchme", "last": True})
        )
        spoken = await speak_until(ws, "30 percent")
        await ws.send(
            json.dumps(
                {
                    "type": "interrupt",
                    "utteranceUntilInterrupt": spoken,
                }
            )
        )
        try:
            while True:
                await asyncio.wait_for(ws.recv(), timeout=0.5)
        except asyncio.TimeoutError:
            pass
        after = await say(ws, "pitchref")
        assert "post-pitch prompt is in place" in after, (
            f"an interrupted pitch left the call in the pre-pitch phase: {after!r}"
        )
    print("PITCH PHASE SWAP (interrupted): pass — a cut-in pitch still counts")

    # A reconnect must not restore the pre-pitch prompt. This holds because
    # _swap_to_after_pitch mutates history[0] in place on the list stored in
    # SESSIONS; the test guards that contract, so a future rewrite that
    # rebuilds the list instead of mutating it fails here.
    setup3 = dict(SETUP)
    setup3["callSid"] = "CAtest0000000000000000000000000012"
    setup3["direction"] = "outbound-api"
    async with websockets.connect(WS_URL, max_size=2**20) as ws:
        await ws.send(json.dumps(setup3))
        await asyncio.sleep(0.5)
        pitch = await say(ws, "pitchme")
        assert "Employee Purchase Program" in pitch, f"no pitch spoken: {pitch!r}"
    setup3_r = dict(setup3)
    setup3_r["connectionType"] = "reconnect"
    async with websockets.connect(WS_URL, max_size=2**20) as ws:
        await ws.send(json.dumps(setup3_r))
        await asyncio.sleep(0.5)
        after = await say(ws, "pitchref")
        assert "post-pitch prompt is in place" in after, (
            f"a reconnect restored the pre-pitch prompt: {after!r}"
        )
    print("PITCH PHASE SWAP (reconnect): pass — the phase survives a reconnect")

    # Twilio normalises numbers, so utteranceUntilInterrupt can fail to match
    # the spoken text verbatim. An empty `heard` means we cannot tell what the
    # caller heard; re-pitching is the worse failure, so the phase must still
    # advance from what was spoken.
    setup4 = dict(SETUP)
    setup4["callSid"] = "CAtest0000000000000000000000000013"
    setup4["direction"] = "outbound-api"
    async with websockets.connect(WS_URL, max_size=2**20) as ws:
        await ws.send(json.dumps(setup4))
        await asyncio.sleep(0.5)
        await ws.send(
            json.dumps({"type": "prompt", "voicePrompt": "pitchme", "last": True})
        )
        await speak_until(ws, "30 percent")
        await ws.send(
            json.dumps(
                {
                    "type": "interrupt",
                    "utteranceUntilInterrupt": "thirty percent off bell plans",
                }
            )
        )
        try:
            while True:
                await asyncio.wait_for(ws.recv(), timeout=0.5)
        except asyncio.TimeoutError:
            pass
        after = await say(ws, "pitchref")
        assert "post-pitch prompt is in place" in after, (
            f"an unmatched interrupt utterance left the call pre-pitch: {after!r}"
        )
    print("PITCH PHASE SWAP (unmatched utterance): pass — falls back to spoken")


async def main():
    print(f"Connecting to {WS_URL} ...")
    async with websockets.connect(WS_URL, max_size=2**20) as ws:
        print("Connected.")
        await ws.send(json.dumps(SETUP))
        print("SENT setup")
        # Inbound setup: greeting, then the AI+recording disclosure, then the
        # question — three non-interruptible text tokens (a caller saying
        # "hello" must not cut the opening off, and the question must come
        # last so the caller is invited to talk only after the notice).
        from server import WELCOME_GREETING, OPENING_QUESTION  # noqa: E402  (path set below)
        raw = await asyncio.wait_for(ws.recv(), timeout=10)
        greet = json.loads(raw)
        print("RECV:", json.dumps(greet)[:300])
        assert greet.get("type") == "text" and greet.get("last") is True, (
            f"greeting must be a final text token: {greet}"
        )
        assert greet.get("interruptible") is False, (
            f"greeting must be non-interruptible: {greet}"
        )
        assert greet.get("token") == WELCOME_GREETING, (
            f"first token must be the welcome greeting: {greet.get('token')!r}"
        )
        print("GREETING: pass — spoken by relay, non-interruptible")
        raw = await asyncio.wait_for(ws.recv(), timeout=10)
        disc = json.loads(raw)
        print("RECV:", json.dumps(disc)[:300])
        assert disc.get("type") == "text" and disc.get("last") is True, (
            f"disclosure must be a final text token: {disc}"
        )
        assert disc.get("interruptible") is False, (
            f"disclosure must be non-interruptible: {disc}"
        )
        token = disc.get("token", "").lower()
        assert "recorded" in token and "ai assistant" in token and "quality assurance" in token, (
            f"disclosure missing AI/recording wording: {disc.get('token')!r}"
        )
        print("DISCLOSURE: pass — non-interruptible AI+recording notice on setup")

        raw = await asyncio.wait_for(ws.recv(), timeout=10)
        question = json.loads(raw)
        print("RECV:", json.dumps(question)[:300])
        assert question.get("type") == "text" and question.get("last") is True, (
            f"opening question must be a final text token: {question}"
        )
        assert question.get("interruptible") is False, (
            f"opening question must be non-interruptible: {question}"
        )
        assert question.get("token") == OPENING_QUESTION, (
            f"opening must end with the question: {question.get('token')!r}"
        )
        print("OPENING QUESTION: pass — spoken last, non-interruptible")

        await ws.send(json.dumps(PROMPT))
        print("SENT prompt (callback request with name + phone)")

        received = await collect_until_last(ws)
        text_msgs = [m for m in received if m.get("type") == "text"]
        tokens = [m.get("token", "") for m in text_msgs]
        full_reply = "".join(tokens)
        print(f"\nFull reply ({len(text_msgs)} tokens): {full_reply!r}")

        assert text_msgs, "no text tokens received"
        assert any(m.get("last") is True for m in text_msgs), (
            "no last=true token received"
        )
        assert full_reply.strip(), "reply was empty"
        print("WS FLOW: pass — streamed tokens + last=true received")

        # Interrupt path: send a second prompt, then interrupt immediately.
        # The interrupt must be handled WHILE the reply streams: the stream
        # is cancelled, so no last=true token may arrive for this reply.
        await ws.send(
            json.dumps(
                {
                    "type": "prompt",
                    "voicePrompt": "Actually, what are your hours?",
                    "lang": "en-CA",
                    "last": True,
                }
            )
        )
        await ws.send(
            json.dumps(
                {"type": "interrupt", "utteranceUntilInterrupt": "Actually, wha"}
            )
        )
        print("SENT second prompt + interrupt")
        saw_last = False
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=2.0)
                msg = json.loads(raw)
                print("RECV (post-interrupt):", json.dumps(msg)[:200])
                if msg.get("type") == "text" and msg.get("last") is True:
                    saw_last = True
        except asyncio.TimeoutError:
            pass
        assert not saw_last, (
            "interrupted reply still sent last=true — stream was not cancelled live"
        )
        print("INTERRUPT PATH: pass — reply cancelled mid-stream, no last=true")

        # Recovery: a fresh prompt after the interrupt must still get a reply,
        # and it must open with a short acknowledgement — the caller talked
        # over the agent, so the agent yields the floor before answering.
        await ws.send(
            json.dumps(
                {
                    "type": "prompt",
                    "voicePrompt": "What are your store hours?",
                    "lang": "en-CA",
                    "last": True,
                }
            )
        )
        from server import interrupt_acks  # noqa: E402
        ack = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
        print("RECV (post-interrupt ack):", json.dumps(ack)[:200])
        assert ack.get("type") == "text" and ack.get("last") is True, (
            f"expected an acknowledgement token after the interrupt: {ack}"
        )
        assert ack.get("interruptible") is True, (
            f"acknowledgement must be interruptible: {ack}"
        )
        assert ack.get("token") in interrupt_acks(), (
            f"unexpected acknowledgement: {ack.get('token')!r}"
        )
        print("INTERRUPT ACK: pass — acknowledgement spoken before the answer")
        received = await collect_until_last(ws)
        assert any(
            m.get("type") == "text" and m.get("last") is True for m in received
        ), "no reply after interrupt — session did not recover"
        print("RECOVERY: pass — session answers again after an interrupt")

        # Handoff via keyword: asking for a representative must end the
        # session with an "end" message carrying handoffData.
        await ws.send(
            json.dumps(
                {
                    "type": "prompt",
                    "voicePrompt": "I'd like to speak to a representative please",
                    "lang": "en-CA",
                    "last": True,
                }
            )
        )
        end_msg = None
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=5)
                m = json.loads(raw)
                print("RECV (handoff):", json.dumps(m)[:200])
                if m.get("type") == "end":
                    end_msg = m
                    break
        except asyncio.TimeoutError:
            pass
        assert end_msg is not None, "no end message on representative request"
        hd = json.loads(end_msg.get("handoffData") or "{}")
        assert hd.get("reason") == "caller-requested-human" and hd.get("callSid"), (
            f"bad handoffData: {hd}"
        )
        print("HANDOFF (keyword): pass — end + handoffData sent")
        await ws.close()

    # Handoff via DTMF 0 on a fresh session.
    setup2 = dict(SETUP)
    setup2["callSid"] = "CAtest0000000000000000000000000001"
    async with websockets.connect(WS_URL, max_size=2**20) as ws2:
        await ws2.send(json.dumps(setup2))
        for _ in range(3):  # greeting, disclosure, question
            await asyncio.wait_for(ws2.recv(), timeout=10)
        await ws2.send(json.dumps({"type": "dtmf", "digit": "0"}))
        end_msg = None
        try:
            while True:
                raw = await asyncio.wait_for(ws2.recv(), timeout=5)
                m = json.loads(raw)
                if m.get("type") == "end":
                    end_msg = m
                    break
        except asyncio.TimeoutError:
            pass
        assert end_msg is not None, "no end message on DTMF 0"
        hd = json.loads(end_msg.get("handoffData") or "{}")
        assert hd.get("reason") == "dtmf-0", f"bad handoffData: {hd}"
        print("HANDOFF (dtmf 0): pass — end + handoffData sent")
        await ws2.close()

    # Outbound: NO disclosure token at setup (the greeting names the AI);
    # the recording notice must wait for a positive "minute" answer.
    setup3 = dict(SETUP)
    setup3["callSid"] = "CAtest0000000000000000000000000002"
    setup3["direction"] = "outbound-api"
    async with websockets.connect(WS_URL, max_size=2**20) as ws3:
        await ws3.send(json.dumps(setup3))
        from server import OUTBOUND_GREETING  # noqa: E402
        raw = await asyncio.wait_for(ws3.recv(), timeout=5)
        greet = json.loads(raw)
        assert greet.get("type") == "text" and greet.get("interruptible") is False \
            and greet.get("token") == OUTBOUND_GREETING, (
            f"outbound setup must speak the outbound greeting non-interruptibly: {greet}"
        )
        print("OUTBOUND GREETING: pass — non-interruptible, outbound wording")
        # Nothing else before the caller answers (no recording notice yet).
        try:
            raw = await asyncio.wait_for(ws3.recv(), timeout=1.0)
            raise AssertionError(f"unexpected message after outbound greeting: {raw}")
        except asyncio.TimeoutError:
            pass
        print("OUTBOUND SETUP: pass — no disclosure before a positive answer")

        await ws3.send(
            json.dumps(
                {"type": "prompt", "voicePrompt": "Yes, I have a minute.", "last": True}
            )
        )
        seen = []
        deadline = asyncio.get_event_loop().time() + 10
        while asyncio.get_event_loop().time() < deadline:
            raw = await asyncio.wait_for(ws3.recv(), timeout=10)
            m = json.loads(raw)
            seen.append(m)
            # stop at the LLM reply's final token (the notice is also last=true)
            if (m.get("type") == "text" and m.get("last") is True
                    and "recorded" not in (m.get("token") or "").lower()):
                break
        notices = [m for m in seen if m.get("type") == "text"
                   and "recorded" in (m.get("token") or "").lower()]
        assert notices, "no recording notice after affirmative answer"
        assert notices[0].get("interruptible") is False and notices[0].get("last") is True, (
            f"recording notice must be non-interruptible and final: {notices[0]}"
        )
        assert any("recorded" not in (m.get("token") or "").lower()
                   for m in seen if m.get("type") == "text"), "no LLM reply after notice"
        print("RECORDING NOTICE: pass — spoken non-interruptibly after 'yes'")
        await ws3.close()

    # Outbound + negative answer: no recording notice at all.
    setup4 = dict(SETUP)
    setup4["callSid"] = "CAtest0000000000000000000000000003"
    setup4["direction"] = "outbound-api"
    async with websockets.connect(WS_URL, max_size=2**20) as ws4:
        await ws4.send(json.dumps(setup4))
        await asyncio.sleep(0.5)
        await ws4.send(
            json.dumps(
                {"type": "prompt", "voicePrompt": "No, I'm busy right now.", "last": True}
            )
        )
        try:
            while True:
                raw = await asyncio.wait_for(ws4.recv(), timeout=4.0)
                m = json.loads(raw)
                assert "recorded" not in (m.get("token") or "").lower(), (
                    f"recording notice fired on a negative answer: {m}"
                )
        except asyncio.TimeoutError:
            pass
        print("NEGATIVE ANSWER: pass — no recording notice")
        await ws4.close()

    # Outbound + a bare greeting: the notice must still fire on the first
    # caller turn. A live call opened with "Hello.", the old affirmative-only
    # gate rejected it, and the notice did not play for 77 seconds.
    setup5 = dict(SETUP)
    setup5["callSid"] = "CAtest0000000000000000000000000004"
    setup5["direction"] = "outbound-api"
    async with websockets.connect(WS_URL, max_size=2**20) as ws5:
        await ws5.send(json.dumps(setup5))
        await asyncio.sleep(0.5)
        await ws5.send(
            json.dumps({"type": "prompt", "voicePrompt": "Hello.", "last": True})
        )
        seen = []
        notices = []
        deadline = asyncio.get_event_loop().time() + 10
        while asyncio.get_event_loop().time() < deadline:
            raw = await asyncio.wait_for(ws5.recv(), timeout=10)
            m = json.loads(raw)
            seen.append(m)
            if m.get("type") != "text":
                continue
            if m.get("interruptible") is False:
                # The outbound greeting and the recording notice are both
                # non-interruptible last=true tokens.
                if "recorded" in (m.get("token") or "").lower():
                    notices.append(m)
                continue
            if m.get("last") is True:
                break
        assert notices, "no recording notice after a bare greeting"
        assert notices[0].get("interruptible") is False, (
            f"recording notice must be non-interruptible: {notices[0]}"
        )
        print("GREETING DISCLOSURE: pass — notice fires on 'Hello.'")
        await ws5.close()

    # A dead-end reply from the model ("Okay.") must be replaced with a next
    # step, not spoken as-is. A live call said "Okay." and "Alright." twice in
    # a row after the caller declined the callback.
    setup6 = dict(SETUP)
    setup6["callSid"] = "CAtest0000000000000000000000000005"
    setup6["direction"] = "outbound-api"
    async with websockets.connect(WS_URL, max_size=2**20) as ws6:
        await ws6.send(json.dumps(setup6))
        await asyncio.sleep(0.5)
        await ws6.send(
            json.dumps({"type": "prompt", "voicePrompt": "deadend", "last": True})
        )
        tokens = []
        deadline = asyncio.get_event_loop().time() + 10
        while asyncio.get_event_loop().time() < deadline:
            raw = await asyncio.wait_for(ws6.recv(), timeout=10)
            m = json.loads(raw)
            if m.get("type") != "text":
                continue
            if m.get("interruptible") is False:
                continue  # the outbound greeting and the recording notice
            tokens.append(m.get("token") or "")
            if m.get("last") is True:
                break
        reply = "".join(tokens).strip()
        assert reply != "Okay.", "dead-end reply spoken as-is"
        assert "?" in reply, f"dead-end recovery has no question: {reply!r}"
        print("DEAD END GUARD: pass — 'Okay.' replaced with a next step")
        await ws6.close()

    # An interrupt must leave a one-shot note telling the model not to repeat
    # itself. The interrupt truncates the assistant turn to what the caller
    # heard, so it no longer reads as "I already gave the pitch" and a live
    # call re-emitted the same 372-char pitch verbatim on "Hello.".
    setup7 = dict(SETUP)
    setup7["callSid"] = "CAtest0000000000000000000000000006"
    setup7["direction"] = "outbound-api"
    async with websockets.connect(WS_URL, max_size=2**20) as ws7:
        await ws7.send(json.dumps(setup7))
        await asyncio.sleep(0.5)
        await ws7.send(
            json.dumps(
                {"type": "prompt", "voicePrompt": "Tell me about EPP.", "last": True}
            )
        )
        await asyncio.sleep(0.2)
        await ws7.send(
            json.dumps(
                {"type": "interrupt", "utteranceUntilInterrupt": "I'm the automated"}
            )
        )
        # Drain whatever the interrupted turn had already queued.
        try:
            while True:
                await asyncio.wait_for(ws7.recv(), timeout=0.5)
        except asyncio.TimeoutError:
            pass
        await ws7.send(
            json.dumps({"type": "prompt", "voicePrompt": "noteref", "last": True})
        )
        acks = {"Sure.", "Go ahead.", "Of course."}
        tokens = []
        deadline = asyncio.get_event_loop().time() + 10
        while asyncio.get_event_loop().time() < deadline:
            raw = await asyncio.wait_for(ws7.recv(), timeout=10)
            m = json.loads(raw)
            if m.get("type") != "text":
                continue
            if m.get("interruptible") is False:
                continue  # the outbound greeting and the recording notice
            tok = m.get("token") or ""
            if tok.strip() in acks:
                continue  # the interrupt acknowledgement
            tokens.append(tok)
            if m.get("last") is True:
                break
        reply = "".join(tokens).strip()
        assert "see the interrupt note" in reply, (
            f"interrupt note did not reach the model: {reply!r}"
        )
        print("INTERRUPT NOTE: pass — model told not to repeat after a cut-in")
        await ws7.close()

    # The same reply twice in a row must be replaced before it is spoken. A
    # live call re-emitted the same 364-char pitch after an interrupt; the
    # truncated assistant turn in history no longer looks like a completed
    # pitch, so the gate compares against state["last_reply"] instead.
    async def _collect(ws):
        tokens = []
        deadline = asyncio.get_event_loop().time() + 10
        while asyncio.get_event_loop().time() < deadline:
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
            m = json.loads(raw)
            if m.get("type") != "text":
                continue
            if m.get("interruptible") is False:
                continue  # the outbound greeting and the recording notice
            tokens.append(m.get("token") or "")
            if m.get("last") is True:
                break
        return "".join(tokens).strip()

    setup8 = dict(SETUP)
    setup8["callSid"] = "CAtest0000000000000000000000000007"
    setup8["direction"] = "outbound-api"
    async with websockets.connect(WS_URL, max_size=2**20) as ws8:
        await ws8.send(json.dumps(setup8))
        await asyncio.sleep(0.5)
        await ws8.send(
            json.dumps(
                {"type": "prompt", "voicePrompt": "Tell me about EPP.", "last": True}
            )
        )
        first = await _collect(ws8)
        assert "Employee Purchase Program" in first, first
        await ws8.send(
            json.dumps(
                {"type": "prompt", "voicePrompt": "Tell me about EPP.", "last": True}
            )
        )
        second = await _collect(ws8)
        assert second != first, f"the same reply was spoken twice: {second!r}"
        assert second in (
            "Sorry, I didn't catch that — could you say it again?",
            "I'm having trouble hearing you. Could you say that once more?",
        ), second
        print("PRE-SPEECH REPEAT GUARD: pass — a repeated reply is replaced")
        await ws8.close()

    check_signature_validator()
    check_name_extraction()
    check_truncation()
    check_recording_gate()
    check_dead_end_guard()
    check_repeat_guard()
    check_pitch_phase()
    check_epp_offers_ws()
    await check_idle_prompt()
    await check_reconnect_state()
    await check_pitch_phase_ws()
    print("\nALL TESTS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
