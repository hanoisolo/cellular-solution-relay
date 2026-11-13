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


def check_affirmative():
    """is_affirmative gates the outbound recording notice."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from server import is_affirmative

    for yes in ("Yes.", "yeah, go ahead", "sure, I have a minute", "ok", "of course"):
        assert is_affirmative(yes), yes
    for no in ("No, I'm busy", "not now", "", "what is this about?", "nope"):
        assert not is_affirmative(no), no
    print("AFFIRMATIVE CHECKS: pass")


def check_repeat_guard():
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from server import is_repeat

    assert is_repeat("Do you have a minute?", "Do you have a minute?")
    assert is_repeat("Do you  have a minute?", "do you have a minute?")
    assert not is_repeat("Do you have a minute?", "Sure, go ahead.")
    assert not is_repeat("", "anything")
    print("REPEAT GUARD CHECKS: pass")


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

        # Recovery: a fresh prompt after the interrupt must still get a reply.
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

    check_signature_validator()
    check_truncation()
    check_affirmative()
    check_repeat_guard()
    check_epp_offers_ws()
    print("\nALL TESTS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
