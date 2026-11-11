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


async def main():
    print(f"Connecting to {WS_URL} ...")
    async with websockets.connect(WS_URL, max_size=2**20) as ws:
        print("Connected.")
        await ws.send(json.dumps(SETUP))
        print("SENT setup")
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
        await asyncio.sleep(1.0)
        # Drain anything that arrived; the server must not crash.
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                print("RECV (post-interrupt):", json.dumps(json.loads(raw))[:200])
        except asyncio.TimeoutError:
            pass
        print("INTERRUPT PATH: pass — connection still alive after interrupt")
        await ws.close()

    check_signature_validator()
    print("\nALL TESTS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
