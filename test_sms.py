#!/usr/bin/env python3
"""Headless route & contract parity tests for the relay's HTTP surface.

Run (Git Bash, from relay-server/):

    MOCK_LLM=1 .venv/Scripts/python.exe test_sms.py

Uses fastapi.testclient.TestClient — in-process, no external port, no live
network. Anti-drift invariant (API-server row of STACK_MATRIX.md): every
route registered on the app has a test asserting status code, response
shape, and 4xx rejection (missing/bad Twilio signature -> 403, wrong
method -> 405). Also covers the /sms TwiML <Message> reply, XML escaping,
per-From SMS history, and the env-gated send_sms helper.

test_ws.py (the WebSocket suite) is the separate non-regression gate and is
intentionally not modified here.
"""

import asyncio
import base64
import hashlib
import hmac
import os
import sys
import xml.etree.ElementTree as ET

os.environ["MOCK_LLM"] = "1"
os.environ.pop("TWILIO_AUTH_TOKEN", None)
for _k in ("TWILIO_ACCOUNT_SID", "TWILIO_FROM", "TWILIO_API_KEY_SID",
           "TWILIO_API_KEY_SECRET", "STORE_SMS_NUMBER",
           "RELAY_SMS_SYSTEM_PROMPT", "RELAY_WELCOME_GREETING",
           "RELAY_OUTBOUND_GREETING"):
    os.environ.pop(_k, None)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fastapi.testclient import TestClient  # noqa: E402

import server  # noqa: E402
from server import (  # noqa: E402
    app,
    message_twiml_xml,
    send_sms,
    SMS_SYSTEM_PROMPT,
    SMS_SESSIONS,
    SYSTEM_PROMPT,
    OUTBOUND_SYSTEM_PROMPT,
    WELCOME_GREETING,
    OUTBOUND_GREETING,
)

client = TestClient(app)
BASE = "http://testserver"
PASSED = []


def check(name, cond, detail=""):
    if not cond:
        print(f"FAIL {name}: {detail}")
        sys.exit(1)
    PASSED.append(name)
    print(f"pass: {name}")


def sign(url, params, token):
    data = url + "".join(k + v for k, v in sorted(params.items()))
    digest = hmac.new(token.encode(), data.encode(), hashlib.sha1).digest()
    return base64.b64encode(digest).decode()


def sms_params(body="hello", from_="+14165550100", to="+14374754742"):
    return {"From": from_, "To": to, "Body": body, "MessageSid": "SMtest123"}


def post_signed(path, params, token, sig=None):
    url = BASE + path
    if sig is None:
        sig = sign(url, params, token)
    return client.post(path, data=params, headers={"X-Twilio-Signature": sig})


def main():
    # --- Route inventory parity: every registered route is covered below ---
    # (FastAPI's built-in /docs, /openapi.json, /redoc are framework routes.)
    BUILTIN = {"/docs", "/docs/oauth2-redirect", "/openapi.json", "/redoc"}
    http_paths = {r.path for r in app.routes
                  if hasattr(r, "methods") and r.path not in BUILTIN}
    ws_paths = {r.path for r in app.routes if not hasattr(r, "methods")}
    check("route inventory", http_paths == {"/", "/health", "/twiml", "/twiml-outbound", "/sms", "/handoff", "/amd"},
          f"http routes: {sorted(http_paths)}")
    check("websocket inventory", ws_paths == {"/ws"}, f"ws routes: {sorted(ws_paths)}")

    # --- / and /health: shape + 405 on wrong method ---
    r = client.get("/")
    check("GET /", r.status_code == 200 and r.json() == {"status": "ok", "service": "cellular-solution-relay"},
          f"{r.status_code} {r.text[:100]}")
    check("POST / -> 405", client.post("/").status_code == 405)
    r = client.get("/health")
    check("GET /health", r.status_code == 200 and r.json() == {"status": "ok"}, f"{r.status_code}")
    check("POST /health -> 405", client.post("/health").status_code == 405)

    # --- /twiml and /twiml-outbound: GET renders ConversationRelay TwiML ---
    r = client.get("/twiml")
    root = ET.fromstring(r.content)
    cr = root.find("./Connect/ConversationRelay")
    check("GET /twiml", r.status_code == 200 and cr is not None
          and cr.get("welcomeGreeting") == WELCOME_GREETING,
          f"{r.status_code} {r.text[:200]}")
    r = client.get("/twiml-outbound")
    cr = ET.fromstring(r.content).find("./Connect/ConversationRelay")
    check("GET /twiml-outbound", r.status_code == 200 and cr is not None
          and cr.get("welcomeGreeting") == OUTBOUND_GREETING,
          f"{r.status_code} {r.text[:200]}")

    # --- ConversationRelay hardening attributes (2026-11-11 tips pass) ---
    r = client.get("/twiml")
    cr = ET.fromstring(r.content).find("./Connect/ConversationRelay")
    check("ConversationRelay ignoreBackchannel + dtmfDetection enabled",
          cr.get("ignoreBackchannel") == "true" and cr.get("dtmfDetection") == "true",
          f"attrs: {dict(cr.attrib)}")
    check("ConversationRelay hints cover carriers + device IDs + store number",
          all(w in (cr.get("hints") or "")
              for w in ("Bell", "Rogers", "Telus", "eSIM", "EID", "IMEI", "EPP", "905-264-2228")),
          f"hints: {cr.get('hints')}")
    check("ConversationRelay keeps welcomeGreeting + reconnect",
          cr.get("welcomeGreeting") == WELCOME_GREETING and cr.get("applicationReconnect") == "true",
          f"attrs: {dict(cr.attrib)}")
    connect_el = ET.fromstring(r.content).find("./Connect")
    check("Connect action URL points at /handoff (crash/handoff fallback)",
          (connect_el.get("action") or "").endswith("/handoff"),
          f"action: {connect_el.get('action')}")

    # --- Shared keep-alive Mistral client: one per event loop ---
    c1 = server._mistral_client()
    c2 = server._mistral_client()
    check("mistral httpx client reused within a loop", c1 is c2 and not c1.is_closed,
          f"c1 is c2: {c1 is c2}, closed: {c1.is_closed}")

    # --- /sms: POST only, TwiML <Message> with the mock reply ---
    check("GET /sms -> 405", client.get("/sms").status_code == 405)

    SMS_SESSIONS.clear()
    p = sms_params("Hi, what deals do you have?")
    r = client.post("/sms", data=p)
    check("POST /sms 200 xml", r.status_code == 200 and "application/xml" in r.headers.get("content-type", ""),
          f"{r.status_code} {r.headers.get('content-type')}")
    msg = ET.fromstring(r.content).find("./Message")
    check("POST /sms <Message> mock reply", msg is not None and "Employee Purchase Program" in (msg.text or ""),
          f"body: {r.text[:200]}")

    p = sms_params("I want a callback please, my number is 416-555-0100")
    r = client.post("/sms", data=p)
    msg = ET.fromstring(r.content).find("./Message")
    check("POST /sms callback mock reply", msg is not None and "callback" in (msg.text or "").lower(),
          f"body: {r.text[:200]}")

    # Per-From history: system + 2 user + 2 assistant after two messages.
    hist = SMS_SESSIONS["+14165550100"]
    check("sms history accumulates per From", len(hist) == 5 and hist[0]["role"] == "system"
          and hist[1]["role"] == "user" and hist[-1]["role"] == "assistant",
          f"len={len(hist)}")
    check("sms system prompt is text-native", "cellularsolution.ca" in SMS_SYSTEM_PROMPT
          and "cellular solution dot c a" not in SMS_SYSTEM_PROMPT)
    check("sms history system prompt", "cellularsolution.ca" in hist[0]["content"]
          and "cellular solution dot c a" not in hist[0]["content"])
    # The voice-only "dot c a" rewrite must never touch an SMS reply:
    # the URL goes out verbatim in the <Message> body.
    sms_reply = "Check eligibility at cellularsolution.ca under Employee Offers."
    check("sms reply keeps URL verbatim",
          ET.fromstring(message_twiml_xml(sms_reply)).find("./Message").text == sms_reply)

    # A different From gets its own thread.
    client.post("/sms", data=sms_params("hello", from_="+14165550199"))
    check("sms sessions keyed by From", "+14165550199" in SMS_SESSIONS
          and len(SMS_SESSIONS["+14165550199"]) == 3)

    # Empty Body -> empty <Response/>, no LLM turn added.
    before = len(SMS_SESSIONS["+14165550199"])
    r = client.post("/sms", data=sms_params("", from_="+14165550199"))
    root = ET.fromstring(r.content)
    check("POST /sms empty body -> empty Response", r.status_code == 200
          and root.find("./Message") is None and len(SMS_SESSIONS["+14165550199"]) == before,
          f"{r.status_code} {r.text[:120]}")

    # --- XML escaping: special chars survive a round trip through TwiML ---
    tricky = '5 < 6 & "quoted" > 2'
    xml = message_twiml_xml(tricky)
    check("message_twiml_xml escapes body", ET.fromstring(xml).find("./Message").text == tricky,
          f"xml: {xml}")

    # --- Signature validation on POST /sms, /twiml, /twiml-outbound ---
    os.environ["TWILIO_AUTH_TOKEN"] = "tok-123"
    p = sms_params("signed hello")
    r = client.post("/sms", data=p)
    check("POST /sms missing signature -> 403", r.status_code == 403, f"{r.status_code}")
    r = client.post("/sms", data=p, headers={"X-Twilio-Signature": "bogus"})
    check("POST /sms bad signature -> 403", r.status_code == 403, f"{r.status_code}")
    r = post_signed("/sms", p, "tok-123")
    check("POST /sms good signature -> 200", r.status_code == 200, f"{r.status_code} {r.text[:120]}")
    r = post_signed("/sms", p, "wrong-token")
    check("POST /sms wrong-token signature -> 403", r.status_code == 403, f"{r.status_code}")

    p2 = {"CallSid": "CA123", "From": "+14165550100", "To": "+14374754742"}
    check("POST /twiml missing signature -> 403", client.post("/twiml", data=p2).status_code == 403)
    check("POST /twiml bad signature -> 403",
          client.post("/twiml", data=p2, headers={"X-Twilio-Signature": "bogus"}).status_code == 403)
    check("POST /twiml good signature -> 200", post_signed("/twiml", p2, "tok-123").status_code == 200)
    check("POST /twiml-outbound bad signature -> 403",
          client.post("/twiml-outbound", data=p2, headers={"X-Twilio-Signature": "bogus"}).status_code == 403)
    check("POST /twiml-outbound good signature -> 200",
          post_signed("/twiml-outbound", p2, "tok-123").status_code == 200)
    # Regression: Twilio signs blank-valued POST params too (e.g. ForwardedFrom=);
    # dropping them server-side must not reject a legitimately signed request.
    p3 = dict(p2)
    p3["ForwardedFrom"] = ""
    check("POST /twiml blank param + good signature -> 200",
          post_signed("/twiml", p3, "tok-123").status_code == 200)
    check("POST /twiml-outbound blank param + good signature -> 200",
          post_signed("/twiml-outbound", p3, "tok-123").status_code == 200)
    p4 = sms_params("hello with blank")
    p4["MediaUrl0"] = ""
    check("POST /sms blank param + good signature -> 200",
          post_signed("/sms", p4, "tok-123").status_code == 200)

    # --- /handoff: <Connect> action URL; POST-only, signature-checked, dials store ---
    check("GET /handoff -> 405", client.get("/handoff").status_code == 405)
    hp = {"CallSid": "CA123", "HandoffData": '{"reason": "caller-requested-human"}'}
    check("POST /handoff missing signature -> 403",
          client.post("/handoff", data=hp).status_code == 403)
    check("POST /handoff bad signature -> 403",
          client.post("/handoff", data=hp, headers={"X-Twilio-Signature": "bogus"}).status_code == 403)
    r = post_signed("/handoff", hp, "tok-123")
    dial = ET.fromstring(r.content).find("./Dial/Number")
    check("POST /handoff good signature -> 200 Dial store line",
          r.status_code == 200 and dial is not None and dial.text == "+19052642228",
          f"{r.status_code} {r.text[:200]}")

    # --- /amd: AsyncAmdStatusCallback; POST-only, signature-checked, empty 200 ---
    check("GET /amd -> 405", client.get("/amd").status_code == 405)
    ap = {"CallSid": "CA123", "AnsweredBy": "machine_start"}
    check("POST /amd missing signature -> 403",
          client.post("/amd", data=ap).status_code == 403)
    check("POST /amd bad signature -> 403",
          client.post("/amd", data=ap, headers={"X-Twilio-Signature": "bogus"}).status_code == 403)
    r = post_signed("/amd", ap, "tok-123")
    check("POST /amd good signature -> 200 empty Response",
          r.status_code == 200 and ET.fromstring(r.content).find("./Message") is None
          and ET.fromstring(r.content).find("./Dial") is None,
          f"{r.status_code} {r.text[:120]}")
    del os.environ["TWILIO_AUTH_TOKEN"]

    # --- send_sms is env-gated: no Twilio env -> None, no network ---
    result = asyncio.run(send_sms("+14165550199", "test"))
    check("send_sms without env returns None", result is None, f"got {result!r}")

    # --- Prompt guardrails: the agent must never imply EPP does not exist ---
    # (regression: an inbound caller asking for the "EPP department" got
    # "we don't have a separate EPP department" — reads as denying EPP)
    for name, prompt in (("inbound", SYSTEM_PROMPT),
                         ("outbound", OUTBOUND_SYSTEM_PROMPT),
                         ("sms", SMS_SYSTEM_PROMPT)):
        check(f"{name} prompt: never deny EPP / no fake department",
              "never say we do not have EPP" in prompt and "no separate department" in prompt,
              f"guardrail missing from {name} prompt")

    print(f"\nALL {len(PASSED)} TESTS PASSED")


if __name__ == "__main__":
    main()
