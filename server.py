"""Cellular Solution AI voice agent — Twilio ConversationRelay server.

Twilio handles the phone call, speech-to-text and text-to-speech. This
server is the brain: it receives transcribed caller speech over a WebSocket,
asks Mistral (mistral-large-4) for a reply, and streams the reply back as
text tokens for Twilio to speak.

Protocol verified against Twilio docs (2026-11-11):
- https://www.twilio.com/docs/voice/conversationrelay/websocket-messages
- https://www.twilio.com/docs/voice/twiml/connect/conversationrelay

Inbound messages: setup, prompt, dtmf, interrupt, info, error.
Outbound messages: text ({type, token, last, interruptible, preemptible}),
play, end ({type, handoffData}).

Credentials come from environment variables only. Nothing secret is ever
written to a file or a log by this server.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
from datetime import datetime, timezone
from urllib.parse import parse_qsl
from xml.sax.saxutils import escape as _xml_escape


def escape_attr(value: str) -> str:
    """Escape a value for use inside a double-quoted XML attribute."""
    return _xml_escape(value, {'"': "&quot;"})

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import Response

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("relay")

MISTRAL_API_KEY = os.environ.get("MISTRAL_API_KEY", "")
MISTRAL_MODEL = os.environ.get("MISTRAL_MODEL", "mistral-large-4")
MISTRAL_URL = "https://api.mistral.ai/v1/chat/completions"
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "")
RELAY_WS_URL = os.environ.get("RELAY_WS_URL", "")
MOCK_LLM = os.environ.get("MOCK_LLM", "") == "1"
PORT = int(os.environ.get("PORT", "8000"))

# Optional voice overrides. en-CA is NOT supported by ConversationRelay on
# this account: Twilio error 64101 rejected Google TTS for en-CA twice
# (en-CA-Standard-A, then Google.en-CA-Neural2-A) and then Google STT
# telephony for en-CA. Language is en-US until Twilio supports en-CA here;
# the voice falls back to Twilio's account defaults (ttsProvider/voice
# omitted from /twiml for the same reason).
RELAY_LANGUAGE = os.environ.get("RELAY_LANGUAGE", "en-US")
RELAY_TTS_PROVIDER = os.environ.get("RELAY_TTS_PROVIDER", "")
RELAY_TTS_VOICE = os.environ.get("RELAY_TTS_VOICE", "")
RELAY_STT_PROVIDER = os.environ.get("RELAY_STT_PROVIDER", "Google")

DEFAULT_HINTS = "Bell, EPP, Employee Purchase Program, mobility, callback"


def _env_or(name: str, default: str) -> str:
    """Runtime-overridable wording: set RELAY_* env vars (e.g. on Railway) to
    change greetings, system prompts or hints without editing code. Read per
    request, not at import time. An empty value falls back to the default."""
    return os.environ.get(name) or default

WELCOME_GREETING = (
    "Hi, thanks for calling Cellular Solution. "
    "You're speaking with our automated assistant. "
    "How can I help you today?"
)

# Outbound calls (the agent dials a lead) must NOT sound like an inbound
# call: the first test calls used the inbound greeting for an outbound
# dial, so the person who picked up heard "thanks for calling" from a
# call they never made. The outbound greeting introduces the call instead.
OUTBOUND_GREETING = (
    "Hi, this is Cellular Solution's automated assistant calling about "
    "Bell's Employee Purchase Program. Do you have a quick minute?"
)

OUTBOUND_SYSTEM_PROMPT = (
    "You are Cellular Solution's automated voice assistant, a Bell authorized "
    "dealer at 81 Zenway Blvd, Unit 6, Vaughan, Ontario. Store hours are "
    "Monday to Friday, 9am to 5pm. Store phone: 905-264-2228. "
    "You are Cellular Solution's automated assistant, not a person. The "
    "greeting already introduced you as one, so do not repeat the disclosure "
    "at all. "
    "YOU called this person; they did not call you. Never say 'thanks for "
    "calling' and never ask 'how can I help you' as an opening — you called "
    "about Bell's Employee Purchase Program, known as EPP: up to 30 percent "
    "off Bell mobility plans and no activation fees. When the person says "
    "yes to having a minute, go straight into the EPP pitch — do not ask "
    "'do you want to know about EPP' or any other permission question "
    "first. If the person's first reply is unclear, garbled, or just a "
    "greeting like 'hello', do NOT ask the minute question again — the "
    "greeting already asked it. Go straight into the pitch. "
    "EPP is for employees of "
    "select partner companies — NOT only Bell employees. Never say someone "
    "has to work for Bell, and never ask if they are a Bell employee. To "
    "check eligibility, tell the person to visit cellular solution dot c a, "
    "click Employee Offers, and type in their "
    "company name — if their company comes up, they are eligible. Proof of "
    "employment is shown after ordering. "
    "Pronunciation: always say the website as 'cellular solution dot c a' — "
    "two words, cellular and solution. Never say 'w w w' or 'triple w'. "
    "Do not repeat the website directions in every reply. Say them once "
    "when the topic first comes up; after that, just offer the callback. "
    "Your job is to qualify interest in Bell mobility and EPP. "
    "Answer basic questions about EPP. NEVER invent or promise a specific "
    "price, plan detail, phone model price, or eligibility answer you cannot "
    "verify. If you do not know, say the store will confirm it. "
    "Eligibility: you do NOT have the list of partner companies. If the "
    "person names their company, NEVER say whether it is eligible or not. "
    "Say the website is the way to check, or offer a callback so the store "
    "can confirm it. "
    "Never close a sale and never take an order. "
    "When the person shows interest, offer a callback from the store right "
    "away and collect their name and phone number conversationally, one "
    "question at a time. After the pitch, the callback offer is always your "
    "next step — do not stall. End your pitch by offering the callback, "
    "not by asking whether their company offers EPP. "
    "Never repeat a phone number back until the caller has given all 10 "
    "digits. Callers often give the number in pieces, so wait patiently "
    "until you have the complete number. If the caller says they are not "
    "finished, ask them to continue giving the number and do not change "
    "the subject. "
    "Keep every reply short: 1 to 3 sentences, plain spoken language. "
    "Every reply must contain something useful — a fact, a question, or an "
    "offer. Never answer with a single word like 'Great' or 'Okay'. Ask "
    "complete questions only, never fragments like 'Your company name?'. "
    "Never end the call yourself — only say goodbye after the caller does. "
    "No markdown, no lists, no bullet points, no special characters."
)

SYSTEM_PROMPT = (
    "You are Cellular Solution's automated voice assistant, a Bell authorized "
    "dealer at 81 Zenway Blvd, Unit 6, Vaughan, Ontario. Store hours are "
    "Monday to Friday, 9am to 5pm. Store phone: 905-264-2228. "
    "You are an automated assistant, not a person. The greeting already "
    "introduced you as one, so never make the disclosure your whole reply — "
    "always combine it with something useful in the same reply. "
    "Your job is to qualify interest in Bell mobility and the Bell Employee "
    "Purchase Program, known as EPP: up to 30 percent off Bell mobility "
    "plans and no activation fees. EPP is for employees of select partner "
    "companies — NOT only Bell employees. Never say someone has to work for "
    "Bell, and never ask if they are a Bell employee. To check eligibility, "
    "tell the caller to visit cellular solution dot c a, click Employee "
    "Offers, and type in their company name — if their company comes up, "
    "they are eligible. Proof of employment is shown after ordering. "
    "Pronunciation: always say the website as 'cellular solution dot c a' — "
    "two words, cellular and solution. Never say 'w w w' or 'triple w'. "
    "Do not repeat the website directions in every reply. Say them once "
    "when the topic first comes up; after that, just offer the callback. "
    "Answer basic questions about EPP. NEVER invent or promise a specific "
    "price, plan detail, phone model price, or eligibility answer you cannot "
    "verify. If you do not know, say the store will confirm it. "
    "Eligibility: you do NOT have the list of partner companies. If the "
    "caller names their company, NEVER say whether it is eligible or not. "
    "Say the website is the way to check, or offer a callback so the store "
    "can confirm it. "
    "Never close a sale and never take an order. "
    "If the caller is interested, offer a callback from the store and collect "
    "their name and phone number conversationally, one question at a time. "
    "Never repeat a phone number back until the caller has given all 10 "
    "digits. Callers often give the number in pieces, so wait patiently "
    "until you have the complete number. If the caller says they are not "
    "finished, ask them to continue giving the number and do not change "
    "the subject. "
    "Keep every reply short: 1 to 3 sentences, plain spoken language. "
    "Every reply must contain something useful — a fact, a question, or an "
    "offer. Never answer with a single word like 'Great' or 'Okay'. Ask "
    "complete questions only, never fragments like 'Your company name?'. "
    "Never end the call yourself — only say goodbye after the caller does. "
    "No markdown, no lists, no bullet points, no special characters."
)

MOCK_REPLIES = {
    "price": (
        "I'm the automated assistant for Cellular Solution. I don't have live pricing "
        "here, so the store will confirm the exact price for you. Would you "
        "like a callback from the store?"
    ),
    "callback": (
        "I'm the automated assistant for Cellular Solution. Of course, I can arrange "
        "a callback from the store. Can you tell me your name and the best "
        "number to reach you at?"
    ),
    "default": (
        "I'm the automated assistant for Cellular Solution. I can help with Bell "
        "mobility and our Employee Purchase Program, which offers up to 30 "
        "percent off Bell mobility plans with no activation fees. What can I "
        "help you with today?"
    ),
}

# In-memory conversation history per callSid, so a dropped WebSocket can
# reconnect (applicationReconnect=true) and pick up where it left off.
SESSIONS: dict[str, list[dict]] = {}
MAX_SESSIONS = 100

app = FastAPI(title="cellular-solution-relay")


# ---------------------------------------------------------------------------
# Logging: every call turn is one JSON line on stdout.
# ---------------------------------------------------------------------------
def log_event(event: str, **fields) -> None:
    record = {
        "event": event,
        "ts": datetime.now(timezone.utc).isoformat(),
        **fields,
    }
    print(json.dumps(record), flush=True)


# ---------------------------------------------------------------------------
# Twilio request signature validation.
#
# X-Twilio-Signature is base64 HMAC-SHA1 of the full request URL followed by
# the sorted POST parameters, keyed with the account auth token. A WebSocket
# upgrade is a GET with no POST parameters, so the signed value is the URL
# alone. Behind Railway's proxy the app sees an internal http URL, so the
# public wss:// URL is rebuilt from X-Forwarded-* headers.
# ---------------------------------------------------------------------------
def twilio_signature_valid(
    url: str, params: dict[str, str], signature: str, token: str
) -> bool:
    data = url + "".join(k + v for k, v in sorted(params.items()))
    digest = hmac.new(token.encode("utf-8"), data.encode("utf-8"), hashlib.sha1).digest()
    expected = base64.b64encode(digest).decode("ascii")
    return hmac.compare_digest(expected, signature)


def public_ws_url(websocket: WebSocket) -> str:
    headers = websocket.headers
    host = headers.get("x-forwarded-host") or headers.get("host", "")
    proto = headers.get("x-forwarded-proto", "")
    scheme = "wss" if proto in ("https", "wss") else "ws"
    path = websocket.url.path
    query = websocket.url.query
    url = f"{scheme}://{host}{path}"
    if query:
        url = f"{url}?{query}"
    return url


def public_http_url(request: Request) -> str:
    headers = request.headers
    host = headers.get("x-forwarded-host") or headers.get("host", "")
    proto = headers.get("x-forwarded-proto", "http")
    scheme = "https" if proto in ("https", "wss") else "http"
    url = f"{scheme}://{host}{request.url.path}"
    if request.url.query:
        url = f"{url}?{request.url.query}"
    return url


# ---------------------------------------------------------------------------
# Callback capture (best effort, V1).
# ---------------------------------------------------------------------------
PHONE_RE = re.compile(
    r"(?:\+?1[\s.\-]?)?(?:\(?(\d{3})\)?[\s.\-]?)(\d{3})[\s.\-]?(\d{4})"
)
# Words that look like a name to a naive regex but never are one. STT
# transcripts are often all-lowercase, so the name patterns below match
# case-insensitively and these stop-words do the disambiguation instead
# (e.g. "my name is Don and my number is..." -> "Don", not "Don and";
# "I'm looking for a deal" -> no name, not "looking").
NAME_STOPWORDS = {
    "and", "my", "the", "is", "a", "an", "so", "please", "can", "could",
    "looking", "calling", "interested", "hoping", "trying", "wondering",
    "just", "here", "not", "glad", "happy", "sorry", "afraid", "new",
    "from", "with", "regarding", "about", "concerning", "for", "to",
    "me", "i", "we", "it", "this", "that",
}
NAME_RES = [
    re.compile(r"\bmy name is ([a-z]+(?: [a-z]+)?)", re.IGNORECASE),
    re.compile(r"\bthis is ([a-z]+(?: [a-z]+)?)", re.IGNORECASE),
    re.compile(r"\bI'?m ([a-z]+(?: [a-z]+)?)", re.IGNORECASE),
    re.compile(r"\bname'?s ([a-z]+(?: [a-z]+)?)", re.IGNORECASE),
]


def extract_phone(text: str) -> str | None:
    match = PHONE_RE.search(text)
    if not match:
        return None
    digits = "".join(match.groups())
    if len(digits) == 10:
        return f"+1{digits}"
    return None


def extract_name(text: str) -> str | None:
    for pattern in NAME_RES:
        match = pattern.search(text)
        if not match:
            continue
        words = match.group(1).split()
        # Drop trailing stop-words ("Don and" -> "Don").
        while words and words[-1].lower() in NAME_STOPWORDS:
            words.pop()
        if not words:
            continue
        # A stop-word first means it was never a name ("I'm looking").
        if words[0].lower() in NAME_STOPWORDS:
            continue
        return " ".join(w.capitalize() for w in words[:2])
    return None


# ---------------------------------------------------------------------------
# Mistral streaming.
# ---------------------------------------------------------------------------
def _content_to_text(content) -> str:
    """Normalize a Mistral delta's content to plain text.

    The API usually returns a plain string, but some models (notably GLM
    served through Mistral) return a list of content blocks instead, e.g.
    [{"type": "text", "text": "..."}]. Passing a list downstream crashes
    the token accumulator ("can only concatenate str (not list) to str"),
    so both shapes are flattened here.
    """
    if not content:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, dict):
                if "text" in block:
                    parts.append(str(block["text"]))
            elif isinstance(block, str):
                parts.append(block)
        return "".join(parts)
    return str(content)


def _speakable_text(text: str) -> str:
    """Rewrite assistant text into exactly what the voice engine should say.

    The model sometimes writes the website as 'cellularsolution.ca,' and
    the voice engine then reads the comma out loud ("dot c a comma") instead
    of pausing at it. Prompt instructions alone don't prevent that — the
    model ignored them once already — so the spoken text is fixed here,
    deterministically, before it reaches Twilio: the URL always becomes
    'cellular solution dot c a' (two words — the voice engine mangles the
    one-word 'cellularsolution'), and the comma after it becomes 'and'.
    """
    spoken = re.sub(
        r"(?:www\.)?cellularsolution\.ca",
        "cellular solution dot c a",
        text,
        flags=re.IGNORECASE,
    )
    # The model sometimes writes the one-word spoken form itself
    # ("cellularsolution dot c a") — split it into two words.
    spoken = re.sub(
        r"cellularsolution",
        "cellular solution",
        spoken,
        flags=re.IGNORECASE,
    )
    # A comma right after the spelled-out site gets read as "comma" by the
    # voice engine, so it has to go — but dropping it leaves the site and
    # the next words run together ("dot c a click"), which sounds wrong.
    # Replace it with "and": "cellular solution dot c a and click...".
    spoken = re.sub(
        r"cellular solution dot c a,\s*(and\s+)?",
        "cellular solution dot c a and ",
        spoken,
    )
    return spoken


def _split_holdback(text: str, hold_back: int = 40) -> tuple[str, str]:
    """Split streamed text into (safe to emit, hold back).

    The emit part ends `hold_back` characters behind the frontier, so a
    website address (at most 25 characters) plus any comma after it always
    stays whole inside the held-back tail until it is complete. A split is
    never made inside the address: if it starts before the split point,
    the split moves back to its start (including a preceding 'www.').
    """
    split = max(0, len(text) - hold_back)
    low = text.lower()
    m = None
    for m in re.finditer(r"cellular\s?solution", low):
        pass  # keep the last match
    if m is not None:
        start = m.start()
        if low[max(0, start - 4):start] == "www.":
            start -= 4
        if start < split:
            split = start
    return text[:split], text[split:]


async def mistral_stream_reply(
    history: list[dict],
) -> "asyncio.AsyncGenerator[str, None]":
    """Yield reply tokens from Mistral as they arrive (SSE stream)."""
    if MOCK_LLM:
        last_user = next(
            (m["content"] for m in reversed(history) if m["role"] == "user"),
            "",
        )
        lowered = last_user.lower()
        if "callback" in lowered or "call me back" in lowered:
            reply = MOCK_REPLIES["callback"]
        elif "price" in lowered or "cost" in lowered or "much" in lowered:
            reply = MOCK_REPLIES["price"]
        else:
            reply = MOCK_REPLIES["default"]
        # Yield word by word so the streaming path is exercised.
        for word in reply.split(" "):
            yield word + " "
            await asyncio.sleep(0.01)
        return

    if not MISTRAL_API_KEY:
        raise RuntimeError("MISTRAL_API_KEY is not set")

    payload = {
        "model": MISTRAL_MODEL,
        "messages": history,
        "temperature": 0.5,
        # mistral-small-latest does not emit "thinking" blocks, so 300 tokens
        # of plain text is plenty for a 1-3 sentence spoken reply.
        # (mistral-large-4 needed 1000+ because its thinking alone could eat
        # the whole budget before any answer text appeared.)
        "max_tokens": 300,
        "stream": True,
    }
    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
    }
    async with httpx.AsyncClient(timeout=60.0) as client:
        async with client.stream(
            "POST", MISTRAL_URL, json=payload, headers=headers
        ) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if data == "[DONE]":
                    break
                try:
                    chunk = json.loads(data)
                except json.JSONDecodeError:
                    continue
                delta = chunk.get("choices", [{}])[0].get("delta", {})
                text = _content_to_text(delta.get("content"))
                if text:
                    yield text


# ---------------------------------------------------------------------------
# HTTP endpoints.
# ---------------------------------------------------------------------------
@app.get("/")
async def root() -> dict:
    return {"status": "ok", "service": "cellular-solution-relay"}


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


def conversation_relay_xml(ws_url: str, greeting: str) -> str:
    """Build the <Connect><ConversationRelay> TwiML for one greeting."""
    # ttsProvider/voice are only included when explicitly configured:
    # Twilio rejected every explicit Google voice tried for en-CA
    # (en-CA-Standard-A and Google.en-CA-Neural2-A both → error 64101),
    # so the voice now falls back to Twilio's account defaults.
    tts_attrs = ""
    if RELAY_TTS_PROVIDER:
        tts_attrs += f'ttsProvider="{RELAY_TTS_PROVIDER}" '
    if RELAY_TTS_VOICE:
        tts_attrs += f'voice="{RELAY_TTS_VOICE}" '
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<Response><Connect>"
        f'<ConversationRelay url="{ws_url}" '
        f'welcomeGreeting="{escape_attr(greeting)}" '
        f'language="{RELAY_LANGUAGE}" '
        f"{tts_attrs}"
        f'transcriptionProvider="{RELAY_STT_PROVIDER}" '
        'speechModel="telephony" '
        'interruptible="any" '
        'applicationReconnect="true" '
        f'hints="{escape_attr(_env_or("RELAY_HINTS", DEFAULT_HINTS))}" />'
        "</Connect></Response>"
    )


async def _twiml_response(request: Request, greeting: str) -> Response:
    if request.method == "POST" and TWILIO_AUTH_TOKEN:
        body = await request.body()
        params = dict(parse_qsl(body.decode("utf-8", "replace")))
        signature = request.headers.get("x-twilio-signature", "")
        url = public_http_url(request)
        if not signature or not twilio_signature_valid(
            url, params, signature, TWILIO_AUTH_TOKEN
        ):
            log_event("twiml_signature_rejected", url=url)
            return Response(content="Forbidden", status_code=403)
    elif request.method == "GET":
        log_event("twiml_unsigned_get", note="test fetch only; Twilio uses POST")

    host = request.headers.get("x-forwarded-host") or request.headers.get("host", "")
    ws_url = RELAY_WS_URL or f"wss://{host}/ws"
    return Response(
        content=conversation_relay_xml(ws_url, greeting),
        media_type="application/xml",
    )


@app.api_route("/twiml", methods=["GET", "POST"])
async def twiml(request: Request) -> Response:
    """TwiML for the Twilio number: connect the call to ConversationRelay."""
    return await _twiml_response(request, _env_or("RELAY_WELCOME_GREETING", WELCOME_GREETING))


@app.api_route("/twiml-outbound", methods=["GET", "POST"])
async def twiml_outbound(request: Request) -> Response:
    """TwiML for outbound dials: same relay, but the greeting introduces
    the call instead of thanking the person for calling."""
    return await _twiml_response(request, _env_or("RELAY_OUTBOUND_GREETING", OUTBOUND_GREETING))


# ---------------------------------------------------------------------------
# WebSocket: ConversationRelay session.
# ---------------------------------------------------------------------------
@app.websocket("/ws")
async def relay_ws(websocket: WebSocket) -> None:
    if TWILIO_AUTH_TOKEN:
        signature = websocket.headers.get("x-twilio-signature", "")
        url = public_ws_url(websocket)
        if not signature or not twilio_signature_valid(
            url, {}, signature, TWILIO_AUTH_TOKEN
        ):
            log_event("ws_signature_rejected", url=url)
            await websocket.close(code=1008)
            return
    else:
        logger.warning(
            "TWILIO_AUTH_TOKEN is not set: WebSocket signature validation is OFF. "
            "Anyone who knows the wss URL can open a session."
        )

    await websocket.accept()

    call_sid = ""
    history: list[dict] = [{"role": "system", "content": _env_or("RELAY_SYSTEM_PROMPT", SYSTEM_PROMPT)}]
    callback_name: str | None = None
    callback_phone: str | None = None
    callback_logged = False
    # Piecemeal phone dictation: callers often give the number in chunks
    # ("289." ... "901." ... "0990"). Digit groups of 3+ accumulate here;
    # once 10 digits are banked, the number is captured (last 10, so a
    # restarted dictation wins over the abandoned prefix).
    phone_buffer = ""
    # Set when the assistant just asked for the caller's name: the next
    # short reply ("Jonathan.") IS the name, not a sentence to parse.
    expecting_name = False
    # Set once the assistant has offered a callback in this call: a bare
    # first name volunteered after that point is a name, even if the LLM
    # asked a different question that turn.
    callback_offered = False
    llm_task: asyncio.Task | None = None

    async def send_text_tokens(reply_stream) -> str:
        """Stream reply tokens to Twilio with a short holdback, normalizing
        each chunk for TTS before it is spoken; return the full reply text
        (raw, for history/logs).

        The holdback is 40 characters: the website ('cellularsolution.ca',
        'www.cellularsolution.ca', 'cellular solution dot c a') is at most
        25 characters, so it always fits inside the held-back tail together
        with any comma after it, and _speakable_text rewrites it before
        Twilio ever sees it — the voice engine can no longer read a comma
        out loud. A chunk is never split inside the website address: if
        the address starts before the split point, the split moves back to
        its start. A comma left at the start of a chunk (the address ended
        the previous chunk) becomes "and" instead of being dropped, so the
        site and the next words don't run together.

        History: pure token streaming let 'cellularsolution.ca,' through
        and the voice engine said "comma"; full buffering fixed that but
        made every turn start with dead air, which the user called out on
        the 103s test call. The holdback is the middle ground: speech
        starts ~0.3s after the first tokens arrive instead of after the
        whole reply is generated.
        """
        HOLD_BACK = 40
        full = ""
        pending = ""
        prev_ended_with_site = False

        def _split_pending(text: str) -> tuple[str, str]:
            """Split text into (safe to emit, hold back)."""
            return _split_holdback(text, HOLD_BACK)

        async def _flush(text: str) -> None:
            nonlocal prev_ended_with_site
            if not text:
                return
            spoken = _speakable_text(text)
            if prev_ended_with_site:
                spoken = spoken.lstrip()
                if spoken.startswith(","):
                    # The comma belongs to the site phrase; the voice engine
                    # would read it as "comma", so it becomes "and" instead.
                    rest = spoken[1:].lstrip()
                    if rest and not rest.lower().startswith("and "):
                        spoken = "and " + rest
                    else:
                        spoken = rest
            prev_ended_with_site = spoken.rstrip().endswith(
                "cellular solution dot c a"
            )
            if spoken:
                await websocket.send_json(
                    {
                        "type": "text",
                        "token": spoken,
                        "last": False,
                        "interruptible": True,
                        "preemptible": False,
                    }
                )

        try:
            async for token in reply_stream:
                if not token:
                    continue
                full += token
                pending += token
                if len(pending) > HOLD_BACK:
                    emit, pending = _split_pending(pending)
                    await _flush(emit)
        except asyncio.CancelledError:
            raise
        await _flush(pending)
        await websocket.send_json(
            {
                "type": "text",
                "token": "",
                "last": True,
                "interruptible": True,
                "preemptible": False,
            }
        )
        return full

    try:
        while True:
            message = await websocket.receive_json()
            msg_type = message.get("type")

            if msg_type == "setup":
                call_sid = message.get("callSid", "")
                connection_type = message.get("connectionType", "initial")
                if connection_type == "reconnect" and call_sid in SESSIONS:
                    history = SESSIONS[call_sid]
                    log_event(
                        "session_reconnected",
                        callSid=call_sid,
                        turns=len(history),
                    )
                else:
                    # Outbound dials get the outbound system prompt: the
                    # agent called the person, so it must introduce the
                    # call's purpose instead of asking "how can I help you".
                    if str(message.get("direction", "")).startswith("outbound"):
                        history[0] = {
                            "role": "system",
                            "content": _env_or("RELAY_OUTBOUND_SYSTEM_PROMPT", OUTBOUND_SYSTEM_PROMPT),
                        }
                        greeting = _env_or("RELAY_OUTBOUND_GREETING", OUTBOUND_GREETING)
                    else:
                        greeting = _env_or("RELAY_WELCOME_GREETING", WELCOME_GREETING)
                    # The greeting is spoken by Twilio before the LLM is
                    # involved, so the model never sees it — and then it
                    # repeats the minute question / EPP intro ("Do you have
                    # a minute to learn about EPP?") right after the caller
                    # already said yes. Seed it as the first assistant turn
                    # so the model knows what was already said.
                    history.append({"role": "assistant", "content": greeting})
                    SESSIONS[call_sid] = history
                    if len(SESSIONS) > MAX_SESSIONS:
                        SESSIONS.pop(next(iter(SESSIONS)))
                log_event(
                    "session_start",
                    callSid=call_sid,
                    from_=message.get("from"),
                    to=message.get("to"),
                    direction=message.get("direction"),
                    connectionType=connection_type,
                )

            elif msg_type == "prompt":
                if not message.get("last", True):
                    continue  # partial prompt; wait for the final one
                user_text = (message.get("voicePrompt") or "").strip()
                if not user_text:
                    continue
                log_event("turn", callSid=call_sid, role="user", text=user_text)
                history.append({"role": "user", "content": user_text})

                # Bare-name reply: the assistant just asked for the name and
                # the caller answered with the name alone ("Jonathan."), or
                # the caller volunteered a bare first name after a callback
                # was offered. A bare name is a single word; two words are
                # only accepted right after an explicit name question.
                words = user_text.strip().strip(".,!?").split()
                fillers = NAME_STOPWORDS | {
                    "uh", "um", "yeah", "yes", "no", "ok", "okay",
                    "sure", "hi", "hello", "thanks", "thank",
                }
                bare_name_ok = False
                if expecting_name:
                    expecting_name = False
                    bare_name_ok = 1 <= len(words) <= 2
                elif callback_offered and not callback_name:
                    bare_name_ok = len(words) == 1
                if (
                    bare_name_ok
                    and words
                    and all(w.isalpha() for w in words)
                    and words[0].lower() not in fillers
                ):
                    callback_name = " ".join(
                        w.capitalize() for w in words[:2]
                    )

                phone_newly_captured = False
                phone = extract_phone(user_text)
                if phone:
                    if not callback_phone:
                        phone_newly_captured = True
                    callback_phone = phone
                    phone_buffer = ""
                else:
                    # Bank digit groups of 3+ across turns ("289." ...
                    # "901." ... "0990."). Groups under 3 digits ("3 pm",
                    # "30 percent") are ignored, and the last 10 banked
                    # digits win, so a stray number mentioned earlier
                    # ("I pay 129") can't corrupt the real number.
                    for group in re.findall(r"\d+", user_text):
                        if len(group) >= 3:
                            phone_buffer += group
                    if len(phone_buffer) >= 10:
                        if not callback_phone:
                            phone_newly_captured = True
                        callback_phone = "+1" + phone_buffer[-10:]
                        phone_buffer = ""
                name = extract_name(user_text)
                if name:
                    callback_name = name

                # Log the callback as soon as both halves are known, instead
                # of waiting for a later assistant reply that happens to
                # contain the word "callback".
                if (
                    not callback_logged
                    and callback_phone
                    and callback_name
                ):
                    callback_logged = True
                    log_event(
                        "CALLBACK_REQUEST",
                        callSid=call_sid,
                        name=callback_name,
                        phone=callback_phone,
                    )

                # Tell the LLM the number is complete: it can't reliably
                # count digits across piecemeal turns, and without this it
                # asks for "the last three digits" after already having
                # all ten.
                if phone_newly_captured:
                    history.append(
                        {
                            "role": "system",
                            "content": (
                                "The caller has now given their complete "
                                "10-digit phone number. Confirm the full "
                                "number in your reply and do not ask for "
                                "more digits."
                            ),
                        }
                    )

                try:
                    llm_task = asyncio.create_task(
                        send_text_tokens(mistral_stream_reply(history))
                    )
                    reply = await llm_task
                except asyncio.CancelledError:
                    log_event("reply_cancelled", callSid=call_sid)
                    continue
                except Exception as exc:  # noqa: BLE001 - keep the call alive
                    logger.exception("LLM error: %s", exc)
                    log_event("llm_error", callSid=call_sid, error=str(exc))
                    reply = (
                        "Sorry, I'm having trouble right now. "
                        "The store can help you at 905-264-2228."
                    )
                    try:
                        await websocket.send_json(
                            {
                                "type": "text",
                                "token": reply,
                                "last": True,
                                "interruptible": True,
                                "preemptible": False,
                            }
                        )
                    except (WebSocketDisconnect, RuntimeError):
                        # The caller already hung up; nothing to say it to.
                        pass
                finally:
                    llm_task = None

                if reply:
                    history.append({"role": "assistant", "content": reply.strip()})
                    log_event(
                        "turn", callSid=call_sid, role="assistant", text=reply.strip()
                    )
                    # The next short reply is likely just the caller's name.
                    lowered_reply = reply.lower()
                    if "callback" in lowered_reply or "call you back" in lowered_reply:
                        callback_offered = True
                    if any(
                        phrase in lowered_reply
                        for phrase in (
                            "first name",
                            "your name",
                            "what's your name",
                            "what is your name",
                            "may i have your name",
                            "can i have your name",
                        )
                    ):
                        expecting_name = True
                    # Backstop: the LLM often echoes the name itself
                    # ("Thank you Jonathan.") even when the caller's bare
                    # reply didn't match the name patterns. A capitalised
                    # word right after "thank you/thanks" is the name.
                    if not callback_name:
                        name_match = re.search(
                            r"(?i:thank you|thanks),?\s+([A-Z][a-z]+)", reply
                        )
                        if name_match:
                            candidate = name_match.group(1)
                            if candidate.lower() not in NAME_STOPWORDS:
                                callback_name = candidate

            elif msg_type == "interrupt":
                if llm_task is not None and not llm_task.done():
                    llm_task.cancel()
                log_event(
                    "interrupt",
                    callSid=call_sid,
                    utteranceUntilInterrupt=message.get("utteranceUntilInterrupt"),
                )

            elif msg_type == "dtmf":
                log_event("dtmf", callSid=call_sid, digit=message.get("digit"))

            elif msg_type == "info":
                log_event(
                    "info",
                    callSid=call_sid,
                    name=message.get("name"),
                    value=message.get("value"),
                )

            elif msg_type == "error":
                log_event(
                    "relay_error",
                    callSid=call_sid,
                    description=message.get("description"),
                )

            else:
                log_event(
                    "unknown_message",
                    callSid=call_sid,
                    type=msg_type,
                )

    except WebSocketDisconnect:
        log_event("session_end", callSid=call_sid)
    except Exception as exc:  # noqa: BLE001 - log and close cleanly
        logger.exception("WebSocket error: %s", exc)
        log_event("session_error", callSid=call_sid, error=str(exc))
        try:
            await websocket.close(code=1011)
        except Exception:  # noqa: BLE001, S110 - socket may already be gone
            pass


if __name__ == "__main__":
    import uvicorn

    if not MISTRAL_API_KEY and not MOCK_LLM:
        logger.warning(
            "MISTRAL_API_KEY is not set and MOCK_LLM is off: "
            "LLM calls will fail until a key is provided."
        )
    if not TWILIO_AUTH_TOKEN:
        logger.warning(
            "TWILIO_AUTH_TOKEN is not set: signature validation is OFF."
        )
    uvicorn.run(app, host="0.0.0.0", port=PORT)
