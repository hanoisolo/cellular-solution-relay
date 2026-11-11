# Cellular Solution — ConversationRelay voice agent

Live LLM phone agent for Cellular Solution (+1 437 475 4742). Twilio
ConversationRelay handles the phone call, speech-to-text and text-to-speech;
this server is the brain: it takes the caller's transcribed speech, asks
Mistral (`mistral-large-4`) for a reply, and streams the reply back as text
tokens for Twilio to speak.

## How a call flows

```
Caller -> Twilio number -> POST /twiml (this server)
  -> <Connect><ConversationRelay url="wss://<host>/ws">
  -> WebSocket: setup -> prompt -> Mistral -> text tokens -> spoken reply
```

## Files

| File | What it is |
| --- | --- |
| `server.py` | FastAPI app: `GET /`, `GET /health`, `GET|POST /twiml`, `GET|POST /twiml-outbound`, `POST /sms`, `WS /ws` |
| `requirements.txt` | Pinned dependencies (verified on Python 3.12) |
| `test_ws.py` | Local WebSocket test (fake setup + prompt, no Twilio/Mistral) |
| `test_sms.py` | Headless route & contract parity tests (in-process TestClient, no port) |
| `.gitignore` | Excludes `.venv/` and `.env` — never commit secrets |

## Run locally

```bash
cd relay-server
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# Test mode (no Mistral key needed, canned replies):
MOCK_LLM=1 .venv/bin/python server.py

# Live mode:
MISTRAL_API_KEY=<your key> .venv/bin/python server.py
```

Server listens on `PORT` (default 8000). Then, in another terminal:

```bash
.venv/bin/python test_ws.py          # WS flow against the MOCK_LLM server
MOCK_LLM=1 .venv/bin/python test_sms.py   # HTTP route-parity suite, no server needed
```

`test_ws.py` opens a WebSocket, sends a fake `setup` + `prompt` (a callback
request with a name and phone number), prints every message the server
returns, and asserts a `text` token with `last=true` arrives. It also
exercises the interrupt path and checks the Twilio signature validator
against an independently computed HMAC.

`test_sms.py` uses FastAPI's in-process `TestClient` (no port, no network)
and asserts route & contract parity for every registered route: status
codes, response shapes, 405 on wrong methods, 403 on missing/bad Twilio
signatures, the `/sms` `<Message>` reply, XML escaping, and per-sender SMS
history.

Check the TwiML endpoint too:

```bash
curl -s http://localhost:8000/twiml
curl -s http://localhost:8000/health
```

## Environment variables

| Variable | Required | What it does |
| --- | --- | --- |
| `MISTRAL_API_KEY` | Yes (unless `MOCK_LLM=1`) | Mistral API key. Read only from the env var; never put it in a file. |
| `MISTRAL_MODEL` | No | Default `mistral-large-4`. |
| `PORT` | No | Railway sets this automatically. Default `8000` locally. |
| `RELAY_WS_URL` | No | Public `wss://` URL of this server. If unset, `/twiml` builds `wss://<request host>/ws`. Set it explicitly on Railway. |
| `TWILIO_AUTH_TOKEN` | No, but **set it in production** | Enables `X-Twilio-Signature` validation on `/twiml`, `/twiml-outbound`, `/sms` (POST) and the `/ws` handshake. Without it the server logs a warning and validation is off. Read per request. |
| `MOCK_LLM` | No | `1` = answer with canned replies, no Mistral call. For testing only. |
| `RELAY_SMS_SYSTEM_PROMPT` | No | SMS system prompt sent to Mistral for `POST /sms`. Default `SMS_SYSTEM_PROMPT`. |
| `TWILIO_ACCOUNT_SID` | Only for outbound SMS | Account SID used by `send_sms()` (callback alerts). |
| `TWILIO_API_KEY_SID` / `TWILIO_API_KEY_SECRET` | Alt to auth token | API-key Basic auth for `send_sms()` (takes precedence over `TWILIO_AUTH_TOKEN`). |
| `TWILIO_FROM` | Only for outbound SMS | Sender number for `send_sms()` (e.g. +14374754742). |
| `STORE_SMS_NUMBER` | No | When set, every captured `CALLBACK_REQUEST` also texts the store: "Callback request: name phone (call sid)". Unset = logs-only, as before. |
| `RELAY_LANGUAGE` | No | Default `en-CA`. TTS/STT language. |
| `RELAY_TTS_PROVIDER` | No | Default `Google`. |
| `RELAY_TTS_VOICE` | No | Default `en-CA-Standard-A`. |
| `RELAY_STT_PROVIDER` | No | Default `Google`, `speechModel=telephony`. |

## Behaviour (baked into the system prompt in `server.py`)

- First thing on every call: the agent says it is an AI assistant, not a
  person (also in the `welcomeGreeting` Twilio plays before the LLM speaks).
- Qualifies Bell mobility / Employee Purchase Program interest (up to 30%
  off Bell mobility, no activation fees, proof of employment after
  ordering).
- **Never** invents or promises a specific price, plan detail, or
  eligibility answer — it says the store will confirm it.
- Never closes a sale. When the caller is interested, it offers a callback
  and collects name + phone number conversationally.
- Replies are short (1–3 sentences), plain spoken language, no markdown.
- Every call turn is logged to stdout as one JSON line. When a callback is
  captured, a structured `CALLBACK_REQUEST` line is logged with `callSid`,
  `name`, `phone`, `timestamp`; if `STORE_SMS_NUMBER` is set, the store also
  gets an SMS at that moment.

## How an SMS flows

```
Texter -> +1 437 475 4742 -> POST /sms (this server, signature-checked)
  -> Mistral (SMS_SYSTEM_PROMPT, per-sender history) -> <Response><Message>
```

- Replies are plain text: the website stays `cellularsolution.ca` verbatim
  (the voice-only "dot c a" rewrite in `_speakable_text` is never applied
  to SMS). Replies are 1–3 sentences, no markdown.
- Per-sender history is kept in memory (`SMS_SESSIONS`, capped) so follow-up
  texts have context; it resets on redeploy.
- Outbound SMS from the agent is env-gated (`send_sms`): used for the
  callback alert above. For ad-hoc sends use `demo/sms_tool.py send`.
- Compliance note: a Canadian +1 local number texting Canadian numbers does
  not need A2P/10DLC registration. Texting **US** numbers as A2P does — register
  the brand/campaign first if that is ever needed.

## Deploy to Railway (dashboard steps — do these by hand)

The Railway connector has no verified write operations, so deployment is a
dashboard job:

1. Push this folder to a Git repo (the Discord bot's `diligent-illumination`
   project already deploys from GitHub — use the same approach).
2. In Railway, open project **diligent-illumination** → **New Service** →
   **GitHub Repo** → pick the repo. Set the **Root Directory** to the
   folder containing `server.py` (e.g. `relay-server`).
3. Railway runs `pip install -r requirements.txt` and starts the service.
   If it asks for a start command, use:
   `uvicorn server:app --host 0.0.0.0 --port $PORT`
4. In the service's **Variables** tab, set:
   - `MISTRAL_API_KEY` = your Mistral key
   - `TWILIO_AUTH_TOKEN` = your Twilio auth token (signature validation)
   - `RELAY_WS_URL` = `wss://<your Railway public domain>/ws`
   - Do **not** set `MOCK_LLM` in production.
5. In **Settings → Networking**, generate a public domain if the service
   doesn't have one yet. Copy it for the next steps.
6. **Twilio console prerequisites** (one-time):
   - Accept the Predictive and Generative AI/ML Features Addendum
     (Console → Voice → Settings → Privacy & Security). ConversationRelay
     won't run until this is accepted.
   - Trust Hub: the primary compliance profile must be **approved** —
     outbound calls are blocked until then (inbound already works).
7. **Cut over the number** (Twilio Console → Phone Numbers →
   +1 437 475 4742 → Voice Configuration): change the incoming-call
   handler from the Welcome TwiML Bin to **Webhook**,
   `https://<your Railway public domain>/twiml`, HTTP POST. The fixed
   menu bins stay untouched, so you can switch back any time.
   Do the same for **Messaging Configuration**: A MESSAGE COMES IN →
   Webhook `https://<your Railway public domain>/sms`, HTTP POST
   (or run `python demo/sms_tool.py configure` from the demo kit).
8. Test: call +1 437 475 4742 from your mobile and talk to Mistral;
   text the same number and get an AI SMS reply.

## Costs (per minute, roughly)

- ConversationRelay $0.07 + inbound voice $0.0085 + Mistral tokens
  (mistral-large-4, short replies) — about 25¢ for a 3-minute call.

## Troubleshooting

- **Call fails immediately / 64102 (unable to connect to WebSocket):**
  the `wss://` URL is wrong or the Railway service is down. Check the
  service logs and that `RELAY_WS_URL` matches the public domain.
- **64106 (invalid argument):** the language/voice combo isn't supported
  by the chosen provider. Set `RELAY_LANGUAGE=en-US` (and
  `RELAY_TTS_VOICE=en-US-Standard-A` if needed) and redeploy.
- **403 from /twiml:** signature validation failed. Behind Railway's
  proxy the server rebuilds the public URL from `X-Forwarded-Host` /
  `X-Forwarded-Proto`; if Railway ever changes those headers, validation
  breaks — check the `twiml_signature_rejected` log line for the URL it
  computed. As a stopgap, unset `TWILIO_AUTH_TOKEN` (validation off, with
  a loud warning in the logs).
- **LLM errors on a live call:** the caller hears a fallback line
  ("Sorry, I'm having trouble right now...") and the `llm_error` line in
  the logs has the detail. Check `MISTRAL_API_KEY` first.
- **No reply, call just sits there:** Twilio only sends a `prompt` after
  the caller speaks. Silence handling is a known V1 gap — the server
  doesn't nudge silent callers yet.

## What this is not (yet)

- No email on callback capture (SMS alert only, if `STORE_SMS_NUMBER` is set;
  otherwise logs only).
- No transfer to a human (no `<Dial>` handoff yet — the `end` message with
  `handoffData` is supported by the protocol but not wired to a transfer).
- No Net2Phone integration (paused at the user's request).

## Changing the agent's wording without touching code

All spoken/prompt wording is overridable via environment variables (read per
request, so set them on Railway and the change applies on the next request or
redeploy — no code edit needed):

| Env var | Overrides | Default |
| --- | --- | --- |
| `RELAY_WELCOME_GREETING` | inbound `welcomeGreeting` (`/twiml`) | `WELCOME_GREETING` in server.py |
| `RELAY_OUTBOUND_GREETING` | outbound `welcomeGreeting` (`/twiml-outbound`) | `OUTBOUND_GREETING` |
| `RELAY_SYSTEM_PROMPT` | inbound system prompt sent to Mistral | `SYSTEM_PROMPT` |
| `RELAY_OUTBOUND_SYSTEM_PROMPT` | outbound system prompt (ws setup `direction` starts with "outbound") | `OUTBOUND_SYSTEM_PROMPT` |
| `RELAY_SMS_SYSTEM_PROMPT` | SMS system prompt (`POST /sms`) | `SMS_SYSTEM_PROMPT` |
| `RELAY_HINTS` | ConversationRelay `hints` attribute (both legs) | `DEFAULT_HINTS` |

Set them in the Railway dashboard (service → Variables) or via the API
(`variableUpsert`). Empty/unset falls back to the compiled-in default, so the
defaults above are always a safe rollback. Attribute values are XML-escaped
(`escape_attr`) before being written into the TwiML.
