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
| `RELAY_SYSTEM_PROMPT` | No | Inbound voice system prompt sent to Mistral. Default `SYSTEM_PROMPT`. |
| `RELAY_OUTBOUND_SYSTEM_PROMPT` | No | Outbound voice system prompt. Default `OUTBOUND_SYSTEM_PROMPT`. |
| `RELAY_OUTBOUND_SYSTEM_PROMPT_AFTER_PITCH` | No | Outbound system prompt swapped in once the EPP pitch has been spoken. Carries no pitch script and no pitch trigger. Default `OUTBOUND_SYSTEM_PROMPT_AFTER_PITCH`. |
| `RELAY_WELCOME_GREETING` | No | Greeting spoken as a non-interruptible token on inbound setup. Default `WELCOME_GREETING`. |
| `RELAY_OUTBOUND_GREETING` | No | Greeting spoken on outbound setup. Default `OUTBOUND_GREETING`. |
| `RELAY_DISCLOSURE` | No | Inbound AI + recording disclosure token. Default `DISCLOSURE`. |
| `RELAY_RECORDING_DISCLOSURE` | No | Outbound recording notice (spoken after a positive answer to the greeting). Default `RECORDING_DISCLOSURE`. |
| `RELAY_INTERRUPT_ACK` | No | Pipe-separated acknowledgements spoken when the caller talks over the agent, rotated per interruption. Default `INTERRUPT_ACKS` (`Sure.\|Go ahead.\|Of course.`). Set empty to disable. |
| `RELAY_HINTS` | No | ConversationRelay STT `hints` attribute. Default `DEFAULT_HINTS`. |
| `RELAY_NUDGE_AFTER` | No | Seconds of LLM silence before the silence nudge fires. Default `3`. |
| `RELAY_NUDGE_TEXT` | No | Silence nudge text. Default `"One moment, ..."`. |
| `RELAY_IDLE_AFTER` | No | Seconds of caller silence before the idle prompt fires, once the caller has spoken. Default `8`. `0` disables. |
| `RELAY_IDLE_TEXT` | No | Idle prompt text. Default `"Are you still there?"`. |
| `RELAY_IDLE_TEXT_2` | No | Second idle prompt, spoken if the first drew no answer. Default `"Take your time. I'm here whenever you're ready."`. |
| `RELAY_STORE_NUMBER` | No | Store number dialed by `/handoff`. Default `+19052642228`. |
| `RELAY_HANDOFF_DIGIT_PAUSE` | No | DTMF lead-in pause before the extension digits, in half-seconds (Twilio `w`). Default `10` = five seconds, clamped 0-20. Raise it if a handoff lands on the store's main line instead of the extension. |
| `RELAY_EPP_HARDWARE_OFFERS` | No | Verified Bell EPP hardware offers block appended to every system prompt (see "EPP offers blocks" below). Default empty = no block. |
| `RELAY_EPP_PLAN_OFFERS` | No | Verified Bell EPP rate plan offers block, same mechanism. Default empty; set when the rate plan grid is loaded. |

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
- No Net2Phone integration (paused at the user's request).

## Call routing (inbound handoff)

When an inbound caller asks for a person, or for one of the destinations below,
the relay ends the session with the destination in `handoffData` and `/handoff`
dials `RELAY_STORE_NUMBER` with the extension as DTMF
(`<Number sendDigits="wwwwwwwwww225">`). The `w` characters are a lead-in pause
of half a second each — five seconds by default — so the store's auto-attendant
can finish its greeting before it takes digits. A pause that is too short gets
the digits swallowed and the caller lands on the main line instead of the
extension; `RELAY_HANDOFF_DIGIT_PAUSE` tunes the length without a code change.

| Caller asks for | Extension | Spoken line |
| :--- | :--- | :--- |
| A live agent about EPP, Bell promotions, or support (the default) | 225 | "Connecting you to our Bell EPP desk now." |
| Neelam | 226 | "Connecting you to Neelam now." |
| Dwayne | 232 | "Connecting you to Dwayne now." |
| Ed | 231 | "Connecting you to Ed now." |
| Accounts receivable / payable, invoices, A/R, A/P | 220 | "Connecting you to accounts receivable now." |

Rules that matter:

- **A name beats a department.** "I need Ed in accounts payable" goes to Ed.
- **The accounts patterns are narrow on purpose.** "billing" and "payment" are
  not matched, so a prospect asking "what's the monthly payment?" is answered
  by the agent instead of being sent to accounts receivable.
- **Outbound calls only ever use the default.** A prospect who happens to say a
  staff member's name is not asking to be transferred to them.
- **DTMF 0** ("press 0 for an operator") uses the default desk.
- The extension is validated before it reaches TwiML (digits only, 1-6 long),
  so a malformed `handoffData` can never inject markup or dial stray digits.

The table lives in `HANDOFF_ROUTES` in `server.py`. Changing a destination is a
one-line edit there; `test_sms.py` asserts the exact set of extensions, so a
typo fails the gate rather than dialling a wrong number.

### A closing ends the call

`/handoff` is the action URL for two different endings, so `handoffData.action`
tells them apart:

| `action` | Who ended it | `/handoff` returns |
| :--- | :--- | :--- |
| absent | the caller asked for a person | `<Dial>` to the store with the extension as DTMF |
| `close` | the agent signed off | empty TwiML, which hangs the call up |

The key is `action`, not `reason`: every close reason must hang up, so adding
one (`caller-closed`, `permission-give-up`) can never silently start dialling
the store.

`is_agent_closing()` decides the close from the model's own reply. It matches
only when the sign-off is the whole final clause — "Goodbye.", "Have a good
day, Jonathan.", "Thanks for calling Cellular Solution. Take care." — and never
when the reply ends in a question. A phrase-level match would hang up on a live
caller: "I can take care of that for you." contains "take care" and "Bye for
now, the store will call you." contains "bye", and neither ends the
conversation.

Two things happen before the session ends:

- **The goodbye is not cut off.** The relay waits out the playback estimate
  before sending `end`, because ConversationRelay tears the session down the
  moment it arrives.
- **A barge-in cancels the close.** The wait is a receive, not a sleep: if the
  caller cuts in during the sign-off ("wait, one more thing"), the message is
  buffered for the main loop and the call stays up. A blind sleep would hang up
  on a caller who was still talking.

If the socket drops mid-goodbye, Twilio posts `/handoff` with no `HandoffData`.
The relay records the close intent in the `CLOSE_PENDING` set before the wait,
so the store is not dialled for a caller who was being hung up on. The entry is
discarded when read and withdrawn on a barge-in, so it can never suppress a
later real handoff. It is a separate set rather than a `CALL_STATE` key because
`CALL_STATE` is copied into the handler's state on a reconnect and re-saved
every turn, which would resurrect the flag.

## Changing the agent's wording without touching code

All runtime-tunable wording and relay settings are overridable via environment
variables (read per request, so set them on Railway and the change applies on
the next request or redeploy — no code edit needed):

| Env var | Overrides | Default |
| --- | --- | --- |
| `RELAY_WELCOME_GREETING` | inbound `welcomeGreeting` (`/twiml`) | `WELCOME_GREETING` in server.py |
| `RELAY_OUTBOUND_GREETING` | outbound `welcomeGreeting` (`/twiml-outbound`) | `OUTBOUND_GREETING` |
| `RELAY_SYSTEM_PROMPT` | inbound system prompt sent to Mistral | `SYSTEM_PROMPT` |
| `RELAY_OUTBOUND_SYSTEM_PROMPT` | outbound system prompt (ws setup `direction` starts with "outbound") | `OUTBOUND_SYSTEM_PROMPT` |
| `RELAY_OUTBOUND_SYSTEM_PROMPT_AFTER_PITCH` | outbound system prompt swapped in once the pitch has been spoken | `OUTBOUND_SYSTEM_PROMPT_AFTER_PITCH` |
| `RELAY_SMS_SYSTEM_PROMPT` | SMS system prompt (`POST /sms`) | `SMS_SYSTEM_PROMPT` |
| `RELAY_HINTS` | ConversationRelay `hints` attribute (both legs) | `DEFAULT_HINTS` |
| `RELAY_DISCLOSURE` | inbound AI + recording disclosure token | `DISCLOSURE` |
| `RELAY_RECORDING_DISCLOSURE` | outbound recording notice token | `RECORDING_DISCLOSURE` |
| `RELAY_INTERRUPT_ACK` | acknowledgement spoken after the caller interrupts (pipe-separated, rotated) | `INTERRUPT_ACKS` |
| `RELAY_NUDGE_AFTER` | silence-nudge delay (seconds) | `3` |
| `RELAY_NUDGE_TEXT` | silence-nudge text | `"One moment, ..."` |
| `RELAY_IDLE_AFTER` | caller-silence delay before the idle prompt (seconds, 0 = off) | `8` |
| `RELAY_IDLE_TEXT` | idle prompt text | `"Are you still there?"` |
| `RELAY_IDLE_TEXT_2` | second idle prompt, if the first drew no answer | `"Take your time. I'm here whenever you're ready."` |
| `RELAY_STORE_NUMBER` | `/handoff` dial target | `+19052642228` |
| `RELAY_HANDOFF_DIGIT_PAUSE` | `/handoff` DTMF lead-in pause, half-seconds | `10` |
| `RELAY_WS_URL` | public `wss://` URL written into `/twiml` | derived from request host |
| `RELAY_LANGUAGE` | STT/TTS language | `en-CA` |
| `RELAY_TTS_PROVIDER` | TTS provider | `Google` |
| `RELAY_TTS_VOICE` | TTS voice | `en-CA-Standard-A` |
| `RELAY_STT_PROVIDER` | STT provider / speech model | `Google`, telephony |
| `RELAY_EPP_HARDWARE_OFFERS` | appended verified hardware offers block (all prompts) | empty = no block |
| `RELAY_EPP_PLAN_OFFERS` | appended verified rate plan offers block (all prompts) | empty = no block |

Set them in the Railway dashboard (service → Variables) or via the API
(`variableUpsert`). Empty/unset falls back to the compiled-in default, so the
defaults above are always a safe rollback. Attribute values are XML-escaped
(`escape_attr`) before being written into the TwiML.

## EPP offers blocks (hardware & rate plans)

Two independent, optional env vars inject **verified, quotable Bell EPP
pricing** into every system prompt (inbound voice, outbound voice, and SMS —
at session start and re-read on every turn, so a Railway var change applies
live):

| Env var | Contents | Default |
| --- | --- | --- |
| `RELAY_EPP_HARDWARE_OFFERS` | Hardware offers payload from `demo/epp_grid.py --kind hardware` | empty = no block |
| `RELAY_EPP_PLAN_OFFERS` | Rate plan offers payload from `--kind plans` (not loaded yet) | empty = no block |

- When a block is set, `with_offers()` appends it after the base prompt
  together with `EPP_OFFERS_GUARDRAIL`: the agent may quote ONLY what is in
  the block, with its `as of YYYY-MM-DD` date; it must never compute,
  estimate, or apply discounts, and never invent a price, plan, or device;
  anything unlisted goes to the store ("the store will confirm it").
- Payloads are generated from the Bell grid CSVs by `demo/epp_grid.py` (see
  `demo/README.md`). When Bell issues a new grid, re-run the tool and
  re-upsert the var — the owner owns that duty.
- Rollback is instant: delete or clear the var and prompts return to the
  compiled defaults on the next request.
- Confidentiality: the real Bell grids are dealer-confidential. Keep them
  local in `demo/` (untracked); never commit them to this public repo and
  never paste them into chat or logs. `demo/epp_grid.example.csv` is
  synthetic.
