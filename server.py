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
import difflib
import hashlib
import hmac
import json
import logging
import os
import re
import time
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

# Speech-recognition hints: words the recognizer should expect on this line
# of business. Overridable at runtime with RELAY_HINTS (Railway var).
# ConversationRelay STT bias list. The staff names are here because callers ask
# for them by name and the recognizer was mangling them: on live calls "Ed"
# came back as "Eid" (CA7f9eff0) and "Neelam" as "Noom", "numb" and "NM"
# (CA47b56c7, CA613c343), so the handoff never fired and the model answered as
# if the caller had said something else — "Eid Mubarak!", "Noom is available
# through Bell mobility plans". "EID" was removed for the same reason: it is
# the eSIM identifier, and it pulled "Ed" toward "Eid".
DEFAULT_HINTS = (
    "Bell, Rogers, Telus, Koodo, Fido, Virgin, Freedom, eSIM, IMEI, "
    "EPP, Employee Purchase Program, mobility, plans, promotions, autopay, "
    "port-in, callback, 905-264-2228, Neelam, Dwayne, Ed"
)


def _env_or(name: str, default: str) -> str:
    """Runtime-overridable wording: set RELAY_* env vars (e.g. on Railway) to
    change greetings, system prompts or hints without editing code. Read per
    request, not at import time. An empty value falls back to the default."""
    return os.environ.get(name) or default

# The inbound opening is three back-to-back non-interruptible tokens:
#   greeting -> disclosure -> question
# Each says something the others do not. The greeting used to introduce the
# assistant as well, so callers heard the same self-introduction twice (the
# disclosure repeats it), and the question used to sit inside the greeting,
# so it was asked before the recording notice instead of last.
WELCOME_GREETING = "Hi, thanks for calling Cellular Solution."

# Spoken last in the inbound opening, after the disclosure, so the caller is
# invited to talk only once the compliance notice has finished.
OPENING_QUESTION = "How can I help you today?"

# Outbound calls (the agent dials a lead) must NOT sound like an inbound
# call: the first test calls used the inbound greeting for an outbound
# dial, so the person who picked up heard "thanks for calling" from a
# call they never made. The outbound greeting introduces the call instead.
OUTBOUND_GREETING = (
    "Hi, this is Cellular Solution's automated assistant calling about "
    "Bell's Employee Purchase Program. Do you have a quick minute?"
)

# Spoken once at the start of every call as a NON-interruptible text token:
# the caller must hear the AI + recording disclosure in full. "our" rather
# than "Cellular Solution's" because the greeting has just named the store —
# saying the name twice in a row was the repeat callers reported.
# Runtime-overridable with RELAY_DISCLOSURE.
DISCLOSURE = (
    "You are speaking with our automated AI assistant, "
    "and this call is being recorded for quality assurance."
)

# On OUTBOUND calls the greeting already identifies the AI assistant, so the
# recording notice is spoken only AFTER the caller positively answers the
# greeting's "do you have a minute?" (owner decision, 2026-11-11), as a
# non-interruptible token. Inbound calls get the full DISCLOSURE at setup.
# Runtime-overridable with RELAY_RECORDING_DISCLOSURE.
RECORDING_DISCLOSURE = (
    "Just so you know, this call is being recorded for quality assurance."
)

# Spoken when the caller goes quiet mid-call. ConversationRelay sends nothing
# on silence, so without this the call sits in dead air until the caller hangs
# up. Armed only after the caller has spoken once: before that the opening is
# still playing and a prompt would talk over it. Runtime-overridable with
# RELAY_IDLE_AFTER (seconds, 0 disables), RELAY_IDLE_TEXT and
# RELAY_IDLE_TEXT_2.
IDLE_TEXT = "Are you still there?"
# Second prompt, after the first one drew no answer. Softer and with no
# question mark: repeating "Are you still there?" at a caller who is thinking
# sounds like a machine checking a line, and a third prompt is nagging.
IDLE_TEXT_2 = "Take your time. I'm here whenever you're ready."
# Prompts per caller turn. Two, then the relay waits in silence: a caller who
# has not answered twice is either gone or does not want to talk.
IDLE_PROMPTS_MAX = 2

# How long the caller keeps hearing a reply after the relay has finished
# SENDING it. ConversationRelay reports no playback-completion event — the
# message set is setup/prompt/dtmf/interrupt/info/error, and `interrupt` only
# fires on a barge-in — so the relay has to estimate from the text it sent.
#
# Measured on this account's Google TTS: on live call CA7d8e6e the caller
# interrupted 9s into the 505-char EPP pitch and Twilio reported 118 chars
# played, i.e. 13.1 chars/s. 12.0 is deliberately slower than measured so the
# estimate never undershoots: an over-estimate makes the idle prompt arrive
# late (dead air), an under-estimate makes it talk over the agent's own
# speech, which is what happened on that call.
SPEECH_CHARS_PER_SEC = 12.0
SPEECH_PAD_S = 1.0


def estimate_speech_seconds(spoken: str) -> float:
    """Seconds of audio for `spoken`, plus a pad for TTS lead-in."""
    return len(spoken or "") / SPEECH_CHARS_PER_SEC + SPEECH_PAD_S

# Short acknowledgement spoken when the caller talks over the agent, so the
# interruption sounds like a person yielding the floor instead of the agent
# going dead mid-sentence. Spoken on the prompt that FOLLOWS the interrupt,
# never at the interrupt itself: Twilio fires "interrupt" the moment the
# caller starts talking, so speaking then would talk over them. Rotated so a
# caller who interrupts twice does not hear the same word twice.
# Pipe-separated; an explicitly empty RELAY_INTERRUPT_ACK disables it.
INTERRUPT_ACKS = "Sure.|Go ahead.|Of course."

# Minimum seconds between acknowledgements, so rapid-fire interruptions do
# not turn into "Sure. Sure. Sure."
INTERRUPT_ACK_COOLDOWN = 2.0

# A caller turn that is only a greeting or filler. Acknowledging a cut-in with
# "Sure." is right after "wait, what about the price?" and wrong after
# "Hello.", which is what a live call produced.
GREETING_ONLY_RE = re.compile(
    r"^(uh|um|er|ah|yeah|yes|no|hi|hello|hey|ok|okay|sure|huh|right|so)"
    r"([,\s]+(there|hello|hi|yeah|ok|okay))?[\s.!?,]*$",
    re.IGNORECASE,
)

# A caller turn that asks something. A repeated assistant reply is then an
# ANSWER to that question, not a loop: a live call asked "what's the call
# about?" twice and the repeat guard answered "I'm having trouble hearing
# you" both times (CA13109f). Keyed on the caller's transcribed words, not on
# the model's output, so the decision is deterministic.
#
# A question is punctuated ("...?") or inverted — a question word as the FIRST
# word. Matching a question word anywhere in the turn is not enough: the bare
# auxiliaries below appear in ordinary statements, so "it was changed" and "I
# was told" counted as questions and unlocked the repeat guard on a live call
# (CAd9a6ac), letting the agent ask the same callback question three times.
QUESTION_WORD_RE = re.compile(
    r"\b(what|why|how|who|whom|whose|when|where|which|is|are|was|were|"
    r"do|does|did|can|could|would|should|may|might)\b",
    re.IGNORECASE,
)

# Fillers a caller puts in front of a question ("uh, can you...", "okay so
# what..."). Stripped before the first-word test, or the inversion is missed.
LEADING_FILLER_RE = re.compile(
    r"^(?:(?:uh|um|er|ah|oh|hey|hi|hello|yeah|yep|okay|ok|so|well|and|but|"
    r"like|just|actually|sorry)[\s,]+)+",
    re.IGNORECASE,
)


def _caller_asked(user_text: str) -> bool:
    """True when the caller's turn is a real question, not a greeting/filler.

    Punctuated ("...?") or inverted — a question word as the first word after
    any leading filler. A question word further into the turn is not enough:
    "it was changed" is a statement, and counting it as a question let the
    repeat guard through on a live call.
    """
    t = (user_text or "").strip()
    if not t or GREETING_ONLY_RE.match(t):
        return False
    if "?" in t:
        return True
    return bool(QUESTION_WORD_RE.match(LEADING_FILLER_RE.sub("", t)))


def interrupt_acks() -> list[str]:
    """Acknowledgements to rotate through. An explicitly empty
    RELAY_INTERRUPT_ACK disables them (returns an empty list)."""
    raw = os.environ.get("RELAY_INTERRUPT_ACK", INTERRUPT_ACKS)
    return [a.strip() for a in raw.split("|") if a.strip()]

NEGATIVE_RE = re.compile(
    r"\b(no|nope|not|busy|don't|dont|can't|cant|hang on|later|stop)\b",
    re.IGNORECASE,
)


# The caller is wrapping up. A short goodbye from the assistant is the right
# reply here, so the dead-end guard must stay out of the way.
CLOSING_RE = re.compile(
    r"\b(that'?s it|that is it|that'?s all|nothing else|goodbye|good bye|bye|"
    r"hang up|i'?m done|we'?re done|no thanks)\b",
    re.IGNORECASE,
)

# The ASSISTANT said goodbye, so the call is over. Deliberately narrower than
# CLOSING_RE: that one also matches caller-side phrases ("no thanks", "that's
# all") which the agent can use mid-conversation without meaning the call is
# finished. A reply that ends in a question is never a closing, however it is
# worded — "anything else before I let you go?" is still an open turn.
#
# The sign-off must also be the whole final clause, not a phrase inside one.
# "I can take care of that for you." contains "take care" and "Bye for now, the
# store will call you." contains "bye", but neither ends the conversation, and
# hanging up on them would cut a live caller off mid-sentence.
AGENT_CLOSING_RE = re.compile(
    r"(?:goodbye|good bye|bye bye|bye for now|bye|good night|"
    r"have a (?:good|great|nice|wonderful) (?:day|one|evening|night|afternoon|"
    r"morning|weekend|week)|"
    r"have a (?:good|great|nice|wonderful) rest of your (?:day|evening)|"
    r"enjoy (?:the rest of )?your (?:day|evening|weekend)|"
    r"take care(?: of yourself)?|"
    r"talk to you (?:later|soon|tomorrow)|"
    r"(?:thanks|thank you) for (?:calling|your time)(?: cellular solution)?"
    r"(?: today)?|"
    r"i'?ll let you go)"
)

# A sign-off can carry a short lead-in: "Okay, goodbye." / "Perfect. Take care."
# Applied repeatedly, so "Thank you and have a good day." reduces to the
# sign-off. Em dashes count as separators: "Okay — goodbye."
AGENT_CLOSING_LEADIN_RE = re.compile(
    r"^(?:okay|ok|alright|all right|well|so|and|great|perfect|lovely|thanks|"
    r"thank you|you're welcome|no problem|sure|absolutely|got it|understood)"
    r"[,!.—–\- ]+"
)

# "Have a good day, Jonathan." — the model addresses the caller by name, so the
# sign-off is not the last comma clause. The tail must be CAPITALIZED, and is
# stripped before lowercasing: a lowercase tail is content, not a name, so
# "Thanks for calling, go ahead." keeps its clause and does not hang up. Only a
# one- or two-word tail is stripped, so "Bye for now, the store will call you."
# keeps its content too.
_TRAILING_VOCATIVE_RE = re.compile(r",\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?$")

_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def is_agent_closing(reply: str) -> bool:
    """True when the assistant's reply signs off, so the call should end.

    A live call (CA36b78) said "Goodbye." five times in a row: the caller
    answered "Goodbye.", the agent answered "Goodbye." again, and the call only
    ended when the caller hung up. Nothing ended the session on a sign-off.
    """
    t = (reply or "").strip()
    if not t:
        return False
    if t.endswith("?"):
        return False
    # Curly apostrophes come back from the model as often as straight ones.
    t = t.replace("\u2019", "'")
    # Only the last sentence can sign off: everything before it is content the
    # caller still needs to hear.
    last = _SENTENCE_SPLIT_RE.split(t)[-1].strip().strip(".!…,;: ")
    # Readings of that sentence, most specific first: the whole thing ("Okay,
    # goodbye."), its last comma clause ("...solution, goodbye."), the sentence
    # without a trailing name ("Have a good day, Jonathan."), that sentence's
    # last comma clause ("...solution, goodbye, Don."), and the tail after a
    # comma-less "and" ("Thanks for your time and have a good day."). A sign-off
    # has to match one of them whole, so a phrase inside a longer clause ("I can
    # take care of that for you.") never counts.
    candidates = [last, last.split(",")[-1], last.split(" and ")[-1]]
    without_vocative = _TRAILING_VOCATIVE_RE.sub("", last)
    if without_vocative != last:
        candidates.append(without_vocative)
        candidates.append(without_vocative.split(",")[-1])
        candidates.append(without_vocative.split(" and ")[-1])
    for candidate in candidates:
        c = candidate.strip().strip(".!…,;: ").lower()
        if AGENT_CLOSING_RE.fullmatch(c):
            return True
        # Strip lead-ins repeatedly: "Thank you and have a good day." needs two
        # passes ("thank you " then "and ") to reach the sign-off.
        while True:
            stripped = AGENT_CLOSING_LEADIN_RE.sub("", c)
            if stripped == c:
                break
            c = stripped
            if AGENT_CLOSING_RE.fullmatch(c):
                return True
    return False

# The caller agreed to listen. The outbound greeting asks "Do you have a quick
# minute?", so the pitch must wait for an actual answer: a live call (CA4ae95)
# said "Hello." three seconds after the greeting and got 23 seconds of pitch
# before it could answer. Word-bounded, and only ever consulted AFTER
# NEGATIVE_RE and CLOSING_RE, so "yeah, no" and "okay, bye" still decline.
AFFIRMATIVE_RE = re.compile(
    r"\b(yes|yeah|yep|yup|sure|ok|okay|alright|all right|go ahead|go for it|"
    r"shoot|certainly|absolutely|of course|please do|sounds good|why not|"
    r"i do|have a minute|uh huh|mm hmm|mmhmm)\b",
    re.IGNORECASE,
)

# The permission phase: the greeting asked for a minute, the caller has not
# answered yet. Relay-spoken and fixed, like the pitch, so the reply is
# deterministic and cannot ramble. Two asks, then a polite close — a caller who
# has heard the greeting plus two re-asks is not going to engage.
MAX_PERMISSION_REASKS = 2
PERMISSION_REASKS = (
    "Sorry — is now an okay time for a quick minute about the Bell employee "
    "offer?",
    "It's Cellular Solution's automated assistant calling about Bell's "
    "Employee Purchase Program. Do you have a quick minute?",
)
PERMISSION_GIVE_UP_CLOSE = (
    "No problem — I'll let you go. You can check your company's eligibility "
    "anytime at cellular solution dot c a under Employee Offers. Have a good "
    "day."
)

# A reply this short with no question leaves the caller with nothing to say.
# The prompt forbids it ("Never answer with a single word like 'Great' or
# 'Okay'") but a live call produced "Okay." and "Alright." twice in a row
# after the caller declined the callback, so the prompt alone is not enough.
DEAD_END_MAX_WORDS = 3
DEAD_END_RECOVERIES = (
    "No problem. Is there anything else about EPP I can explain before we finish?",
    "That's fine. Is there anything else you would like to know about EPP?",
)


def should_disclose_recording(user_text: str, caller_turns: int) -> bool:
    """True when the outbound recording notice should be spoken on this turn.

    The notice used to wait for an affirmative answer to "do you have a quick
    minute?". A live call showed the cost: the caller opened with "Hello.",
    which is not affirmative, so no notice played for 77 seconds and then it
    landed on "Uh, okay. That's it." on the way out. A greeting is not
    consent, but it is not a refusal either, so disclose on the first turn
    that is not a clear decline — and on the second turn regardless, so a
    call that keeps going can never run without the notice.
    """
    if caller_turns >= 2:
        return True
    t = (user_text or "").strip().lower()
    if not t:
        return False
    return not NEGATIVE_RE.search(t)


def is_dead_end(reply: str) -> bool:
    """True when a reply gives the caller nothing to respond to: a few words
    with no question in them. A reply that is itself a closing ("You're
    welcome. Goodbye.") is a legitimate close, not a dead end — a live call
    said "Thanks." and the guard answered with another question (CA13109f).
    """
    t = (reply or "").strip()
    if not t:
        return True
    if CLOSING_RE.search(t):
        return False
    if "?" in t:
        return False
    return len(t.split()) <= DEAD_END_MAX_WORDS


def _pitch_delivered(reply: str) -> bool:
    """True when this text is the EPP pitch, so the post-pitch prompt should
    take over. Matches on the pitch's own claims rather than on length alone,
    so a long answer about something else does not trip it.

    Both spellings of the discount count: the model writes "30 percent" when
    speaking but "30%" when it slips into written form, and a miss here means
    the pre-pitch prompt survives the pitch.
    """
    t = (reply or "").lower()
    if len(t) < REPEAT_MIN_CHARS:
        return False
    return any(
        claim in t
        for claim in ("activation fee", "30 percent", "30%", "30 %")
    )


def _after_pitch_prompt() -> str:
    """The phase-2 outbound system prompt, with the EPP offers injected."""
    return with_offers(
        _env_or(
            "RELAY_OUTBOUND_SYSTEM_PROMPT_AFTER_PITCH",
            OUTBOUND_SYSTEM_PROMPT_AFTER_PITCH,
        )
    )


def _swap_to_after_pitch(history: list, state: dict, call_sid: str) -> None:
    """Move the call past the pitch: flag it and replace the system prompt with
    the post-pitch one, which carries no pitch script and no "go straight into
    the pitch" trigger. Idempotent, so the reply path and the interrupt path
    can both call it.
    """
    if state["pitch_delivered"]:
        return
    state["pitch_delivered"] = True
    history[0] = {"role": "system", "content": _after_pitch_prompt()}
    log_event("pitch_delivered", callSid=call_sid)


def _retire_pitch_phase(history: list, state: dict) -> None:
    """Leave the pitch phase WITHOUT delivering the pitch: the caller declined,
    or never consented to listen.

    Swaps in the post-pitch prompt so the model cannot pitch on a later turn —
    the pre-pitch prompt still carries the pitch script and the "go straight
    into the pitch" trigger, so simply skipping the relay pitch would let the
    model produce an uncontrolled one. `pitch_delivered` and its log event stay
    reserved for an actual pitch, so this is a separate flag.
    """
    if state.get("pitch_skipped"):
        return
    state["pitch_skipped"] = True
    history[0] = {"role": "system", "content": _after_pitch_prompt()}


def _gate_commits(partial: str, recent_replies=()) -> bool:
    """True once a reply being generated is past the point where it could be
    a dead end or a repeat of a recent reply."""
    t = (partial or "").strip()
    if not t:
        return False
    # Still tracking a recent reply word for word: it may be a repeat, so keep
    # holding until it diverges. This is what catches the model re-emitting a
    # pitch after an interrupt, where the truncated assistant turn in history
    # no longer looks like a completed pitch.
    c = _norm_text(t)
    for prev in recent_replies:
        p = _norm_text(prev)
        if p and c and (p.startswith(c) or c.startswith(p)):
            return False
    # A question the caller cannot answer must never be spoken, so hold the
    # reply instead of committing on its question mark.
    if FORBIDDEN_QUESTION_RE.search(t):
        return False
    # A claim about a colleague's availability is invented — the relay has no
    # calendar access — so hold the reply the same way. The caller must never
    # hear it.
    if claims_staff_availability(t):
        return False
    # A denial of the relay's own transfer capability is false — the handoff
    # dials the store's extension — so hold it the same way.
    if TRANSFER_DENIAL_RE.search(t):
        return False
    # So is a promise to transfer: any real transfer already happened before
    # the model was called.
    if TRANSFER_PROMISE_RE.search(t):
        return False
    # A dollar figure the offers blocks do not contain is invented.
    if invented_price(t):
        return False
    # A price-ish reply is held until its first sentence completes, so an
    # invented figure can be replaced with the whole sentence rather than
    # leaving a dangling lead-in already spoken.
    if PRICE_RISK_RE.search(t) and not re.search(r"[.?!]", t):
        return False
    # A closing commits at once: holding it to end-of-stream only delays a
    # goodbye, and is_dead_end no longer treats it as a dead end.
    if CLOSING_RE.search(t):
        return True
    return "?" in t or len(t.split()) > DEAD_END_MAX_WORDS


def _norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip().lower()


def _forbidden_at(spoken: str, pending: str) -> int | None:
    """Index in `pending` where a forbidden question starts, or None.

    The pre-speech gate only holds a reply until it commits, so a forbidden
    question that arrives AFTER the commit point ("Sure, I can help. Are you
    with a company that offers EPP?") would stream out unfiltered. This scans
    the not-yet-spoken text before each flush. The lookback covers a match
    that straddles the last flush: the longest pattern is ~20 chars, so 32
    chars of already-spoken text is enough.
    """
    look = 32
    tail = (spoken or "")[-look:]
    m = FORBIDDEN_QUESTION_RE.search(tail + (pending or ""))
    if not m:
        return None
    return max(0, m.start() - len(tail))


# Near-duplicate repeat detection. Replies shorter than REPEAT_MIN_CHARS are
# compared exactly only, so ordinary short answers are never flagged. A long
# reply counts as a repeat when it is either mostly the same text
# (REPEAT_SIMILARITY) or shares a long verbatim block (REPEAT_MIN_BLOCK) —
# the model rewords the opening of the pitch but keeps the body word for
# word, which the ratio alone misses.
REPEAT_MIN_CHARS = 80
REPEAT_SIMILARITY = 0.85
REPEAT_MIN_BLOCK = 150

# How many recent replies the pre-speech gate compares against. One is not
# enough: a live call re-spoke the pitch two turns after the first one, with a
# short "Sorry, I didn't catch that" fallback in between.
RECENT_REPLIES_KEPT = 3

# Questions the caller cannot answer. The agent does not have the list of
# partner companies, so "are you with a company that offers EPP?" asks the
# caller to know something only the store knows. A live call asked exactly
# that (CA7d8e6e) even though the prompt forbids it, which is why this is a
# deterministic guard and not another prompt rule.
FORBIDDEN_QUESTION_RE = re.compile(
    r"\b(are you with|is your (company|employer)|does your (company|employer)"
    r"|do you work for|what (company|employer) do you"
    r"|are you (a|an) bell employee|is your company (eligible|on the list)"
    r"|does your company offer)\b",
    re.IGNORECASE,
)

# Spoken instead of a forbidden question: the next step the call actually
# wants, which the caller can always answer.
CALLBACK_QUESTION_FALLBACK = (
    "Would you like the store to call you back about Bell's Employee "
    "Purchase Program?"
)

# Spoken in place of a reply the model looped. Two variants, so the guard
# never answers a loop with a loop of its own.
#
# Neither claims a comprehension failure. The guard fires because the MODEL
# repeated itself, not because the caller was unintelligible, and a live call
# (CAd9a6ac) answered two perfectly clear turns with "Sorry, I didn't catch
# that" and "I'm having trouble hearing you" — which sounds like a broken line
# and invites the caller to repeat themselves for nothing. These redirect
# instead: the agent owns the repetition and asks what is actually useful.
REPEAT_FALLBACKS = (
    "Let me put that another way — what would be most useful for you right now?",
    "Sorry, let me start over. What would you like to know?",
)

# The staff members the store routes by name. Kept in step with the labels in
# HANDOFF_ROUTES by a test, because the availability guard below keys on them.
HANDOFF_PEOPLE = ("Neelam", "Dwayne", "Ed")

# A reply that says whether a named colleague is available. The relay has no
# calendar access, so any such claim is invented. On a live outbound call
# (CAdc6292) a caller asked to be transferred to Ed and the agent answered
# "Ed isn't available right now." — a fabrication about a real person, and the
# kind of thing that makes a caller hang up. Outbound calls never route to a
# named person by design, so the honest answer is a callback offer.
AVAILABILITY_CLAIM_RE = re.compile(
    r"\b(?:isn'?t|is not|is|'s|’s|aren'?t|are not|are|will be|won'?t be)\s+"
    r"(?:currently\s+|right now\s+)?(?:not\s+)?"
    r"(?:available|unavailable|in today|in right now|at (?:his|her|their) desk|"
    r"out of the office|out of town|away|off today|on vacation|on holiday|"
    r"busy|with (?:a|another) customer|back (?:today|tomorrow|soon|later|"
    r"next week|on))\b",
    re.IGNORECASE,
)

# Spoken instead of the claim. Both end in a question so the dead-end guard
# leaves them alone, and neither matches CLOSING_RE so a fallback can never
# end the call.
AVAILABILITY_FALLBACKS = (
    "I can't check anyone's schedule from here, but the store can help — "
    "would you like them to call you back?",
    "I don't have access to anyone's calendar, but I can have the store reach "
    "out to you. Would that work?",
)


# The relay CAN transfer: handoff_route() dials the store's extension before
# the model is ever called. So a reply that says it cannot transfer is a
# fabrication about the agent's own capability. On CA47b56c7 a caller asked
# "Can you transfer me to numb?" and the agent answered "I can't transfer
# calls." — the caller hung up. The request net below means such a caller is
# connected instead of answered; this guard is the backstop for any remaining
# denial, so the words are never spoken.
TRANSFER_DENIAL_RE = re.compile(
    r"\b(?:can'?t|cannot|can not|unable to|not able to|no way to|"
    r"don'?t have the (?:ability|option|capability) to|"
    r"am not able to|do not have the (?:ability|option|capability) to)\s+"
    r"(?:transfer|connect|put (?:you|them) through|forward)\b",
    re.IGNORECASE,
)

# Spoken instead of the denial. Both end in a question so the dead-end guard
# leaves them alone, and neither matches CLOSING_RE so a fallback can never
# end the call.
TRANSFER_FALLBACKS = (
    "The store can help with that — would you like them to call you back?",
    "I can have the store reach out to you about that. Would that work?",
)

# The model ANNOUNCING a transfer it cannot perform. handoff_route() runs on
# the CALLER's words before the model is called, so by the time the model
# speaks, any real transfer has already happened — a promise in the reply is
# therefore always false. On CAe33eb7 the agent said "I can connect you to the
# store at 905-264-2228 so they can assist further. I'm connecting you now."
# and then "I'm connecting you to the store at 905-264-2228 now." No handoff
# fired: the caller was told twice they were being connected and stayed on the
# line. A false promise is worse than a denial, so it is guarded the same way.
TRANSFER_PROMISE_RE = re.compile(
    r"\b(?:i'?m|i am|i'?ll|i will|we'?re|we are|let me|can|could)\s+"
    r"(?:connect|transfer|put)(?:ing)?\s+(?:you|them)\b"
    r"|\bconnecting you\b|\btransferring you\b|\bputting you through\b"
    r"|\bone moment while i\b",
    re.IGNORECASE,
)

# A dollar figure the model produced. The VERIFIED OFFERS blocks are the only
# pricing the agent may quote, so any amount that does not appear in them is
# invented. On CAe33eb7 the agent told a caller "The cheapest Bell mobility
# plans start at about $35 a month with the Employee Purchase Program" —
# RELAY_EPP_PLAN_OFFERS was unset, so $35 came from nowhere, and a caller could
# hold the store to it. The prompt already forbids this; the model did it
# anyway, which is why this is a deterministic guard.
PRICE_AMOUNT_RE = re.compile(
    r"\$\s?(\d{1,4})(?:\.(\d{1,2}))?"
    r"|\b(\d{1,4})(?:\.(\d{1,2}))?\s*(?:dollars?|bucks)\b(?:\s+(\d{1,2})\b)?",
    re.IGNORECASE,
)

# Spoken instead of an invented price. Both end in a question so the dead-end
# guard leaves them alone, and neither quotes a figure, so the guard cannot
# loop on its own fallback.
PRICE_FALLBACKS = (
    "I don't have live pricing here, so the store will confirm the exact price "
    "for you. Would you like them to call you back?",
    "The store can confirm the exact price for you. Would you like a callback?",
)


# A reply that is heading toward a price. The pre-speech gate normally commits
# at the first question mark or after three words, which is too early here: by
# the time the figure arrives the lead-in ("The cheapest Bell mobility plans
# start at about") has already been spoken, and the caller hears a dangling
# fragment before the fallback. So a price-ish reply is held until its first
# sentence completes, and the whole sentence is then replaced if the figure is
# invented. Only price replies pay the hold.
PRICE_RISK_RE = re.compile(
    r"\$|\b(?:dollars?|bucks|price|pricing|costs?|monthly|cheapest|cheaper)\b"
    r"|\b(?:start|starts|starting) at\b|\ba month\b|\bper month\b",
    re.IGNORECASE,
)


def _amounts(text: str) -> set[str]:
    """Every dollar figure in `text`, normalized to a bare 2dp number string.

    Handles the spoken forms the model actually produces: "$62.91", "$62",
    "62.91 dollars", "62 dollars", and "62 dollars 91" (the cents said
    separately, which is how a price reads aloud).
    """
    out: set[str] = set()
    for m in PRICE_AMOUNT_RE.finditer(text or ""):
        whole = m.group(1) or m.group(3)
        if not whole:
            continue
        cents = m.group(2) or m.group(4) or m.group(5) or "0"
        try:
            out.add(f"{float(whole) + float(cents) / 100:.2f}")
        except ValueError:
            continue
    return out


def invented_price(reply: str) -> bool:
    """True when the reply quotes a dollar figure the offers blocks do not
    contain. With no offers configured, every figure is invented."""
    quoted = _amounts(reply)
    if not quoted:
        return False
    verified = _amounts(
        _env_or("RELAY_EPP_HARDWARE_OFFERS", EPP_HARDWARE_OFFERS)
        + " "
        + _env_or("RELAY_EPP_PLAN_OFFERS", EPP_PLAN_OFFERS)
    )
    return bool(quoted - verified)


# A one- or two-word caller turn that cannot stand alone. STT returns
# fragments when the caller is cut off or the line clips: on CAe33eb7 "It"
# drew a 294-character, 25-second EPP pitch and "My." drew "It sounds like you
# may have something to share." A fragment needs a short clarifying question,
# not a pitch, so the relay answers it directly instead of calling the model.
#
# Only words that cannot be an answer count. A single meaningful word —
# "billing", "iPhone", "EPP" — is a real request and still reaches the model,
# so the guard cannot swallow a caller who says one useful thing.
FRAGMENT_MAX_WORDS = 2
FRAGMENT_WORDS = frozenset({
    "a", "ah", "an", "and", "are", "at", "be", "been", "but", "can", "could",
    "did", "do", "does", "er", "for", "had", "has", "have", "he", "here",
    "how", "i", "i'm", "im", "in", "is", "it", "it's", "its", "just", "like",
    "me", "my", "not", "of", "oh", "on", "she", "should", "so", "that",
    "that's", "thats", "the", "there", "they", "this", "to", "uh", "um",
    "was", "we", "well", "were", "what", "when", "where", "who", "why",
    "will", "with", "would", "you", "your",
})
FRAGMENT_PROMPTS = (
    "Sorry, I only caught part of that — what can I help you with?",
    "I didn't get all of that. What can I help you with today?",
)


def is_fragment(user_text: str) -> bool:
    """True when the caller's turn is too short to be an answer.

    A greeting or a yes/no is a real turn and is left to the model, as is
    anything carrying a digit (a phone number or a price the caller is
    quoting). Otherwise the turn is a fragment only when every word in it is
    one that cannot stand alone.
    """
    t = (user_text or "").strip()
    if not t or GREETING_ONLY_RE.match(t):
        return False
    if any(ch.isdigit() for ch in t):
        return False
    words = [w for w in re.split(r"[\s,.;:!?]+", t.lower()) if w]
    if not words or len(words) > FRAGMENT_MAX_WORDS:
        return False
    return all(w in FRAGMENT_WORDS for w in words)


def claims_staff_availability(reply: str) -> bool:
    """True when the reply says whether a named colleague is available.

    Requires BOTH an availability phrase and a staff name in the same text:
    "the store isn't available right now" is a statement about opening hours,
    not about a person, and must not be replaced.
    """
    t = reply or ""
    if not AVAILABILITY_CLAIM_RE.search(t):
        return False
    return any(
        re.search(rf"\b{re.escape(name)}\b", t, re.IGNORECASE)
        for name in HANDOFF_PEOPLE
    )


def _availability_at(spoken: str, pending: str) -> int | None:
    """Index in `pending` where a staff-availability claim starts, or None.

    Mirrors _forbidden_at: the pre-speech gate only holds a reply until it
    commits, so a claim that arrives AFTER the commit point ("Would you like
    the store to call you back? Ed isn't available right now.") would stream
    out unfiltered. The lookback covers a match that straddles the last flush.

    The cut backs up to the start of the sentence holding the claim, so the
    subject is not left dangling — otherwise the caller hears "…call you back?
    Ed" followed by the fallback.
    """
    look = 64
    tail = (spoken or "")[-look:]
    window = tail + (pending or "")
    if not claims_staff_availability(window):
        return None
    m = AVAILABILITY_CLAIM_RE.search(window)
    if not m:
        return None
    start = m.start()
    cut = max(
        window.rfind(". ", 0, start),
        window.rfind("? ", 0, start),
        window.rfind("! ", 0, start),
    )
    if cut != -1:
        start = cut + 2
    return max(0, start - len(tail))


def _transfer_denial_at(spoken: str, pending: str) -> int | None:
    """Index in `pending` where a "I can't transfer you" denial starts, or None.

    Same shape as _availability_at: the pre-speech gate only holds a reply
    until it commits, so a denial arriving after the commit point would stream
    out unfiltered. The cut backs up to the start of the sentence holding the
    denial so the caller does not hear a dangling fragment.
    """
    look = 64
    tail = (spoken or "")[-look:]
    window = tail + (pending or "")
    m = TRANSFER_DENIAL_RE.search(window)
    if not m:
        return None
    start = m.start()
    cut = max(
        window.rfind(". ", 0, start),
        window.rfind("? ", 0, start),
        window.rfind("! ", 0, start),
    )
    if cut != -1:
        start = cut + 2
    return max(0, start - len(tail))


def _transfer_promise_at(spoken: str, pending: str) -> int | None:
    """Index in `pending` where a false transfer promise starts, or None.

    Same shape as _transfer_denial_at: the pre-speech gate only holds a reply
    until it commits, so a promise arriving after the commit point would
    stream out unfiltered.
    """
    look = 64
    tail = (spoken or "")[-look:]
    window = tail + (pending or "")
    m = TRANSFER_PROMISE_RE.search(window)
    if not m:
        return None
    start = m.start()
    cut = max(
        window.rfind(". ", 0, start),
        window.rfind("? ", 0, start),
        window.rfind("! ", 0, start),
    )
    if cut != -1:
        start = cut + 2
    return max(0, start - len(tail))


def _invented_price_at(spoken: str, pending: str) -> int | None:
    """Index in `pending` where an invented price starts, or None.

    Same shape as _transfer_denial_at. The lookback is wider than the other
    guards because a price claim carries its own lead-in ("the cheapest plans
    start at about $35 a month"), and cutting mid-sentence would leave the
    caller with a dangling fragment.
    """
    look = 96
    tail = (spoken or "")[-look:]
    window = tail + (pending or "")
    if not invented_price(window):
        return None
    verified = _amounts(
        _env_or("RELAY_EPP_HARDWARE_OFFERS", EPP_HARDWARE_OFFERS)
        + " "
        + _env_or("RELAY_EPP_PLAN_OFFERS", EPP_PLAN_OFFERS)
    )
    for m in PRICE_AMOUNT_RE.finditer(window):
        whole = m.group(1) or m.group(3)
        if not whole:
            continue
        cents = m.group(2) or m.group(4) or m.group(5) or "0"
        try:
            amount = f"{float(whole) + float(cents) / 100:.2f}"
        except ValueError:
            continue
        if amount in verified:
            continue
        start = m.start()
        cut = max(
            window.rfind(". ", 0, start),
            window.rfind("? ", 0, start),
            window.rfind("! ", 0, start),
        )
        if cut == -1:
            # No sentence boundary to cut at, so the whole pending buffer is
            # the lead-in ("The cheapest Bell mobility plans start at about").
            # Speaking it would leave the caller with a dangling fragment
            # before the fallback, so drop all of it.
            return 0
        return max(0, cut + 2 - len(tail))
    return None


def is_repeat(prev: str, cur: str) -> bool:
    """True when the new reply repeats the previous one — verbatim, or, for
    long replies, near-verbatim. Backstop against the model looping the same
    pitch on a call.

    A live call showed the model re-emitting the EPP pitch four times with
    only the opening words reworded ("Great. EPP gives..." / "Bell's Employee
    Purchase Program, or EPP, gives..."), so exact matching was not enough.
    Short replies are compared exactly only: "Sure." and "Go ahead." are
    legitimate consecutive answers, not a loop.
    """
    p, c = _norm_text(prev), _norm_text(cur)
    if not p or not c:
        return False
    if p == c:
        return True
    if len(p) < REPEAT_MIN_CHARS or len(c) < REPEAT_MIN_CHARS:
        return False
    # A reworded repeat can be the previous reply with a clause added or
    # dropped. On a live call (CAdc6292) the agent asked the callback question
    # twice, the second time without the "Ed isn't available right now."
    # opener: the ratio landed at 0.844, just under REPEAT_SIMILARITY, and the
    # shared block (81 chars) was under REPEAT_MIN_BLOCK, so neither path
    # fired. Containment catches it — the shorter reply IS the longer one.
    # The length gate above keeps this from flagging "Sure." inside a reply.
    if c in p or p in c:
        return True
    matcher = difflib.SequenceMatcher(None, p, c)
    if matcher.ratio() >= REPEAT_SIMILARITY:
        return True
    return matcher.find_longest_match(0, len(p), 0, len(c)).size >= REPEAT_MIN_BLOCK

# Shared by both outbound phases. The ONLY difference between them is whether
# the pitch script is still available: once the pitch has been spoken, the
# "go straight into the pitch" trigger has to go, or the model re-pitches on
# every unclear turn. A live call re-emitted the same 316-char pitch on
# "Hello." because that trigger outranked "give the pitch ONCE".
_OUTBOUND_IDENTITY = (
    "You are Cellular Solution's automated voice assistant, a Bell authorized "
    "dealer at 81 Zenway Blvd, Unit 6, Vaughan, Ontario. Store hours are "
    "Monday to Friday, 9am to 5pm. Store phone: 905-264-2228. "
    "You are Cellular Solution's automated assistant, not a person. The "
    "greeting already introduced you as one, so do not repeat the disclosure "
    "at all. "
    "YOU called this person; they did not call you. Never say 'thanks for "
    "calling' and never ask 'how can I help you' as an opening. "
)

_OUTBOUND_EPP_FACTS = (
    "Bell's Employee Purchase Program, known as EPP, gives up to 30 percent "
    "off Bell mobility plans and no activation fees. "
    "EPP is for employees of select partner companies — NOT only Bell "
    "employees. Never say someone has to work for Bell, and never ask if they "
    "are a Bell employee. To check eligibility, tell the person to visit "
    "cellular solution dot c a, click Employee Offers, and type in their "
    "company name — if their company comes up, they are eligible. Proof of "
    "employment is shown after ordering. "
    "If the person asks for an 'EPP department' or any department or person "
    "to speak to, never say we do not have EPP: there is no separate "
    "department — EPP is handled by this assistant and by the store at "
    "905-264-2228. Offer to check eligibility on the website or to arrange "
    "a store callback. "
    "Pronunciation: always say the website as 'cellular solution dot c a' — "
    "two words, cellular and solution. Never say 'w w w' or 'triple w'. "
    "Never repeat a previous reply, in whole or in part. Say the website "
    "directions once when the topic first comes up; after that, just "
    "offer the callback. "
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
    "question at a time. "
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

# The EPP pitch, spoken by the RELAY rather than generated by the model.
# A live call (CA7d8e6e) got a 505-char monologue from the model: ~39s of
# audio, with the callback offer buried at char 430, so the caller heard 23%
# of it and hung up without ever being offered a callback. A fixed script is
# deterministic in length, cannot be re-pitched, and removes the phase-swap
# race entirely. Written in speakable form: the website is spelled out.
OUTBOUND_PITCH = (
    "Bell's Employee Purchase Program gives employees of select partner "
    "companies up to 30 percent off Bell mobility plans with no activation "
    "fees. You can check your company at cellular solution dot c a under "
    "Employee Offers. Would you like the store to call you back?"
)

# Ceiling for the spoken pitch. At the measured 13.1 chars/s this is ~24s of
# audio; anything longer buries the callback offer.
PITCH_MAX_CHARS = 320

# Phase 1: the pitch has not been given yet.
OUTBOUND_SYSTEM_PROMPT = (
    _OUTBOUND_IDENTITY
    + "You called about Bell's Employee Purchase Program, known as EPP. "
    "The relay speaks the EPP pitch itself, so you never give it: do not "
    "describe the program, the discount, or the website unless the caller "
    "asks a direct question about it. "
    "If the caller declines the call, thank them briefly and end. "
    + _OUTBOUND_EPP_FACTS
)

# Phase 2: the pitch has been spoken. The pitch script and the "go straight
# into the pitch" trigger are GONE, so an unclear turn cannot resurrect them.
OUTBOUND_SYSTEM_PROMPT_AFTER_PITCH = (
    _OUTBOUND_IDENTITY
    + "You have ALREADY given the EPP pitch on this call. Never give it "
    "again, not even reworded, and never repeat the website directions. "
    "If the caller says 'hello', is unclear, or asks what the call is "
    "about, answer in ONE short sentence in your own words about what the "
    "call is about, then ask this question: 'Would you like the store to "
    "call you back about Bell's Employee Purchase Program?' Never ask which "
    "company they work for or whether their company offers EPP: they "
    "cannot know that, and only the store can confirm eligibility. "
    "Your next step is the callback offer: ask for their name and the best "
    "number to reach them at, one question at a time. "
    + _OUTBOUND_EPP_FACTS
)

SYSTEM_PROMPT = (
    "You are Cellular Solution's automated voice assistant, a Bell authorized "
    "dealer at 81 Zenway Blvd, Unit 6, Vaughan, Ontario. Store hours are "
    "Monday to Friday, 9am to 5pm. Store phone: 905-264-2228. "
    "You are an automated assistant, not a person. The opening already "
    "introduced you as one, so never make the disclosure your whole reply — "
    "always combine it with something useful in the same reply. The opening "
    "also already asked how you can help, so never ask that again — answer "
    "what the caller actually said. "
    "Your job is to qualify interest in Bell mobility and the Bell Employee "
    "Purchase Program, known as EPP: up to 30 percent off Bell mobility "
    "plans and no activation fees. EPP is for employees of select partner "
    "companies — NOT only Bell employees. Never say someone has to work for "
    "Bell, and never ask if they are a Bell employee. To check eligibility, "
    "tell the caller to visit cellular solution dot c a, click Employee "
    "Offers, and type in their company name — if their company comes up, "
    "they are eligible. Proof of employment is shown after ordering. "
    "If the caller asks for an 'EPP department' or any department or person "
    "to speak to, never say we do not have EPP: there is no separate "
    "department — EPP is handled by this assistant and by the store at "
    "905-264-2228. Offer to check eligibility on the website or to arrange "
    "a store callback. "
    "Never say you cannot transfer or connect a call. You can: the phone "
    "system puts callers through to the store. Never announce a transfer "
    "yourself either — do not say you are connecting, transferring or putting "
    "someone through, because the phone system has already done it before you "
    "speak. If a caller wants a person and is still on the line, offer a "
    "callback from the store. "
    "Never state a price, a monthly payment or a dollar amount unless it "
    "appears word for word in the verified pricing blocks below. If you do "
    "not have it, say the store will confirm the exact price. "
    "Pronunciation: always say the website as 'cellular solution dot c a' — "
    "two words, cellular and solution. Never say 'w w w' or 'triple w'. "
    "Never repeat a previous reply, in whole or in part. Say the website "
    "directions once when the topic first comes up; after that, just "
    "offer the callback. "
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

# SMS replies are plain text, not speech: the website stays
# "cellularsolution.ca" verbatim — the "dot c a" pronunciation rewrite in
# _speakable_text is voice-only and is never applied to SMS.
SMS_SYSTEM_PROMPT = (
    "You are Cellular Solution's automated SMS assistant, a Bell authorized "
    "dealer at 81 Zenway Blvd, Unit 6, Vaughan, Ontario. Store hours are "
    "Monday to Friday, 9am to 5pm. Store phone: 905-264-2228. "
    "Always disclose in your first reply that you are Cellular Solution's "
    "automated AI assistant, and combine that disclosure with a helpful answer. "
    "Your job is to qualify interest in Bell mobility and the Bell Employee "
    "Purchase Program (EPP): up to 30% off Bell mobility plans and no "
    "activation fees. EPP is for employees of select partner companies, not "
    "only Bell employees. Never say someone must work for Bell or ask if they "
    "are a Bell employee. To check eligibility, tell them to visit "
    "cellularsolution.ca, click Employee Offers, and type in their company "
    "name. Proof of employment is shown after ordering. "
    "If they ask for an 'EPP department' or a person to speak to, never say "
    "we do not have EPP: there is no separate department — EPP is handled by "
    "this assistant and by the store at 905-264-2228. Offer to check "
    "eligibility at cellularsolution.ca or a store callback. "
    "Always write the website as cellularsolution.ca (never write 'dot c a'). "
    "Answer basic questions about EPP. NEVER invent or promise a specific "
    "price, plan detail, phone model price, or eligibility answer you cannot "
    "verify; say the store will confirm it. "
    "You do NOT have the list of partner companies; if someone names their "
    "company, never say whether it is eligible or not — direct them to "
    "cellularsolution.ca or offer a callback from the store. "
    "Never close a sale and never take an order. "
    "When the person is interested, offer a callback from the store (or they "
    "can call 905-264-2228) and collect their name and phone number. "
    "Keep every reply short: 1 to 3 sentences, plain text suitable for SMS. "
    "No markdown, no bullet points, no lists, no asterisks."
)

# Verified EPP offers are owner-supplied data (Bell pricing grids), injected at
# request time via RELAY_EPP_HARDWARE_OFFERS / RELAY_EPP_PLAN_OFFERS (e.g. as
# Railway service vars). Empty = no offers block = prompts byte-identical to
# the pre-grid behaviour, so unsetting the vars is a clean rollback.
EPP_HARDWARE_OFFERS = ""
EPP_PLAN_OFFERS = ""

EPP_OFFERS_GUARDRAIL = (
    "VERIFIED OFFERS GUARDRAILS: The VERIFIED OFFERS blocks below are the "
    "ONLY pricing you may quote. Quote exactly, with the as-of date. Never "
    "compute, estimate, or apply discounts yourself. Never invent a "
    "price/plan/device not in a block. If asked about something not "
    "listed, say the store will confirm it."
)


def with_offers(base: str) -> str:
    """Append non-empty verified EPP offer blocks (hardware and/or rate plans)
    plus the quoting guardrails to a base system prompt. Read per request so
    Railway variable updates apply live without code changes."""
    hw = _env_or("RELAY_EPP_HARDWARE_OFFERS", EPP_HARDWARE_OFFERS).strip()
    plans = _env_or("RELAY_EPP_PLAN_OFFERS", EPP_PLAN_OFFERS).strip()
    if not hw and not plans:
        return base
    parts = [base, EPP_OFFERS_GUARDRAIL]
    if hw:
        parts.append(f"VERIFIED HARDWARE OFFERS:\n{hw}")
    if plans:
        parts.append(f"VERIFIED RATE PLAN OFFERS:\n{plans}")
    return "\n\n".join(parts)


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
    # Deliberately a dead end, so the dead-end guard can be exercised
    # end-to-end. Only reachable in MOCK_LLM mode.
    "deadend": "Okay.",
    # Report whether the one-shot interrupt note reached the model.
    "note_seen": "Yes, I can see the interrupt note in the history.",
    "note_missing": "No interrupt note was found in the history.",
    # A pitch, so the phase swap can be exercised end-to-end. Only reachable
    # in MOCK_LLM mode.
    "pitch": (
        "Bell's Employee Purchase Program, or EPP, gives employees of select "
        "partner companies up to 30 percent off Bell mobility plans and waives "
        "activation fees. To check if your company is eligible, visit cellular "
        "solution dot c a, click Employee Offers, and type in your company "
        "name. Would you like the store to call you back?"
    ),
    # A question the caller cannot answer, so the guard can be exercised
    # end-to-end. Only reachable in MOCK_LLM mode.
    "badq": "Are you with a company that offers EPP?",
    # The same question AFTER a sentence that commits the pre-speech gate, so
    # only the chunk scan can catch it. Only reachable in MOCK_LLM mode.
    "badq_late": (
        "Sure, I can help with that. Are you with a company that offers EPP?"
    ),
    # The designed post-pitch answer to "what's the call about?". Deliberately
    # free of the pitch's own claims ("30 percent", "activation fee") so
    # _pitch_delivered does not read it as a re-pitch. Only reachable in
    # MOCK_LLM mode.
    "about": (
        "This call is about Bell's Employee Purchase Program for your company. "
        "Would you like the store to call you back about it?"
    ),
    # Report which outbound prompt phase the model is being given.
    "pitch_seen": "Yes, the post-pitch prompt is in place.",
    "pitch_missing": "No, the pre-pitch prompt is still in place.",
    # Name-capture probes. Only reachable in MOCK_LLM mode.
    "askname": "Sure — what's your first name?",
    "sttname": "Sorry, what's your first name?",
    "gotnumber": "Got it, 416-000-1111. What's your first name?",
    # A bare sign-off, so the closing exemption in the repeat guard can be
    # exercised end-to-end. Only reachable in MOCK_LLM mode.
    "bye": "Goodbye.",
    # A normal reply that CONTAINS sign-off vocabulary without being one, so the
    # close trigger's false-positive path can be exercised end-to-end. Only
    # reachable in MOCK_LLM mode.
    "takecare": "I can take care of that for you.",
    # A fabricated claim about a colleague's availability, so the availability
    # guard can be exercised end-to-end. The claim is the FIRST sentence, so
    # the pre-speech gate holds it. Only reachable in MOCK_LLM mode.
    "availref": (
        "Ed isn't available right now. Would you like the store to call you "
        "back about Bell's Employee Purchase Program?"
    ),
    # The same claim AFTER a sentence that commits the pre-speech gate, so only
    # the chunk scan can catch it. Only reachable in MOCK_LLM mode.
    "availref_late": (
        "Would you like the store to call you back? Ed isn't available right now."
    ),
    # A denial of the relay's own transfer capability, so the transfer-denial
    # guard can be exercised end-to-end. The denial is the FIRST sentence, so
    # the pre-speech gate holds it. Only reachable in MOCK_LLM mode.
    "canttransferref": (
        "I can't transfer calls. Would you like the store to call you back "
        "about Bell's Employee Purchase Program?"
    ),
    # The same denial AFTER a sentence that commits the pre-speech gate, so
    # only the chunk scan can catch it. Only reachable in MOCK_LLM mode.
    "canttransferref_late": (
        "Would you like the store to call you back? I can't transfer calls."
    ),
    # A false transfer promise, so the promise guard can be exercised
    # end-to-end. The promise is the FIRST sentence, so the pre-speech gate
    # holds it. Only reachable in MOCK_LLM mode.
    "promiseref": (
        "I'm connecting you to the store at 905-264-2228 now. Is there "
        "anything else I can help with?"
    ),
    # The same promise AFTER a sentence that commits the pre-speech gate, so
    # only the chunk scan can catch it. Only reachable in MOCK_LLM mode.
    "promiseref_late": (
        "Would you like the store to call you back? I'm connecting you now."
    ),
    # An invented price, so the price guard can be exercised end-to-end. The
    # figure is the FIRST sentence, so the pre-speech gate holds it. Only
    # reachable in MOCK_LLM mode.
    "priceref": (
        "The cheapest Bell mobility plans start at about $35 a month with the "
        "Employee Purchase Program. Would you like the store to call you back?"
    ),
    # The same figure AFTER a sentence that commits the pre-speech gate, so
    # only the chunk scan can catch it. Only reachable in MOCK_LLM mode.
    "priceref_late": (
        "Would you like the store to call you back? Plans start at $35 a month."
    ),
}


def _mock_reply_for(history: list[dict]) -> str:
    """Pick a canned reply from the latest user message (MOCK_LLM mode)."""
    last_user = next(
        (m["content"] for m in reversed(history) if m["role"] == "user"),
        "",
    )
    lowered = last_user.lower()
    # Count probes first: "callbackdonecount" contains "callback", and the
    # name probes must not fall through to the generic replies.
    if "namereaskcount" in lowered:
        n = sum(
            1
            for m in history
            if m.get("role") == "system" and NAME_REASK_NOTE in (m.get("content") or "")
        )
        return f"reask_notes={n} in the history."
    if "giveupcount" in lowered:
        n = sum(
            1
            for m in history
            if m.get("role") == "system"
            and NAME_GIVE_UP_NOTE in (m.get("content") or "")
        )
        return f"giveup_notes={n} in the history."
    if "callbackdonecount" in lowered:
        n = sum(
            1
            for m in history
            if m.get("role") == "system"
            and "ALREADY arranged on this call" in (m.get("content") or "")
        )
        return f"callback_done_notes={n} in the history."
    if "askname" in lowered:
        return MOCK_REPLIES["askname"]
    if "curious" in lowered:
        # A stop-word from NAME_STOPWORDS, so the bare-name path refuses it —
        # the "Done." for "Don" case.
        return MOCK_REPLIES["sttname"]
    if "4160001111" in lowered:
        return MOCK_REPLIES["gotnumber"]
    if "byeref" in lowered:
        return MOCK_REPLIES["bye"]
    if "availref_late" in lowered:
        return MOCK_REPLIES["availref_late"]
    if "availref" in lowered:
        return MOCK_REPLIES["availref"]
    if "canttransferref_late" in lowered:
        return MOCK_REPLIES["canttransferref_late"]
    if "canttransferref" in lowered:
        return MOCK_REPLIES["canttransferref"]
    if "promiseref_late" in lowered:
        return MOCK_REPLIES["promiseref_late"]
    if "promiseref" in lowered:
        return MOCK_REPLIES["promiseref"]
    # Before the "price" probe: "priceref" contains "price".
    if "priceref_late" in lowered:
        return MOCK_REPLIES["priceref_late"]
    if "priceref" in lowered:
        return MOCK_REPLIES["priceref"]
    if "takecareref" in lowered:
        return MOCK_REPLIES["takecare"]
    if "pitchref" in lowered:
        swapped = any(
            m.get("role") == "system"
            and "ALREADY given the EPP pitch" in (m.get("content") or "")
            for m in history
        )
        return MOCK_REPLIES["pitch_seen"] if swapped else MOCK_REPLIES["pitch_missing"]
    if "pitchme" in lowered:
        return MOCK_REPLIES["pitch"]
    if "aboutref" in lowered:
        return MOCK_REPLIES["about"]
    if "badqlate" in lowered:
        return MOCK_REPLIES["badq_late"]
    if "badq" in lowered:
        return MOCK_REPLIES["badq"]
    if "noteref" in lowered:
        seen = any(
            m.get("role") == "system"
            and "interrupted you mid-sentence" in (m.get("content") or "")
            for m in history
        )
        return MOCK_REPLIES["note_seen"] if seen else MOCK_REPLIES["note_missing"]
    if "deadend" in lowered:
        return MOCK_REPLIES["deadend"]
    if "callback" in lowered or "call me back" in lowered:
        return MOCK_REPLIES["callback"]
    if "price" in lowered or "cost" in lowered or "much" in lowered:
        return MOCK_REPLIES["price"]
    return MOCK_REPLIES["default"]


# In-memory conversation history per callSid, so a dropped WebSocket can
# reconnect (applicationReconnect=true) and pick up where it left off.
SESSIONS: dict[str, list[dict]] = {}
# Per-call state that is NOT conversation history: the callback details the
# caller has already dictated, and the flags that decide whether a notice or
# a name prompt is still owed. These used to be locals of the WebSocket
# handler, so a reconnect silently reset them: a caller who had already given
# their name and number lost them (no CALLBACK_REQUEST, no store SMS, and a
# handoff payload with no callback), and an outbound call could repeat the
# recording notice. Kept beside SESSIONS and restored on reconnect.
CALL_STATE: dict[str, dict] = {}

# Calls whose relay session is ending because the agent signed off. Kept apart
# from CALL_STATE on purpose: CALL_STATE is copied into the handler's `state`
# on a reconnect and re-saved every turn, so a flag stored there would be
# resurrected on every later turn and could suppress a real handoff. This set
# is written before the goodbye finishes playing and discarded when read, so a
# socket drop mid-goodbye hangs up instead of dialling the store, and nothing
# else is affected.
CLOSE_PENDING: set[str] = set()
MAX_SESSIONS = 100


def new_call_state() -> dict:
    """Fresh per-call state for a new (non-reconnect) session."""
    return {
        "callback_name": None,
        "callback_phone": None,
        "callback_logged": False,
        "phone_buffer": "",
        "phone_ambiguous": False,
        "expecting_name": False,
        "name_attempts": 0,
        "callback_offered": False,
        "recording_disclosed": False,
        "caller_turns": 0,
        "interrupted": False,
        "recent_replies": [],
        "pitch_delivered": False,
        # The permission phase: the greeting asked for a minute and the caller
        # has not answered yet. `pitch_skipped` means the phase is over without
        # a pitch (declined, or never consented), so the gate stays shut.
        "permission_reasks": 0,
        "pitch_skipped": False,
    }

# Voice history per call keeps only the system prompt plus the most recent
# turns, so each Mistral request stays small and fast on long calls.
MAX_HISTORY_MESSAGES = 20

# One keep-alive HTTP client per event loop, reused across turns: reconnecting
# to Mistral (hosted in the EU) on every reply would add a full TLS round trip
# to every turn. Keyed by running loop so in-process tests (TestClient's anyio
# portal) never share a client across loops.
_MISTRAL_CLIENTS: dict[int, "httpx.AsyncClient"] = {}


def _mistral_client() -> "httpx.AsyncClient":
    try:
        key = id(asyncio.get_running_loop())
    except RuntimeError:
        key = 0
    client = _MISTRAL_CLIENTS.get(key)
    if client is None or client.is_closed:
        client = httpx.AsyncClient(
            timeout=60.0,
            limits=httpx.Limits(max_keepalive_connections=5, keepalive_expiry=30.0),
        )
        _MISTRAL_CLIENTS[key] = client
    return client

# In-memory SMS history per sender (From number), capped like SESSIONS.
SMS_SESSIONS: dict[str, list[dict]] = {}
MAX_SMS_SESSIONS = 100
MAX_SMS_HISTORY_MESSAGES = 20

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


def _twilio_auth_token() -> str:
    """Resolve the auth token per request, so runtime env changes (tests,
    Railway var edits) apply without an import-time re-read."""
    return os.environ.get("TWILIO_AUTH_TOKEN") or TWILIO_AUTH_TOKEN


# ---------------------------------------------------------------------------
# Callback capture (best effort, V1).
# ---------------------------------------------------------------------------
PHONE_RE = re.compile(
    r"(?<!\d)(?:\+?1[\s.\-]?)?(?:\(?(\d{3})\)?[\s.\-]?)(\d{3})[\s.\-]?(\d{4})(?!\d)"
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
    # "I'm <state>" replies are not names. A live call captured "Listening"
    # as the caller's name from "I'm listening.", and the store would have
    # been sent a callback request for a person who does not exist.
    "listening", "still", "there", "fine", "good", "okay", "ok", "alright",
    "well", "right", "correct", "sure", "waiting", "back", "done", "ready",
    "speaking", "talking", "going", "doing", "having", "checking", "asking",
    "saying", "telling", "actually", "currently", "now", "home", "busy",
    "available", "curious", "confused", "satisfied", "done",
}
NAME_RES = [
    re.compile(r"\bmy name is ([a-z]+(?: [a-z]+)?)", re.IGNORECASE),
    re.compile(r"\bthis is ([a-z]+(?: [a-z]+)?)", re.IGNORECASE),
    re.compile(r"\bI'?m ([a-z]+(?: [a-z]+)?)", re.IGNORECASE),
    re.compile(r"\bname'?s ([a-z]+(?: [a-z]+)?)", re.IGNORECASE),
]

# Name-capture reconciliation. A live call (CA2903) heard "Done." for "Don":
# the model accepted it as the name and moved on, but the bare-name path
# refuses stop-words, so state["callback_name"] stayed None and the
# CALLBACK_REQUEST block never fired — no log event, no store SMS. The relay
# now tells the model what it actually captured, the same way the phone notes
# do. Bounded so a persistent STT error cannot loop forever.
MAX_NAME_ATTEMPTS = 2

NAME_REASK_NOTE = (
    "The caller's reply did not contain a usable first name. Ask for their "
    "first name again, briefly, as one short question."
)

NAME_GIVE_UP_NOTE = (
    "The caller has not given a usable first name after repeated asks. Do NOT "
    "ask for the name again. Confirm the callback using the phone number you "
    "already have, and say the store will ask for their name when they call "
    "back."
)

# The caller asks for a person ("representative", "agent", "a real person").
# Matching this hands the call to the store instead of answering.
HANDOFF_RE = re.compile(
    r"\b(representative|agent|human|operator|real person|live person|"
    r"talk to (a |an )?person|speak to (a |an )?(person|someone)|someone real)\b",
    re.IGNORECASE,
)

# Where a handoff lands. The store's main line is one number; every
# destination is a DTMF extension on it, dialled with <Number sendDigits>.
# Order matters: a name beats a department, so "I need Ed in accounts payable"
# is a call for Ed. The last entry is the default — what "can I talk to a real
# person" means on this line — and is also what an outbound handoff uses,
# because a prospect who asks for a person wants the EPP desk, not a named
# staff member.
#
# The accounts patterns are deliberately narrow. "billing" and "payment" are
# NOT here: a sales prospect asking "what's the monthly payment?" must not be
# routed to accounts receivable.
HANDOFF_ROUTES = (
    (
        "226",
        "Neelam",
        re.compile(
            r"\b(?:neelam|neelum|neel|neela|nila|noom|numb|nm)\b", re.IGNORECASE
        ),
    ),
    (
        "232",
        "Dwayne",
        re.compile(r"\b(?:dwayne|dwain|duane|dwane|dwayn)\b", re.IGNORECASE),
    ),
    ("231", "Ed", re.compile(r"\b(?:ed|eid)\b", re.IGNORECASE)),
    (
        "220",
        "accounts receivable",
        re.compile(
            r"\b(accounts? (receivable|payable)|receivables?|payables?|"
            r"a\s*/\s*r|a\s*/\s*p|invoic(?:e|es|ing))\b",
            re.IGNORECASE,
        ),
    ),
)
DEFAULT_HANDOFF_ROUTE = ("225", "our Bell EPP desk")

# A pause before the extension digits. Twilio sends "w" as half a second, and
# the store's auto-attendant answers with a long recorded greeting before it
# will take digits. Measured on the live line (2026-11-13): the greeting runs
# "Thank you for calling Cellular Solution. We look forward to answering your
# call. If you know the extension you are trying to reach, you may dial it at
# any time during this message. Press 1 ... Press 4 for the company directory."
# Digits sent at 5s land inside the first sentence and are DISCARDED — the
# caller stays on the main line. Digits sent at 14.5s are accepted and the
# call transfers. So the pause has to clear the opening sentence.
#
# Twilio caps sendDigits at 32 characters, so 29 halves (14.5s) is the longest
# pause that still leaves room for a 3-digit extension. That is the default.
# RELAY_HANDOFF_DIGIT_PAUSE overrides it in half-seconds, clamped 0-29 and
# further capped so the whole string stays within 32 characters.
HANDOFF_DIGIT_PAUSE_HALVES = 29
HANDOFF_DIGIT_PAUSE = "w" * HANDOFF_DIGIT_PAUSE_HALVES

# Twilio's documented limit for <Number sendDigits>.
SEND_DIGITS_MAX = 32


def handoff_digit_pause(extension: str = "") -> str:
    """The DTMF lead-in pause, as Twilio "w" (half-second) characters.

    Read per request so the length can be tuned on Railway without a code
    change: how long the store's auto-attendant talks before it accepts
    digits is not something this repo can know. A value that is not an
    integer falls back to the default rather than dialling with no pause.

    Capped so the pause plus the extension never exceeds Twilio's 32-character
    sendDigits limit — an over-long string is rejected and the caller is left
    on the main line, which is the failure this pause exists to prevent.
    """
    raw = os.environ.get("RELAY_HANDOFF_DIGIT_PAUSE", "").strip()
    halves = HANDOFF_DIGIT_PAUSE_HALVES
    if raw:
        try:
            halves = int(raw)
        except ValueError:
            halves = HANDOFF_DIGIT_PAUSE_HALVES
    halves = max(0, min(HANDOFF_DIGIT_PAUSE_HALVES, halves))
    halves = min(halves, max(0, SEND_DIGITS_MAX - len(extension or "")))
    return "w" * halves


# A caller asking to be transferred, whatever name they use. The named routes
# above catch the destinations the store publishes; this catches the rest
# ("transfer me to billing", "can you connect me") so the caller is connected
# to the store instead of being told the agent cannot transfer. Deliberately
# requires "me"/"us"/"this call" after the verb: "can I transfer my number to
# Bell?" is a port-in question, not a request to be put through.
TRANSFER_REQUEST_RE = re.compile(
    r"\b(?:transfer|connect|put)\s+(?:me|us)\b|\btransfer\s+(?:this|the)\s+call\b",
    re.IGNORECASE,
)


def handoff_route(user_text: str, is_outbound: bool = False) -> tuple[str, str] | None:
    """The (extension, spoken label) for this turn, or None to keep talking.

    Returns a route when the caller asked for a person or for one of the
    destinations above; None means the model should answer normally. Outbound
    calls always use the default: the caller is a prospect, and a stray name in
    their sentence is not a request to be transferred to that person.
    """
    if is_outbound:
        return DEFAULT_HANDOFF_ROUTE if HANDOFF_RE.search(user_text or "") else None
    t = user_text or ""
    for extension, label, pattern in HANDOFF_ROUTES:
        if pattern.search(t):
            return extension, label
    if HANDOFF_RE.search(t):
        return DEFAULT_HANDOFF_ROUTE
    if TRANSFER_REQUEST_RE.search(t):
        return DEFAULT_HANDOFF_ROUTE
    return None


def _truncate_to_utterance(spoken: str, utterance: str) -> str:
    """Cut a spoken reply down to what the caller actually heard.

    On an interrupt, Twilio sends utteranceUntilInterrupt — the caller's
    words up to the point they cut in. Keeping the full reply in history
    would make Mistral believe it said things the caller never heard (a
    price, a disclosure). If the utterance is not found verbatim (it can be
    truncated mid-word), match its longest prefix that appears in the
    spoken text; if nothing matches, the caller heard none of it.
    """
    if not utterance or not utterance.strip():
        return spoken
    u = utterance.strip().lower()
    s = spoken.lower()
    idx = s.find(u)
    if idx != -1:
        return spoken[: idx + len(u)]
    for end in range(len(u), 0, -1):
        idx = s.find(u[:end])
        if idx != -1:
            return spoken[: idx + end]
    return ""


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
    if MOCK_LLM or os.environ.get("MOCK_LLM", "") == "1":
        reply = _mock_reply_for(history)
        # Yield word by word so the streaming path is exercised. The pitch
        # streams slower so a test can interrupt it mid-sentence, the way a
        # caller cuts in after hearing most of it.
        delay = 0.03 if reply == MOCK_REPLIES["pitch"] else 0.01
        for word in reply.split(" "):
            yield word + " "
            await asyncio.sleep(delay)
        return

    if not MISTRAL_API_KEY:
        raise RuntimeError("MISTRAL_API_KEY is not set")

    payload = {
        "model": MISTRAL_MODEL,
        "messages": history,
        # Low temperature: prices and disclosures must come out identical on
        # every call, not creatively reworded.
        "temperature": 0.3,
        # 200 tokens covers a 1-3 sentence spoken reply (mistral-small emits
        # no "thinking" blocks; mistral-large-4 needed 1000+ because its
        # thinking alone could eat the whole budget before any answer text).
        "max_tokens": 200,
        "stream": True,
    }
    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
    }
    client = _mistral_client()
    async with client.stream(
        "POST", MISTRAL_URL, json=payload, headers=headers
    ) as response:
        if response.status_code >= 400:
            # Log the provider's own error body (model retired, bad param, ...)
            # so a 400 is diagnosable from the relay logs alone.
            body = (await response.aread()).decode("utf-8", "replace")
            raise RuntimeError(
                f"Mistral HTTP {response.status_code} for model {MISTRAL_MODEL!r}: "
                f"{body[:400]}"
            )
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


async def mistral_sms_reply(history: list[dict]) -> str:
    """Non-streaming Mistral reply for an inbound SMS (TwiML <Message> body).

    SMS has no token-by-token playback, so the whole reply is fetched in
    one request and returned as a single string.
    """
    if MOCK_LLM or os.environ.get("MOCK_LLM", "") == "1":
        return _mock_reply_for(history)

    if not MISTRAL_API_KEY:
        raise RuntimeError("MISTRAL_API_KEY is not set")

    payload = {
        "model": MISTRAL_MODEL,
        "messages": history,
        "temperature": 0.5,
        "max_tokens": 300,
        "stream": False,
    }
    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json",
    }
    client = _mistral_client()
    response = await client.post(MISTRAL_URL, json=payload, headers=headers, timeout=30.0)
    if response.status_code >= 400:
        raise RuntimeError(
            f"Mistral HTTP {response.status_code} for model {MISTRAL_MODEL!r}: "
            f"{response.text[:400]}"
        )
    chunk = response.json()
    return _content_to_text(
        chunk.get("choices", [{}])[0].get("message", {}).get("content")
    )


# ---------------------------------------------------------------------------
# HTTP endpoints.
# ---------------------------------------------------------------------------
@app.get("/")
async def root() -> dict:
    return {"status": "ok", "service": "cellular-solution-relay"}


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


def conversation_relay_xml(ws_url: str) -> str:
    """Build the <Connect><ConversationRelay> TwiML.

    No welcomeGreeting attribute: the relay speaks the greeting itself as a
    non-interruptible token on setup, so a caller saying "hello" cannot cut
    the opening off mid-sentence (the TwiML attribute is interruptible)."""
    # ttsProvider/voice are only included when explicitly configured:
    # Twilio rejected every explicit Google voice tried for en-CA
    # (en-CA-Standard-A and Google.en-CA-Neural2-A both → error 64101),
    # so the voice now falls back to Twilio's account defaults.
    tts_attrs = ""
    if RELAY_TTS_PROVIDER:
        tts_attrs += f'ttsProvider="{RELAY_TTS_PROVIDER}" '
    if RELAY_TTS_VOICE:
        tts_attrs += f'voice="{RELAY_TTS_VOICE}" '
    # <Connect> action URL: when the ConversationRelay session ends — the relay
    # sends "end" for a human handoff, or the WebSocket drops — Twilio POSTs
    # here and the call continues from /handoff (dials the store) instead of
    # dying in silence.
    action_url = ws_url.replace("wss://", "https://").replace("ws://", "http://")
    if action_url.endswith("/ws"):
        action_url = action_url[: -len("/ws")] + "/handoff"
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<Response><Connect action="{escape_attr(action_url)}">'
        f'<ConversationRelay url="{ws_url}" '
        f'language="{RELAY_LANGUAGE}" '
        f"{tts_attrs}"
        f'transcriptionProvider="{RELAY_STT_PROVIDER}" '
        'speechModel="telephony" '
        'interruptible="any" '
        # "uh-huh"/"yeah" must not cut the agent off mid-sentence.
        'ignoreBackchannel="true" '
        # Keypad entry (PINs, "press 0 for a person") arrives as dtmf messages.
        'dtmfDetection="true" '
        'applicationReconnect="true" '
        f'hints="{escape_attr(_env_or("RELAY_HINTS", DEFAULT_HINTS))}" />'
        "</Connect></Response>"
    )


async def _twiml_response(request: Request) -> Response:
    token = _twilio_auth_token()
    if request.method == "POST" and token:
        body = await request.body()
        # keep_blank_values: Twilio signs every POST param it sends, INCLUDING
        # empty-valued ones (e.g. ForwardedFrom=). Dropping blanks here would
        # change the signed data string and reject legitimate Twilio requests.
        params = dict(parse_qsl(body.decode("utf-8", "replace"), keep_blank_values=True))
        signature = request.headers.get("x-twilio-signature", "")
        url = public_http_url(request)
        if not signature or not twilio_signature_valid(
            url, params, signature, token
        ):
            log_event("twiml_signature_rejected", url=url)
            return Response(content="Forbidden", status_code=403)
    elif request.method == "GET":
        log_event("twiml_unsigned_get", note="test fetch only; Twilio uses POST")

    host = request.headers.get("x-forwarded-host") or request.headers.get("host", "")
    ws_url = RELAY_WS_URL or f"wss://{host}/ws"
    return Response(
        content=conversation_relay_xml(ws_url),
        media_type="application/xml",
    )


@app.api_route("/twiml", methods=["GET", "POST"])
async def twiml(request: Request) -> Response:
    """TwiML for the Twilio number: connect the call to ConversationRelay.
    The greeting is chosen by direction in the WS setup and spoken by the
    relay as a non-interruptible token."""
    return await _twiml_response(request)


@app.api_route("/twiml-outbound", methods=["GET", "POST"])
async def twiml_outbound(request: Request) -> Response:
    """TwiML for outbound dials: same relay XML; the setup message's
    direction=outbound-api makes the relay use the outbound greeting/prompt."""
    return await _twiml_response(request)


# CallStatus values that mean there is no caller left to connect. <Connect>'s
# action URL fires on a plain hangup as well as on a handoff, so these are the
# cases where dialling the store would ring a live line for nobody. "ringing"
# is here because a leg that never connected has no caller on it either.
NO_CALLER_STATUSES = frozenset(
    {"completed", "busy", "failed", "no-answer", "canceled", "cancelled", "ringing"}
)


@app.post("/handoff")
async def handoff_action(request: Request) -> Response:
    """Action URL of <Connect>: fires when the relay session ends (human
    handoff via the "end" message, or a dropped WebSocket). Connects the
    caller to the store line so they always reach a person."""
    token = _twilio_auth_token()
    body = await request.body()
    params = dict(parse_qsl(body.decode("utf-8", "replace"), keep_blank_values=True))
    if token:
        signature = request.headers.get("x-twilio-signature", "")
        url = public_http_url(request)
        if not signature or not twilio_signature_valid(url, params, signature, token):
            log_event("handoff_signature_rejected", url=url)
            return Response(content="Forbidden", status_code=403)
    handoff_data = (params.get("HandoffData") or "").strip()
    call_status = (params.get("CallStatus") or "").strip().lower()
    # Take the close intent here, before any early return: every path through
    # this endpoint ends the call, so leaving the entry behind would leak a
    # CallSid and could hang up a later call that reused it.
    close_pending = params.get("CallSid", "") in CLOSE_PENDING
    CLOSE_PENDING.discard(params.get("CallSid", ""))
    log_event(
        "handoff_action",
        callSid=params.get("CallSid", ""),
        callStatus=call_status or None,
        handoffData=handoff_data[:500],
    )
    # A plain hangup lands here too: the action URL fires whenever the session
    # ends, including when the caller simply hangs up. Dialling the store then
    # rings a live line for a caller who is already gone, and the store picks
    # up silence. Only dial when the relay asked for a handoff (HandoffData) or
    # the call is still up; an absent CallStatus keeps the old behaviour.
    if not handoff_data and call_status in NO_CALLER_STATUSES:
        log_event(
            "handoff_skipped_call_ended",
            callSid=params.get("CallSid", ""),
            callStatus=call_status,
        )
        return Response(content=EMPTY_RESPONSE_XML, media_type="application/xml")
    # Parse once: the reason decides whether to dial at all, and the route
    # decides where. Both ride in the same HandoffData JSON.
    parsed = {}
    if handoff_data:
        try:
            parsed = json.loads(handoff_data)
        except ValueError:
            parsed = {}
        if not isinstance(parsed, dict):
            parsed = {}
    # The agent signed off, so the conversation is over. Hanging up is the
    # whole point: dialling the store would put a caller who just said goodbye
    # through to a live line, and the caller would hear the store answer a call
    # they had already ended. Keyed on `action`, not `reason`: every close
    # reason must hang up, and a new reason must not silently start dialling.
    if str(parsed.get("action") or "") == "close":
        log_event(
            "handoff_skipped_closed",
            callSid=params.get("CallSid", ""),
            reason=parsed.get("reason"),
        )
        return Response(content=EMPTY_RESPONSE_XML, media_type="application/xml")
    # A socket drop during the goodbye leaves HandoffData empty. The relay
    # records the close intent before waiting out playback, so the store is not
    # dialled for a caller who was being hung up on.
    if not parsed and close_pending:
        log_event(
            "handoff_skipped_closed",
            callSid=params.get("CallSid", ""),
            reason="socket-dropped",
        )
        return Response(content=EMPTY_RESPONSE_XML, media_type="application/xml")
    store = os.environ.get("RELAY_STORE_NUMBER", "").strip() or "+19052642228"
    # The relay chose an extension for this caller (a named staff member, the
    # accounts desk, or the EPP desk by default). It arrives as JSON in
    # HandoffData, which is caller-influenced only through the relay's own
    # routing table — but it is still validated before it reaches TwiML, so a
    # malformed or hostile payload can never inject markup or dial digits.
    extension = ""
    label = ""
    route = parsed.get("route")
    if isinstance(route, dict):
        candidate = str(route.get("extension") or "").strip()
        if candidate.isdigit() and 1 <= len(candidate) <= 6:
            extension = candidate
            label = str(route.get("label") or "").strip()[:40]
    if extension:
        number = (
            f'<Number sendDigits="{handoff_digit_pause(extension)}{extension}">'
            f"{_xml_escape(store)}</Number>"
        )
        say = f"Connecting you to {_xml_escape(label)} now." if label else (
            "Connecting you to our store now."
        )
    else:
        number = f"<Number>{_xml_escape(store)}</Number>"
        say = "Connecting you to our store now."
    log_event(
        "handoff_dial",
        callSid=params.get("CallSid", ""),
        extension=extension or None,
        label=label or None,
    )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        "<Response>"
        f"<Say>{say}</Say>"
        f'<Dial timeout="30">{number}</Dial>'
        "</Response>"
    )
    return Response(content=xml, media_type="application/xml")


@app.post("/amd")
async def amd_status(request: Request) -> Response:
    """AsyncAmdStatusCallback for outbound calls: Twilio POSTs the AMD result
    (AnsweredBy=human|machine_*|fax|unknown) here a few seconds into the
    call. Logged only — the batch poll loop hangs up machine-answered calls.
    Always returns an empty 200 so Twilio is not retried."""
    token = _twilio_auth_token()
    body = await request.body()
    params = dict(parse_qsl(body.decode("utf-8", "replace"), keep_blank_values=True))
    if token:
        signature = request.headers.get("x-twilio-signature", "")
        url = public_http_url(request)
        if not signature or not twilio_signature_valid(url, params, signature, token):
            log_event("amd_signature_rejected", url=url)
            return Response(content="Forbidden", status_code=403)
    log_event(
        "amd_result",
        callSid=params.get("CallSid", ""),
        answeredBy=params.get("AnsweredBy", ""),
    )
    return Response(content=EMPTY_RESPONSE_XML, media_type="application/xml")


# ---------------------------------------------------------------------------
# SMS: inbound webhook + outbound send.
# ---------------------------------------------------------------------------
def message_twiml_xml(body: str) -> str:
    """Build the <Response><Message> TwiML for an SMS reply."""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f"<Response><Message>{_xml_escape(body)}</Message></Response>"
    )


EMPTY_RESPONSE_XML = '<?xml version="1.0" encoding="UTF-8"?><Response></Response>'


async def send_sms(to: str, body: str) -> dict | None:
    """Send an outbound SMS via the Twilio Messages API.

    Env-gated: needs TWILIO_ACCOUNT_SID, TWILIO_FROM and either
    TWILIO_AUTH_TOKEN or the TWILIO_API_KEY_SID/SECRET pair. When any are
    missing, returns None and the caller stays logs-only. Never raises —
    an SMS failure must not break a voice call.
    """
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
    from_number = os.environ.get("TWILIO_FROM", "").strip()
    api_key_sid = os.environ.get("TWILIO_API_KEY_SID", "").strip()
    api_key_secret = os.environ.get("TWILIO_API_KEY_SECRET", "").strip()
    auth_token = _twilio_auth_token().strip()
    if api_key_sid and api_key_secret:
        auth = f"{api_key_sid}:{api_key_secret}"
    elif account_sid and auth_token:
        auth = f"{account_sid}:{auth_token}"
    else:
        auth = ""
    if not account_sid or not auth or not from_number or not to:
        return None
    url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"
    headers = {
        "Authorization": "Basic " + base64.b64encode(auth.encode()).decode(),
    }
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.post(
                url,
                data={"To": to, "From": from_number, "Body": body},
                headers=headers,
            )
        result = response.json() if response.content else {}
        if 200 <= response.status_code < 300:
            log_event("sms_sent", to=to, sid=result.get("sid"), status=result.get("status"))
            return result
        log_event("sms_send_error", to=to, http=response.status_code, error=str(result)[:300])
        return None
    except Exception as exc:  # noqa: BLE001 - never break the call flow
        log_event("sms_send_error", to=to, error=str(exc)[:300])
        return None


@app.post("/sms")
async def sms_inbound(request: Request) -> Response:
    """TwiML for inbound SMS on the Twilio number: the Mistral reply as a
    <Message>. POST-only, so a plain GET can never trigger an LLM call."""
    token = _twilio_auth_token()
    body = await request.body()
    params = dict(parse_qsl(body.decode("utf-8", "replace"), keep_blank_values=True))
    if token:
        signature = request.headers.get("x-twilio-signature", "")
        url = public_http_url(request)
        if not signature or not twilio_signature_valid(url, params, signature, token):
            log_event("sms_signature_rejected", url=url)
            return Response(content="Forbidden", status_code=403)
    else:
        logger.warning("TWILIO_AUTH_TOKEN is not set: SMS signature validation is OFF.")

    from_number = (params.get("From") or "").strip() or "unknown"
    to_number = (params.get("To") or "").strip()
    body_text = (params.get("Body") or "").strip()
    log_event("sms_inbound", from_=from_number, to=to_number, body=body_text)

    if not body_text:
        return Response(content=EMPTY_RESPONSE_XML, media_type="application/xml")

    history = SMS_SESSIONS.get(from_number)
    if history is None:
        history = [{"role": "system", "content": with_offers(_env_or("RELAY_SMS_SYSTEM_PROMPT", SMS_SYSTEM_PROMPT))}]
        SMS_SESSIONS[from_number] = history
        if len(SMS_SESSIONS) > MAX_SMS_SESSIONS:
            SMS_SESSIONS.pop(next(iter(SMS_SESSIONS)))
    else:
        # Re-read the prompt every turn so a Railway var change applies live.
        history[0]["content"] = with_offers(_env_or("RELAY_SMS_SYSTEM_PROMPT", SMS_SYSTEM_PROMPT))
    history.append({"role": "user", "content": body_text})
    while len(history) > 1 + MAX_SMS_HISTORY_MESSAGES:
        del history[1]

    try:
        reply = await mistral_sms_reply(history)
    except Exception as exc:  # noqa: BLE001 - keep the SMS thread alive
        logger.exception("LLM error: %s", exc)
        log_event("sms_llm_error", from_=from_number, error=str(exc))
        reply = (
            "Sorry, I'm having trouble right now. "
            "The store can help you at 905-264-2228."
        )
    history.append({"role": "assistant", "content": reply})
    log_event("sms_outbound_reply", to=from_number, body=reply)
    return Response(content=message_twiml_xml(reply), media_type="application/xml")


# ---------------------------------------------------------------------------
# WebSocket: ConversationRelay session.
# ---------------------------------------------------------------------------
@app.websocket("/ws")
async def relay_ws(websocket: WebSocket) -> None:
    token = _twilio_auth_token()
    if token:
        signature = websocket.headers.get("x-twilio-signature", "")
        url = public_ws_url(websocket)
        if not signature or not twilio_signature_valid(
            url, {}, signature, token
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
    history: list[dict] = [{"role": "system", "content": with_offers(_env_or("RELAY_SYSTEM_PROMPT", SYSTEM_PROMPT))}]
    # Per-call state: the callback details the caller has dictated and the
    # flags for notices/name prompts still owed. Mutated in place and mirrored
    # into CALL_STATE after every turn, so a reconnect restores it instead of
    # starting from scratch. Keys are defined in new_call_state().
    state = new_call_state()
    llm_task: asyncio.Task | None = None
    # Accumulates the text already sent to Twilio for the in-flight reply, so
    # an interrupt can truncate history to what the caller actually heard.
    spoken_box = [""]
    # Outbound calls: recording notice fires on the first affirmative answer.
    is_outbound = False
    # Turn timing: set when a prompt arrives, read by send_text_tokens and the
    # reply path to log first_token / turn_done latencies per turn.
    t_prompt = 0.0
    # Set when Twilio reports an interruption. The acknowledgement is spoken
    # on the prompt that follows, not at the interrupt itself (the caller is
    # still mid-sentence then).
    interrupted = False
    ack_index = 0
    last_ack_at = 0.0
    # Rotates the clarifying question asked of a fragment turn, so a caller
    # whose line keeps clipping does not hear the same sentence twice.
    fragment_index = 0
    # Idle prompt: armed once the caller has spoken, so it can never talk over
    # the opening. At most IDLE_PROMPTS_MAX prompts per caller turn.
    caller_spoke = False
    idle_prompts = 0
    # When the caller will stop hearing the agent, estimated from the text
    # sent. The idle timer counts from here, not from send-complete.
    agent_speaking_until = 0.0
    # A message that arrived while the relay was waiting out playback. The main
    # loop must see it, so it is buffered rather than dropped.
    pending_message: dict | None = None
    # A receive that is already in flight. The websockets library forbids two
    # concurrent receives, so a receive started by wait_out_playback() is
    # handed to the main loop instead of being cancelled or abandoned: an
    # abandoned receive would swallow the next message and drop it.
    pending_recv: asyncio.Future | None = None
    speech_buffer = ""
    # What was last spoken: "reply", "idle", "ack", "notice", "opening",
    # "nudge" or "error". The interrupt acknowledgement only makes sense after
    # a reply — "Sure." answering a cut-off "Are you still there?" does not.
    last_speech_kind = ""

    async def send_speech(
        token: str, last: bool, interruptible: bool, kind: str
    ) -> float:
        """Send a text token and track how long the caller will still be
        hearing it. Every text send goes through here, so the idle timer can
        wait for playback instead of firing over the agent's own speech.
        Returns the estimated playback seconds when this closes an utterance.
        """
        nonlocal agent_speaking_until, speech_buffer, last_speech_kind
        await websocket.send_json(
            {
                "type": "text",
                "token": token,
                "last": last,
                "interruptible": interruptible,
                "preemptible": False,
            }
        )
        if token:
            speech_buffer += token
        if not last:
            return 0.0
        est = estimate_speech_seconds(speech_buffer)
        agent_speaking_until = time.monotonic() + est
        last_speech_kind = kind
        speech_buffer = ""
        return est

    def save_call_state() -> None:
        """Mirror the per-call state into CALL_STATE so a reconnect restores
        it. Called after every turn that can change it."""
        if call_sid:
            CALL_STATE[call_sid] = dict(state)

    async def do_close(reason: str = "caller-closed") -> None:
        """End the session because the conversation is over, not because the
        caller wants a person.

        /handoff is the action URL for both cases, so `action` is what tells
        them apart: it returns empty TwiML for a close, which hangs the call
        up. Dialling the store here would put a caller who just said goodbye
        through to a live line.
        """
        log_event("session_close", callSid=call_sid, reason=reason)
        await websocket.send_json(
            {
                "type": "end",
                "handoffData": json.dumps(
                    {"action": "close", "reason": reason, "callSid": call_sid}
                ),
            }
        )

    async def do_handoff(
        reason: str,
        extra: dict | None = None,
        route: tuple[str, str] | None = None,
    ) -> None:
        """End the relay session; Twilio then POSTs the action URL (/handoff)
        and the caller is connected to the store line.

        `route` is the (extension, label) the caller should land on. It rides
        in handoffData because /handoff is a separate HTTP request with no
        access to this session's state.
        """
        extension, label = route or DEFAULT_HANDOFF_ROUTE
        payload = {
            "reason": reason,
            "callSid": call_sid,
            "route": {"extension": extension, "label": label},
        }
        if state["callback_name"] or state["callback_phone"]:
            payload["callback"] = {
                "name": state["callback_name"],
                "phone": state["callback_phone"],
            }
        if extra:
            payload.update(extra)
        log_event(
            "handoff", callSid=call_sid, reason=reason, extension=extension, label=label
        )
        await websocket.send_json(
            {"type": "end", "handoffData": json.dumps(payload)}
        )

    async def send_text_tokens(
        reply_stream,
        spoken_box=None,
        prev_assistant="",
        recent_replies=(),
        closing=False,
        user_text="",
    ) -> tuple[str, float]:
        """Stream reply tokens to Twilio with a short holdback, normalizing
        each chunk for TTS before it is spoken; return the full reply text
        (raw, for history/logs) and the estimated seconds of audio still to
        play when the last token was sent.

        The holdback is 26 characters: the website ('cellularsolution.ca',
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
        # 26 chars: the longest spoken site form ("cellular solution dot c a"
        # + comma) is 26, so it always stays whole in the held-back tail.
        HOLD_BACK = 26
        full = ""
        pending = ""
        prev_ended_with_site = False
        # Pre-speech gate: a reply is held until it is past the point where it
        # could be a dead end, so "Okay." can be replaced BEFORE the caller
        # hears it. Rewriting `reply` after the stream had already been sent
        # only changed the log, not what was said.
        gate = ""
        committed = False

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
                if spoken_box is not None:
                    # What the caller actually heard so far, for interrupt
                    # truncation (utteranceUntilInterrupt).
                    spoken_box[0] += spoken
                await send_speech(spoken, False, True, "reply")

        # Silence nudge: if Mistral takes longer than RELAY_NUDGE_AFTER
        # seconds (default 3) to produce its first token, speak a short
        # "One moment..." line so the caller does not hear dead air.
        # The nudge is NOT part of `full`, so it never enters history.
        nudge_after = float(os.environ.get("RELAY_NUDGE_AFTER", "3") or 0)
        nudge_text = os.environ.get("RELAY_NUDGE_TEXT", "") or "One moment, "

        async def _next_token():
            try:
                return await it.__anext__()
            except StopAsyncIteration:
                return None

        it = reply_stream.__aiter__()
        try:
            if nudge_after > 0:
                # shield() keeps the pending __anext__ alive across the
                # timeout — cancelling it would kill the stream generator.
                nxt = asyncio.ensure_future(_next_token())
                try:
                    token = await asyncio.wait_for(
                        asyncio.shield(nxt), timeout=nudge_after
                    )
                except asyncio.TimeoutError:
                    log_event("nudge_sent", callSid=call_sid)
                    await send_speech(nudge_text, False, True, "nudge")
                    token = await nxt
            else:
                token = await _next_token()
            first_token_ms = None
            while token is not None:
                if token:
                    if first_token_ms is None:
                        first_token_ms = (time.monotonic() - t_prompt) * 1000
                        log_event(
                            "first_token", callSid=call_sid, ms=round(first_token_ms, 1)
                        )
                    full += token
                    if committed:
                        pending += token
                    else:
                        gate += token
                        if _gate_commits(gate, recent_replies):
                            committed = True
                            pending += gate
                            gate = ""
                    if len(pending) > HOLD_BACK:
                        hit = _forbidden_at(
                            spoken_box[0] if spoken_box is not None else "", pending
                        )
                        if hit is not None:
                            # A forbidden question is about to be spoken. Say
                            # the safe prefix, then the fallback, and drop the
                            # rest of the stream: the caller must never hear
                            # a question only the store can answer.
                            safe = pending[:hit]
                            pending = ""
                            if safe:
                                await _flush(safe)
                            log_event(
                                "forbidden_question_guard",
                                callSid=call_sid,
                                text=full.strip(),
                            )
                            full = CALLBACK_QUESTION_FALLBACK
                            await _flush(full)
                            break
                        avail = _availability_at(
                            spoken_box[0] if spoken_box is not None else "", pending
                        )
                        if avail is not None:
                            # A claim about a colleague's availability is about
                            # to be spoken. Same treatment: say the safe prefix,
                            # then the fallback, and drop the rest of the
                            # stream — the relay has no calendar access, so the
                            # claim is invented.
                            safe = pending[:avail]
                            pending = ""
                            if safe:
                                await _flush(safe)
                            log_event(
                                "availability_guard",
                                callSid=call_sid,
                                text=full.strip(),
                            )
                            full = AVAILABILITY_FALLBACKS[0]
                            await _flush(full)
                            break
                        denial = _transfer_denial_at(
                            spoken_box[0] if spoken_box is not None else "", pending
                        )
                        if denial is not None:
                            # A denial of the relay's own transfer capability is
                            # about to be spoken. Same treatment: say the safe
                            # prefix, then the fallback, and drop the rest.
                            safe = pending[:denial]
                            pending = ""
                            if safe:
                                await _flush(safe)
                            log_event(
                                "transfer_denial_guard",
                                callSid=call_sid,
                                text=full.strip(),
                            )
                            full = TRANSFER_FALLBACKS[0]
                            await _flush(full)
                            break
                        promise = _transfer_promise_at(
                            spoken_box[0] if spoken_box is not None else "", pending
                        )
                        if promise is not None:
                            # The model is announcing a transfer it cannot
                            # perform. Same treatment: say the safe prefix,
                            # then the fallback, and drop the rest.
                            safe = pending[:promise]
                            pending = ""
                            if safe:
                                await _flush(safe)
                            log_event(
                                "transfer_promise_guard",
                                callSid=call_sid,
                                text=full.strip(),
                            )
                            full = TRANSFER_FALLBACKS[0]
                            await _flush(full)
                            break
                        price = _invented_price_at(
                            spoken_box[0] if spoken_box is not None else "", pending
                        )
                        if price is not None:
                            # A dollar figure the offers blocks do not contain
                            # is invented. Same treatment.
                            safe = pending[:price]
                            pending = ""
                            if safe:
                                await _flush(safe)
                            log_event(
                                "invented_price_guard",
                                callSid=call_sid,
                                text=full.strip(),
                            )
                            full = PRICE_FALLBACKS[0]
                            await _flush(full)
                            break
                        emit, pending = _split_pending(pending)
                        await _flush(emit)
                token = await _next_token()
        except asyncio.CancelledError:
            raise
        if not committed:
            # The stream ended while the reply was still held: either it is a
            # repeat of a recent reply, or it is a dead end. Replace it before
            # anything is spoken.
            replacement = None
            repeated = next(
                (r for r in recent_replies if is_repeat(r, gate)), None
            )
            if claims_staff_availability(gate):
                log_event(
                    "availability_guard", callSid=call_sid, text=gate.strip()
                )
                replacement = AVAILABILITY_FALLBACKS[0]
                if any(
                    _norm_text(r) == _norm_text(replacement) for r in recent_replies
                ):
                    replacement = AVAILABILITY_FALLBACKS[1]
            elif FORBIDDEN_QUESTION_RE.search(gate):
                log_event(
                    "forbidden_question_guard", callSid=call_sid, text=gate.strip()
                )
                replacement = CALLBACK_QUESTION_FALLBACK
            elif TRANSFER_DENIAL_RE.search(gate):
                log_event(
                    "transfer_denial_guard", callSid=call_sid, text=gate.strip()
                )
                replacement = TRANSFER_FALLBACKS[0]
                if any(
                    _norm_text(r) == _norm_text(replacement) for r in recent_replies
                ):
                    replacement = TRANSFER_FALLBACKS[1]
            elif TRANSFER_PROMISE_RE.search(gate):
                log_event(
                    "transfer_promise_guard", callSid=call_sid, text=gate.strip()
                )
                replacement = TRANSFER_FALLBACKS[0]
                if any(
                    _norm_text(r) == _norm_text(replacement) for r in recent_replies
                ):
                    replacement = TRANSFER_FALLBACKS[1]
            elif invented_price(gate):
                log_event(
                    "invented_price_guard", callSid=call_sid, text=gate.strip()
                )
                replacement = PRICE_FALLBACKS[0]
                if any(
                    _norm_text(r) == _norm_text(replacement) for r in recent_replies
                ):
                    replacement = PRICE_FALLBACKS[1]
            elif repeated is not None:
                # A repeat in answer to a clear question is an ANSWER, not a
                # loop: the post-pitch prompt tells the model what to say when
                # the caller asks what the call is about, so the same sentence
                # twice is expected. A pitch-like repeat is still replaced —
                # that is the loop this guard was built for.
                if CLOSING_RE.search(gate):
                    # A sign-off is not content the caller has to hear only
                    # once. A live call (CA2e04) said "Goodbye." and then
                    # answered "Sorry, I didn't catch that" four times while
                    # the caller tried to hang up.
                    log_event(
                        "repeat_allowed_closing", callSid=call_sid, text=gate.strip()
                    )
                elif _caller_asked(user_text) and not _pitch_delivered(gate):
                    log_event(
                        "repeat_allowed_answer", callSid=call_sid, text=gate.strip()
                    )
                else:
                    log_event("repeat_guard", callSid=call_sid, text=gate.strip())
                    replacement = REPEAT_FALLBACKS[0]
                    if _norm_text(repeated) == _norm_text(replacement):
                        replacement = REPEAT_FALLBACKS[1]
            elif not closing and is_dead_end(gate):
                log_event("dead_end_guard", callSid=call_sid, text=gate.strip())
                replacement = DEAD_END_RECOVERIES[0]
                if any(
                    _norm_text(r) == _norm_text(replacement) for r in recent_replies
                ):
                    replacement = DEAD_END_RECOVERIES[1]
            if replacement is not None:
                full = replacement
                await _flush(replacement)
                gate = ""
            pending += gate
        await _flush(pending)
        est_playback_s = await send_speech("", True, True, "reply")
        return full, est_playback_s

    def recv_future() -> asyncio.Future:
        """The one in-flight receive, started on first use.

        Shared by wait_out_playback() and next_message() so a receive that
        outlives a timed wait is reused rather than abandoned.
        """
        nonlocal pending_recv
        if pending_recv is None:
            pending_recv = asyncio.ensure_future(websocket.receive_json())
        return pending_recv

    def take(fut: asyncio.Future) -> dict:
        """Consume a completed receive and forget it, so it is never awaited
        twice."""
        nonlocal pending_recv
        if pending_recv is fut:
            pending_recv = None
        return fut.result()

    async def wait_out_playback() -> dict | None:
        """Wait for the agent's audio to finish, without ignoring the caller.

        The tokens are already sent, so ending the session immediately would
        cut a goodbye off mid-word. A blind sleep would also ignore a barge-in:
        the caller says "wait, one more thing" during the sign-off and the
        relay hangs up anyway. Any message that arrives during the wait is
        buffered for the main loop and returned, so the caller can decide
        whether the close still applies.

        On timeout the receive is left in flight for next_message() rather than
        cancelled: asyncio.wait does not cancel, and an abandoned receive would
        swallow the next message and drop it.
        """
        nonlocal pending_message
        remaining = max(0.0, agent_speaking_until - time.monotonic())
        if remaining <= 0:
            return None
        recv = recv_future()
        done, _pending = await asyncio.wait({recv}, timeout=remaining)
        if not done:
            return None
        pending_message = take(recv)
        return pending_message

    async def next_message() -> dict:
        """Receive the next Twilio message, speaking the idle prompt if the
        caller has gone quiet.

        The pending receive is kept rather than cancelled when the timer
        fires, so a message that lands at the same instant is not dropped, and
        it is reused across prompts: the websockets library forbids two
        concurrent receives. The timer only arms after the caller has spoken
        once: before that the opening is still playing and a prompt would talk
        over it.
        """
        nonlocal idle_prompts, pending_message
        if pending_message is not None:
            message, pending_message = pending_message, None
            return message
        idle_after = float(os.environ.get("RELAY_IDLE_AFTER", "8") or 0)
        if idle_after <= 0 or not caller_spoke or idle_prompts >= IDLE_PROMPTS_MAX:
            recv = recv_future()
            await recv
            return take(recv)
        recv = recv_future()
        while True:
            # Count from when the caller stops HEARING the agent, not from when
            # the text was sent: a 505-char pitch needs ~39s of audio, and the
            # old timer fired 8s after send, so the agent said "Are you still
            # there?" over its own pitch (live call CA7d8e6e).
            wait = idle_after + max(0.0, agent_speaking_until - time.monotonic())
            done, _pending = await asyncio.wait({recv}, timeout=wait)
            if done:
                return take(recv)
            idle_prompts += 1
            if idle_prompts == 1:
                text = os.environ.get("RELAY_IDLE_TEXT", "") or IDLE_TEXT
            else:
                text = os.environ.get("RELAY_IDLE_TEXT_2", "") or IDLE_TEXT_2
            await send_speech(text, True, True, "idle")
            log_event("idle_prompt", callSid=call_sid, text=text, n=idle_prompts)
            if idle_prompts >= IDLE_PROMPTS_MAX:
                # Out of prompts: wait for the caller with no timer running.
                await recv
                return take(recv)

    try:
        while True:
            message = await next_message()
            msg_type = message.get("type")

            if msg_type == "setup":
                call_sid = message.get("callSid", "")
                connection_type = message.get("connectionType", "initial")
                # A reconnect means the call is still up, so any close intent
                # recorded before the drop is stale: without this, a later drop
                # on the live call would hang up instead of dialling the store.
                CLOSE_PENDING.discard(call_sid)
                is_outbound = str(message.get("direction", "")).startswith("outbound")
                if is_outbound:
                    greeting = _env_or("RELAY_OUTBOUND_GREETING", OUTBOUND_GREETING)
                else:
                    greeting = _env_or("RELAY_WELCOME_GREETING", WELCOME_GREETING)
                if connection_type == "reconnect" and call_sid in SESSIONS:
                    history = SESSIONS[call_sid]
                    # Restore the callback details and owed-notice flags: they
                    # are not part of the conversation history, so without this
                    # a reconnect silently dropped a name/number the caller had
                    # already dictated.
                    state.update(CALL_STATE.get(call_sid) or new_call_state())
                    log_event(
                        "session_reconnected",
                        callSid=call_sid,
                        turns=len(history),
                        callback_name=state["callback_name"],
                        callback_phone=state["callback_phone"],
                    )
                else:
                    state.clear()
                    state.update(new_call_state())
                    # Outbound dials get the outbound system prompt: the
                    # agent called the person, so it must introduce the
                    # call's purpose instead of asking "how can I help you".
                    if is_outbound:
                        history[0] = {
                            "role": "system",
                            "content": with_offers(_env_or("RELAY_OUTBOUND_SYSTEM_PROMPT", OUTBOUND_SYSTEM_PROMPT)),
                        }
                    # The greeting is spoken by the relay (not the LLM), so
                    # the model never sees it — and then it repeats the minute
                    # question / EPP intro right after the caller said yes.
                    # Seed it as the first assistant turn so the model knows
                    # what was already said.
                    history.append({"role": "assistant", "content": greeting})
                    SESSIONS[call_sid] = history
                    CALL_STATE[call_sid] = dict(state)
                    if len(SESSIONS) > MAX_SESSIONS:
                        evicted = next(iter(SESSIONS))
                        SESSIONS.pop(evicted)
                        CALL_STATE.pop(evicted, None)
                if connection_type != "reconnect":
                    # The opening is spoken as NON-interruptible tokens: with
                    # the TwiML welcomeGreeting attribute a caller saying
                    # "hello" cut the greeting off mid-sentence (interruptible
                    # defaults to "any"). Speaking it ourselves with
                    # interruptible=false makes the opening uninterruptible.
                    async def _speak_opening(text: str) -> None:
                        await send_speech(text, True, False, "opening")

                    await _speak_opening(greeting)
                    if is_outbound:
                        # Outbound: the greeting already names the AI and ends
                        # with the minute question; the recording notice waits
                        # for a positive answer.
                        state["recording_disclosed"] = False
                        opening = greeting
                    else:
                        # Inbound: greeting -> AI + recording disclosure ->
                        # question, so the caller is invited to speak only
                        # after the compliance notice has finished.
                        disclosure = _env_or("RELAY_DISCLOSURE", DISCLOSURE)
                        await _speak_opening(disclosure)
                        await _speak_opening(OPENING_QUESTION)
                        history.append({"role": "assistant", "content": disclosure})
                        history.append(
                            {"role": "assistant", "content": OPENING_QUESTION}
                        )
                        state["recording_disclosed"] = True
                        opening = f"{greeting} {disclosure} {OPENING_QUESTION}"
                    # The opening is spoken by the relay, not the model, so it
                    # never reached the turn log: a transcript could not show
                    # what the caller actually heard at the start of the call.
                    log_event("turn", callSid=call_sid, role="assistant", text=opening)
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
                t_prompt = time.monotonic()
                # The caller is talking again: re-arm the idle prompt.
                caller_spoke = True
                idle_prompts = 0
                state["caller_turns"] += 1
                history.append({"role": "user", "content": user_text})

                # Caller asked for a person, or for one of the destinations the
                # store routes by extension ("is Neelam there?", "accounts
                # payable"). Hand off instead of answering: the model cannot
                # transfer a call, and guessing at a colleague's availability
                # is worse than connecting them.
                route = handoff_route(user_text, is_outbound=is_outbound)
                if route:
                    await do_handoff(
                        "caller-requested-human",
                        {"lastUtterance": user_text},
                        route=route,
                    )
                    return

                # A one- or two-word turn is a fragment, not an answer: the
                # caller was cut off or the line clipped. Answer it directly
                # with a short clarifying question instead of calling the
                # model, which pitched a 25-second EPP monologue at "It" and
                # answered "My." with "It sounds like you may have something
                # to share." (CAe33eb7). Not added to history, like the
                # interrupt ack: it is a prompt, not something the model said.
                if is_fragment(user_text):
                    prompt = FRAGMENT_PROMPTS[fragment_index % len(FRAGMENT_PROMPTS)]
                    fragment_index += 1
                    await send_speech(prompt, True, True, "fragment")
                    log_event(
                        "fragment_prompt", callSid=call_sid, text=user_text,
                        spoken=prompt,
                    )
                    continue

                # The caller talked over the agent. Acknowledge it before the
                # answer so the cut sounds like a person yielding the floor
                # instead of the agent going dead. Interruptible, so a caller
                # who keeps talking is not talked over in turn. Not added to
                # history: it is a filler, not something the model said.
                if interrupted:
                    interrupted = False
                    acks = interrupt_acks()
                    now = time.monotonic()
                    # A bare greeting or filler is not a cut-in worth
                    # acknowledging: "Hello." -> "Sure." sounds broken.
                    if GREETING_ONLY_RE.match(user_text):
                        acks = []
                    if acks and now - last_ack_at >= INTERRUPT_ACK_COOLDOWN:
                        ack = acks[ack_index % len(acks)]
                        ack_index += 1
                        last_ack_at = now
                        await send_speech(ack, True, True, "ack")
                        log_event("interrupt_ack", callSid=call_sid, text=ack)

                # Outbound: speak the recording notice early, non-interruptible.
                # It is appended to history only AFTER the reply (appending it
                # here would leave history ending in "assistant", which Mistral
                # rejects with HTTP 400).
                notice_to_log = None
                if (
                    is_outbound
                    and not state["recording_disclosed"]
                    and should_disclose_recording(user_text, state["caller_turns"])
                ):
                    state["recording_disclosed"] = True
                    notice_to_log = _env_or("RELAY_RECORDING_DISCLOSURE", RECORDING_DISCLOSURE)
                    await send_speech(notice_to_log, True, False, "notice")
                    log_event("recording_disclosed", callSid=call_sid)

                # Bare-name reply: the assistant just asked for the name and
                # the caller answered with the name alone ("Jonathan."), or
                # the caller volunteered a bare first name after a callback
                # was offered. A bare name is a single word; two words are
                # only accepted right after an explicit name question.
                # Captured before the bare-name block below consumes it: True
                # on the turn right after the assistant asked for the name.
                name_asked_this_turn = bool(state["expecting_name"])
                words = user_text.strip().strip(".,!?").split()
                fillers = NAME_STOPWORDS | {
                    "uh", "um", "yeah", "yes", "no", "ok", "okay",
                    "sure", "hi", "hello", "thanks", "thank",
                }
                bare_name_ok = False
                if state["expecting_name"]:
                    state["expecting_name"] = False
                    bare_name_ok = 1 <= len(words) <= 2
                elif state["callback_offered"] and not state["callback_name"]:
                    bare_name_ok = len(words) == 1
                if (
                    bare_name_ok
                    and words
                    and all(w.isalpha() for w in words)
                    and words[0].lower() not in fillers
                ):
                    state["callback_name"] = " ".join(
                        w.capitalize() for w in words[:2]
                    )

                phone_newly_captured = False
                phone = extract_phone(user_text)
                if phone:
                    if not state["callback_phone"]:
                        phone_newly_captured = True
                    state["callback_phone"] = phone
                    state["phone_buffer"] = ""
                else:
                    # Bank digit groups of 3+ across turns ("289." ...
                    # "901." ... "0990."). Groups under 3 digits ("3 pm",
                    # "30 percent") are ignored.
                    for group in re.findall(r"\d+", user_text):
                        if len(group) >= 3:
                            state["phone_buffer"] += group
                    buf = state["phone_buffer"]
                    if len(buf) == 10:
                        if not state["callback_phone"]:
                            phone_newly_captured = True
                        state["callback_phone"] = "+1" + buf
                        state["phone_buffer"] = ""
                    elif len(buf) == 11 and buf.startswith("1"):
                        if not state["callback_phone"]:
                            phone_newly_captured = True
                        state["callback_phone"] = "+1" + buf[1:]
                        state["phone_buffer"] = ""
                    elif len(buf) > 10:
                        # Ambiguous, e.g. 11 digits not starting with 1 (a live
                        # call heard "90052642228" for 905-264-2228). Never
                        # store a number the caller did not confirm: drop the
                        # buffer and have the model ask for the ten digits
                        # again.
                        state["phone_buffer"] = ""
                        state["phone_ambiguous"] = True
                name = extract_name(user_text)
                if name:
                    state["callback_name"] = name

                # Reconcile the model's judgement with the relay's, the same
                # way the phone notes below do: the model may accept a
                # stop-word ("Done." for "Don") as a name, but the bare-name
                # path above refuses it, so state["callback_name"] stays None
                # and the CALLBACK_REQUEST block would never fire. Inserted at
                # len(history)-1 so the last entry stays the user message (a
                # trailing system message is a Mistral 400).
                if (
                    name_asked_this_turn
                    and not state["callback_name"]
                    and not _caller_asked(user_text)
                ):
                    # Only a real attempt counts: a caller who answers the
                    # name question with a question of their own has not
                    # failed to give a name, and the model should answer them
                    # rather than re-ask.
                    state["name_attempts"] += 1
                    note = (
                        NAME_REASK_NOTE
                        if state["name_attempts"] <= MAX_NAME_ATTEMPTS
                        else NAME_GIVE_UP_NOTE
                    )
                    history.insert(
                        len(history) - 1, {"role": "system", "content": note}
                    )

                # Log the callback as soon as both halves are known, instead
                # of waiting for a later assistant reply that happens to
                # contain the word "callback". A caller who never gives a
                # usable name still gets a callback: after MAX_NAME_ATTEMPTS
                # the phone alone is enough, so the store is not left with
                # nothing (live call CA2903 lost the whole request that way).
                if (
                    not state["callback_logged"]
                    and state["callback_phone"]
                    and (
                        state["callback_name"]
                        or state["name_attempts"] > MAX_NAME_ATTEMPTS
                    )
                ):
                    state["callback_logged"] = True
                    log_event(
                        "CALLBACK_REQUEST",
                        callSid=call_sid,
                        name=state["callback_name"],
                        phone=state["callback_phone"],
                    )
                    # If the store wants SMS alerts, text the callback
                    # details as they are captured. Env-gated: without
                    # STORE_SMS_NUMBER (or Twilio creds) this stays logs-only.
                    store_sms = os.environ.get("STORE_SMS_NUMBER", "").strip()
                    if store_sms:
                        asyncio.create_task(
                            send_sms(
                                store_sms,
                                f"Callback request: {state['callback_name'] or 'name not provided'} {state['callback_phone']} (call {call_sid})",
                            )
                        )

                # Surface the booked callback to the model. A live call
                # (CA2903) confirmed the callback at 13:59:49 and then offered
                # it again at 14:00:21, because the post-pitch prompt still
                # says "your next step is the callback offer" and nothing told
                # the model the callback was already arranged. Inserted on
                # EVERY turn while the callback is booked: a once-only note
                # would be evicted by the MAX_HISTORY_MESSAGES trim (only
                # history[0] is permanent), and a prompt rule alone has
                # already lost to state twice in this codebase.
                if state["callback_logged"]:
                    history.insert(
                        len(history) - 1,
                        {
                            "role": "system",
                            "content": (
                                "A store callback is ALREADY arranged on this "
                                f"call: {state['callback_name'] or 'the caller'} "
                                f"at {state['callback_phone']}. Do not offer, "
                                "ask about, or re-arrange a callback again. You "
                                "may confirm it in one sentence if relevant; "
                                "otherwise answer the caller's question or "
                                "close the call politely."
                            ),
                        },
                    )

                # Tell the LLM the number is complete: it can't reliably
                # count digits across piecemeal turns, and without this it
                # asks for "the last three digits" after already having
                # all ten.
                if phone_newly_captured:
                    # Insert BEFORE the just-appended user message: Mistral
                    # requires the LAST history entry to be user/tool, and a
                    # trailing system message is a 400.
                    history.insert(
                        len(history) - 1,
                        {
                            "role": "system",
                            "content": (
                                "The caller has now given their complete "
                                "10-digit phone number. Confirm the full "
                                "number in your reply and do not ask for "
                                "more digits."
                            ),
                        },
                    )
                elif state.get("phone_ambiguous"):
                    # The digits did not form a clean 10-digit number (a live
                    # call heard "90052642228" for 905-264-2228 and the old
                    # regex silently kept the first ten). Nothing was stored,
                    # so the model must not read back a number it invented.
                    state["phone_ambiguous"] = False
                    history.insert(
                        len(history) - 1,
                        {
                            "role": "system",
                            "content": (
                                "The caller's digits did not form a clear "
                                "10-digit phone number. Do NOT confirm or "
                                "repeat any number. Ask the caller to say the "
                                "ten digits again, slowly."
                            ),
                        },
                    )

                # Outbound: the relay speaks the pitch itself, so its length is
                # fixed and it can never be re-pitched. The model used to
                # generate it and produced a 505-char monologue (~39s of audio)
                # with the callback offer at char 430 (live call CA7d8e6e).
                # Skipped on a clear decline or a goodbye: a caller who is
                # leaving must not be pitched at.
                #
                # This runs AFTER the name/phone extraction above on purpose: a
                # caller whose first words are "I'm Don, 416-555-0100" must
                # still have the callback captured, and the pitch turn used to
                # `continue` before any of that ran.
                #
                # The greeting asks "Do you have a quick minute?", so the pitch
                # waits for an actual answer. A live call (CA4ae95) said
                # "Hello." three seconds after the greeting — answering the
                # phone, not consenting — and got 23 seconds of pitch before it
                # could say anything. Order matters: a decline is checked first
                # so "yeah, no" and "okay, bye" still decline, and a caller who
                # volunteers a callback number on turn one is engaging, so that
                # counts as consent.
                if (
                    is_outbound
                    and not state["pitch_delivered"]
                    and not state.get("pitch_skipped")
                ):
                    if NEGATIVE_RE.search(user_text) or CLOSING_RE.search(user_text):
                        # Declined: retire the phase so the model cannot pitch
                        # on a later turn, then let it close politely.
                        _retire_pitch_phase(history, state)
                        save_call_state()
                    elif AFFIRMATIVE_RE.search(user_text) or state["callback_phone"]:
                        spoken_pitch = _speakable_text(OUTBOUND_PITCH)
                        # Flag the phase and persist BEFORE speaking: a socket
                        # drop between the send and the save would restore
                        # pitch_delivered=False and re-pitch on reconnect.
                        _swap_to_after_pitch(history, state, call_sid)
                        save_call_state()
                        await send_speech(spoken_pitch, True, True, "reply")
                        # The notice was spoken before the pitch, so it goes
                        # into history first: the model reads the turn in the
                        # order the caller heard it.
                        if notice_to_log:
                            history.append(
                                {"role": "assistant", "content": notice_to_log}
                            )
                        history.append(
                            {"role": "assistant", "content": spoken_pitch}
                        )
                        state["recent_replies"].append(spoken_pitch)
                        del state["recent_replies"][:-RECENT_REPLIES_KEPT]
                        while len(history) > 1 + MAX_HISTORY_MESSAGES:
                            del history[1]
                        log_event(
                            "pitch_spoken",
                            callSid=call_sid,
                            chars=len(spoken_pitch),
                            est_playback_s=round(
                                estimate_speech_seconds(spoken_pitch), 1
                            ),
                        )
                        continue
                    else:
                        # Permission phase: the caller has not answered the
                        # greeting's question yet. Re-ask, then close politely.
                        # Relay-spoken so the reply is deterministic and cannot
                        # turn into an uncontrolled pitch.
                        if state["permission_reasks"] < MAX_PERMISSION_REASKS:
                            line = PERMISSION_REASKS[state["permission_reasks"]]
                            state["permission_reasks"] += 1
                            event = "permission_reask"
                        else:
                            line = PERMISSION_GIVE_UP_CLOSE
                            _retire_pitch_phase(history, state)
                            event = "permission_give_up"
                        spoken_line = _speakable_text(line)
                        save_call_state()
                        await send_speech(spoken_line, True, True, "reply")
                        if notice_to_log:
                            history.append(
                                {"role": "assistant", "content": notice_to_log}
                            )
                        history.append(
                            {"role": "assistant", "content": spoken_line}
                        )
                        state["recent_replies"].append(spoken_line)
                        del state["recent_replies"][:-RECENT_REPLIES_KEPT]
                        while len(history) > 1 + MAX_HISTORY_MESSAGES:
                            del history[1]
                        log_event(event, callSid=call_sid, text=spoken_line)
                        if event == "permission_give_up":
                            # The line ends with "Have a good day." — the call
                            # is over, so close it instead of leaving the
                            # session open for idle prompts on a caller who
                            # never agreed to talk.
                            if call_sid:
                                CLOSE_PENDING.add(call_sid)
                            barged = await wait_out_playback()
                            if barged is not None and barged.get("type") in (
                                "interrupt",
                                "dtmf",
                                "prompt",
                            ):
                                CLOSE_PENDING.discard(call_sid)
                                log_event(
                                    "close_aborted",
                                    callSid=call_sid,
                                    type=barged.get("type"),
                                )
                                continue
                            await do_close("permission-give-up")
                            return
                        continue

                # The caller cut the assistant off last turn. The truncated
                # assistant turn no longer reads as "I already said this", so
                # the model re-emits the whole pitch — a live call repeated
                # the same 372-char pitch verbatim on "Hello.". One-shot note,
                # inserted before the just-appended user message for the same
                # reason as the phone-number note above.
                if state.pop("interrupted", False):
                    history.insert(
                        len(history) - 1,
                        {
                            "role": "system",
                            "content": (
                                "The caller interrupted you mid-sentence. Do "
                                "not repeat anything you already said, and do "
                                "not give the EPP pitch again. Respond only to "
                                "what the caller just said; if it was unclear, "
                                "ask one short clarifying question."
                            ),
                        },
                    )

                # Everything the caller has dictated this turn is now in
                # `state`; persist it before the LLM call so a WebSocket drop
                # mid-reply cannot lose a name or number.
                save_call_state()

                spoken_box[0] = ""
                prev_assistant = next(
                    (m["content"] for m in reversed(history) if m["role"] == "assistant"),
                    "",
                )
                closing = bool(CLOSING_RE.search(user_text))
                try:
                    llm_task = asyncio.create_task(
                        send_text_tokens(
                            mistral_stream_reply(history),
                            spoken_box,
                            prev_assistant=prev_assistant,
                            recent_replies=state["recent_replies"],
                            closing=closing,
                            user_text=user_text,
                        )
                    )
                    reply = ""
                    est_playback_s = 0.0
                    # Race the LLM task against incoming Twilio messages so an
                    # interrupt is handled WHILE the reply is still streaming,
                    # not only after it finished.
                    recv_task = None
                    try:
                        while True:
                            recv_task = asyncio.ensure_future(websocket.receive_json())
                            done, _pending = await asyncio.wait(
                                {llm_task, recv_task},
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if llm_task in done:
                                reply, est_playback_s = llm_task.result()
                                break
                            live = recv_task.result()
                            if live.get("type") == "interrupt":
                                llm_task.cancel()
                                try:
                                    await llm_task
                                except asyncio.CancelledError:
                                    pass
                                # Twilio stopped playback at this instant, so
                                # the estimate is spent.
                                agent_speaking_until = time.monotonic()
                                utterance = (live.get("utteranceUntilInterrupt") or "").strip()
                                # Keep only what the caller heard before cutting
                                # in, so Mistral never thinks it said things the
                                # caller never heard (a price, a disclosure).
                                heard = _truncate_to_utterance(spoken_box[0], utterance)
                                if heard:
                                    history.append({"role": "assistant", "content": heard})
                                if notice_to_log:
                                    history.append({"role": "assistant", "content": notice_to_log})
                                while len(history) > 1 + MAX_HISTORY_MESSAGES:
                                    del history[1]
                                log_event(
                                    "interrupt",
                                    callSid=call_sid,
                                    utteranceUntilInterrupt=utterance or None,
                                    heard_chars=len(heard),
                                )
                                # Acknowledge on the prompt that follows, not
                                # now: the caller is still mid-sentence.
                                interrupted = True
                                # The truncated assistant turn no longer looks
                                # like "I already gave the pitch", so the model
                                # re-emits it. A live call repeated the same
                                # 372-char pitch verbatim on "Hello.".
                                state["interrupted"] = True
                                # The caller may have heard the whole pitch
                                # before cutting in. If so the call is past the
                                # pitch even though the reply never completed,
                                # and the pre-pitch prompt must not come back.
                                # Check what was SPOKEN as well as what was
                                # heard: `heard` is a best-effort truncation and
                                # comes back short (or empty) when the utterance
                                # does not match verbatim, which Twilio does to
                                # numbers. Re-pitching is the worse failure.
                                if is_outbound and (
                                    _pitch_delivered(heard)
                                    or _pitch_delivered(spoken_box[0])
                                ):
                                    _swap_to_after_pitch(history, state, call_sid)
                                    # The swap happens after the turn's
                                    # save_call_state(), so persist it here or a
                                    # reconnect restores pitch_delivered=False.
                                    save_call_state()
                                reply = ""
                                break
                            elif live.get("type") == "dtmf":
                                digit = live.get("digit")
                                log_event("dtmf", callSid=call_sid, digit=digit)
                                if digit == "0":
                                    await do_handoff("dtmf-0")
                                    return
                            else:
                                log_event(
                                    "early_message", callSid=call_sid, type=live.get("type")
                                )
                    finally:
                        # The websockets library forbids concurrent recv():
                        # a still-pending receive task must be cancelled AND
                        # awaited before the main loop calls receive_json().
                        if recv_task is not None:
                            if not recv_task.done():
                                recv_task.cancel()
                            try:
                                await recv_task
                            except (asyncio.CancelledError, Exception):
                                pass
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
                        await send_speech(reply, True, True, "error")
                    except (WebSocketDisconnect, RuntimeError):
                        # The caller already hung up; nothing to say it to.
                        pass
                finally:
                    if llm_task is not None and not llm_task.done():
                        llm_task.cancel()
                    llm_task = None

                if reply:
                    # What the caller actually heard, kept for the pre-speech
                    # gate on the next turn. History cannot serve this: an
                    # interrupt truncates the assistant turn to the fragment
                    # the caller heard, so the full reply is lost there. A ring
                    # of the last few replies, not just the last one: the turn
                    # between two pitch attempts is often a short fallback, so
                    # comparing against one reply misses the pitch repeat.
                    state["recent_replies"].append(reply.strip())
                    del state["recent_replies"][:-RECENT_REPLIES_KEPT]
                    if (
                        is_outbound
                        and not state["pitch_delivered"]
                        and not state.get("pitch_skipped")
                        and _pitch_delivered(reply)
                    ):
                        # The pitch is now behind us. Swap the system prompt for
                        # one with no pitch script and no "go straight into the
                        # pitch" trigger: the pre-pitch prompt tells the model to
                        # pitch on any unclear first reply, which is exactly what
                        # made it re-pitch after a "hello". Removing the trigger
                        # is the fix; another negative rule is not.
                        _swap_to_after_pitch(history, state, call_sid)
                    if is_repeat(prev_assistant, reply.strip()) and not (
                        CLOSING_RE.search(reply)
                        or (_caller_asked(user_text) and not _pitch_delivered(reply))
                    ):
                        # History-level backstop only: the caller has already
                        # heard this reply, so this keeps the model from
                        # seeing its own loop on the next turn. What the
                        # caller hears is decided by the pre-speech gate in
                        # send_text_tokens.
                        log_event("repeat_guard", callSid=call_sid)
                        reply = REPEAT_FALLBACKS[0]
                        if _norm_text(prev_assistant) == _norm_text(reply):
                            reply = REPEAT_FALLBACKS[1]
                    log_event(
                        "turn_done",
                        callSid=call_sid,
                        ms=round((time.monotonic() - t_prompt) * 1000, 1),
                        chars=len(reply),
                        est_playback_s=round(est_playback_s, 1),
                    )
                    history.append({"role": "assistant", "content": reply.strip()})
                    if notice_to_log:
                        # The recording notice was spoken before this reply;
                        # record it after the reply so history keeps ending in
                        # a role Mistral accepts at the START of the next turn.
                        history.append({"role": "assistant", "content": notice_to_log})
                    # Keep only the system prompt plus the most recent turns.
                    while len(history) > 1 + MAX_HISTORY_MESSAGES:
                        del history[1]
                    log_event(
                        "turn", callSid=call_sid, role="assistant", text=reply.strip()
                    )
                    # The next short reply is likely just the caller's name.
                    lowered_reply = reply.lower()
                    if "callback" in lowered_reply or "call you back" in lowered_reply:
                        state["callback_offered"] = True
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
                        state["expecting_name"] = True
                    # Backstop: the LLM often echoes the name itself
                    # ("Thank you Jonathan.") even when the caller's bare
                    # reply didn't match the name patterns. A capitalised
                    # word right after "thank you/thanks" is the name.
                    if not state["callback_name"]:
                        name_match = re.search(
                            r"(?i:thank you|thanks),?\s+([A-Z][a-z]+)", reply
                        )
                        if name_match:
                            candidate = name_match.group(1)
                            if candidate.lower() not in NAME_STOPWORDS:
                                state["callback_name"] = candidate
                    # The reply may have set the callback-offered / name-owed
                    # flags; persist them for a reconnect.
                    save_call_state()
                    # The agent signed off, so the call is over. Wait out the
                    # playback estimate first: the tokens are already sent, and
                    # ending the session immediately would cut the goodbye off
                    # mid-word. A live call (CA36b78) said "Goodbye." five times
                    # because nothing ended the session on a sign-off.
                    if is_agent_closing(reply):
                        # Record the intent before the wait: if the socket drops
                        # mid-goodbye, Twilio posts /handoff with no
                        # HandoffData, and without this the store would be
                        # dialled for a caller who was being hung up on.
                        if call_sid:
                            CLOSE_PENDING.add(call_sid)
                        barged = await wait_out_playback()
                        if barged is not None and barged.get("type") in (
                            "interrupt",
                            "dtmf",
                            "prompt",
                        ):
                            # The caller is still talking, or pressed a key, or
                            # said something new: the sign-off did not end the
                            # conversation. The message is buffered, so the main
                            # loop handles it on the next pass. The close intent
                            # is withdrawn, or a later socket drop would hang up
                            # a call that is still going.
                            CLOSE_PENDING.discard(call_sid)
                            log_event(
                                "close_aborted",
                                callSid=call_sid,
                                type=barged.get("type"),
                            )
                            continue
                        await do_close()
                        return

            elif msg_type == "interrupt":
                if llm_task is not None and not llm_task.done():
                    llm_task.cancel()
                # Twilio stopped playback at this instant, so the estimate is
                # spent: the idle clock counts from here.
                agent_speaking_until = time.monotonic()
                # Only acknowledge a cut-off REPLY. "Sure." answering a
                # cut-off "Are you still there?" is a non-sequitur (live call
                # CA7d8e6e: caller said "Hello.", agent said "Sure.").
                interrupted = last_speech_kind == "reply"
                # Same one-shot note the streaming path sets. The relay-spoken
                # pitch goes out in a single token, so a caller who cuts in on
                # it lands here rather than in the streaming race; without the
                # note the model re-emits the pitch on the next turn.
                if interrupted:
                    state["interrupted"] = True
                log_event(
                    "interrupt",
                    callSid=call_sid,
                    utteranceUntilInterrupt=message.get("utteranceUntilInterrupt"),
                    interrupted_kind=last_speech_kind,
                )

            elif msg_type == "dtmf":
                digit = message.get("digit")
                log_event("dtmf", callSid=call_sid, digit=digit)
                if digit == "0":
                    await do_handoff("dtmf-0")
                    return

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
    finally:
        # A receive left in flight by wait_out_playback() would otherwise be
        # garbage-collected with its exception unretrieved, logging a spurious
        # "Task exception was never retrieved" on every normal close. Always
        # await it, even when it is already done: a peer that closed the socket
        # while do_close() was yielding leaves a completed future holding an
        # unretrieved WebSocketDisconnect.
        if pending_recv is not None:
            if not pending_recv.done():
                pending_recv.cancel()
            try:
                await pending_recv
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
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
