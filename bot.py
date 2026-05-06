"""
8OS Telegram Bot — Week 1 MVP

Quiz flow:
  1. Full name
  2. Birth date (DD/MM/YYYY)
  3. Top 3 life/work domains
  4. Primary goal right now

Calls POST /onboard on the Orchestrator and delivers the personalized briefing.
"""

import asyncio
import logging
import os
import time
from collections import defaultdict
from threading import Lock

import httpx
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    ConversationHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

ORCHESTRATOR_URL = os.environ["ORCHESTRATOR_URL"].rstrip("/")
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ORCHESTRATOR_API_KEY = os.environ.get("ORCHESTRATOR_API_KEY", "")

# ConversationHandler states
NAME, BIRTHDATE, DOMAINS, GOAL = range(4)

# Max Telegram message length
_TG_MAX_LEN = 4096

# Prometheus metrics
_command_counts = Counter(
    "telegram_bot_commands_total", "Total bot commands received", ["command", "status"]
)
_quiz_steps = Counter(
    "telegram_bot_quiz_steps_total", "Quiz step completions", ["step"]
)
_quiz_completions = Counter(
    "telegram_bot_quiz_completions_total", "Successful quiz completions"
)
_orchestrator_calls = Counter(
    "telegram_bot_orchestrator_calls_total", "Orchestrator API calls", ["endpoint", "status"]
)
_orchestrator_latency = Histogram(
    "telegram_bot_orchestrator_latency_seconds", "Orchestrator call latency", ["endpoint"]
)
_active_conversations = Gauge(
    "telegram_bot_active_conversations", "Currently active quiz conversations"
)
_message_chars_sent = Counter(
    "telegram_bot_chars_sent_total", "Characters sent to users"
)

_metrics_lock = Lock()
_command_stats = defaultdict(lambda: {"success": 0, "error": 0})
_quiz_step_stats = defaultdict(int)


def _chunk_text(text: str) -> list[str]:
    """Split text into ≤4096-char chunks without cutting mid-word."""
    if len(text) <= _TG_MAX_LEN:
        return [text]
    chunks = []
    while text:
        if len(text) <= _TG_MAX_LEN:
            chunks.append(text)
            break
        split_at = text.rfind("\n", 0, _TG_MAX_LEN)
        if split_at == -1:
            split_at = text.rfind(" ", 0, _TG_MAX_LEN)
        if split_at == -1:
            split_at = _TG_MAX_LEN
        chunks.append(text[:split_at].rstrip())
        text = text[split_at:].lstrip()
    return chunks


async def _get_existing_briefing(user_id: str) -> str | None:
    """Fetch existing briefing for returning user. Returns None if not found."""
    headers = {}
    if ORCHESTRATOR_API_KEY:
        headers["X-API-Key"] = ORCHESTRATOR_API_KEY

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{ORCHESTRATOR_URL}/briefing/by-telegram/{user_id}",
                headers=headers,
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
            return data.get("briefing", "")
    except Exception:
        return None


async def _call_orchestrator(payload: dict) -> str:
    """Call POST /onboard and return briefing text. Retries once on transient error."""
    headers = {"Content-Type": "application/json"}
    if ORCHESTRATOR_API_KEY:
        headers["X-API-Key"] = ORCHESTRATOR_API_KEY

    for attempt in range(2):
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(
                    f"{ORCHESTRATOR_URL}/onboard",
                    json=payload,
                    headers=headers,
                )
                resp.raise_for_status()
                data = resp.json()
                return data.get("briefing", data.get("briefing_text", str(data)))
        except (httpx.TransportError, httpx.TimeoutException) as exc:
            if attempt == 0:
                logger.warning("Orchestrator transient error (attempt 1): %s", exc)
                await asyncio.sleep(2)
                continue
            raise
    raise RuntimeError("Orchestrator unreachable after retry")


# ---------------------------------------------------------------------------
# /start — begins quiz (or shows welcome banner for returning users)
# ---------------------------------------------------------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = str(update.effective_user.id)

    existing_briefing = await _get_existing_briefing(user_id)
    if existing_briefing:
        stored_name = context.user_data.get("name")
        name = stored_name or update.effective_user.first_name or "Operative"
        archetype_line = ""
        for line in existing_briefing.split("\n"):
            if any(keyword in line.lower() for keyword in ["archetype", "type:", "element:"]):
                archetype_line = line.strip()
                break
        if not archetype_line and len(existing_briefing) > 50:
            archetype_line = existing_briefing[:100].split(".")[0] + "."

        banner = (
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🎯 *8OS WELCOME BACK*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        )
        banner += f"Welcome, *{name}*.\n\n"
        if archetype_line:
            banner += f"_{archetype_line}_\n\n"
        banner += (
            "Your OS is active and running.\n\n"
            "Options:\n"
            "• `/status` — View your current OS briefing\n"
            "• `/start` — Regenerate with a new quiz\n\n"
            "Or simply send your message to interact with your OS."
        )
        await update.message.reply_text(banner, parse_mode="Markdown")
        return ConversationHandler.END

    context.user_data.clear()
    await update.message.reply_text(
        "🎯 *MISSION BRIEFING INITIALISED*\n\n"
        "I'm your 8OS intelligence officer. I'll build your personalized operating system "
        "from four data points.\n\n"
        "Respond with precision. No fluff needed.\n\n"
        "*Step 1 of 4 — Identification*\n"
        "State your full name:",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove(),
    )
    return NAME


async def receive_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["name"] = update.message.text.strip()
    _quiz_steps.labels(step="name").inc()
    await update.message.reply_text(
        "*Step 2 of 4 — Date of Birth*\n"
        "Enter your birth date in DD/MM/YYYY format.\n"
        "Example: `15/03/1988`",
        parse_mode="Markdown",
    )
    return BIRTHDATE


async def receive_birthdate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw = update.message.text.strip()
    try:
        from datetime import datetime
        dt = datetime.strptime(raw, "%d/%m/%Y")
    except ValueError:
        await update.message.reply_text(
            "⚠️ Invalid format. Use DD/MM/YYYY — e.g. `15/03/1988`",
            parse_mode="Markdown",
        )
        return BIRTHDATE

    context.user_data["birth_date"] = raw
    context.user_data["birth_date_iso"] = dt.strftime("%Y-%m-%d")
    _quiz_steps.labels(step="birthdate").inc()
    domain_buttons = [
        ["Career", "Health", "Relationships"],
        ["Finances", "Creativity", "Learning"],
        ["Family", "Spirituality", "Leadership"],
    ]
    await update.message.reply_text(
        "*Step 3 of 4 — Operational Domains*\n"
        "Name your top 3 life/work domains — the arenas where you need your OS to perform.\n"
        "Type them or pick from below (comma-separated if typing):",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(
            domain_buttons, one_time_keyboard=True, resize_keyboard=True
        ),
    )
    return DOMAINS


async def receive_domains(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw = update.message.text.strip()
    domains = [d.strip() for d in raw.replace(";", ",").split(",") if d.strip()]
    if not domains:
        await update.message.reply_text("Enter at least one domain.")
        return DOMAINS

    context.user_data["domains"] = domains[:3]
    _quiz_steps.labels(step="domains").inc()
    await update.message.reply_text(
        "*Step 4 of 4 — Primary Objective*\n"
        "What is your #1 goal right now? Be specific — one clear mission outcome:",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove(),
    )
    return GOAL


async def receive_goal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["goal"] = update.message.text.strip()
    _quiz_steps.labels(step="goal").inc()

    await update.message.reply_text(
        "⚙️ *Generating your OS…*\n\nAnalysing BaZi profile, archetype mapping, "
        "and priority matrix. Stand by.",
        parse_mode="Markdown",
    )

    payload = {
        "telegram_id": str(update.effective_user.id),
        "name": context.user_data["name"],
        "birth_date": context.user_data["birth_date_iso"],
        "goals": [context.user_data["goal"]],
        "domains": context.user_data["domains"],
    }

    try:
        start_time = time.time()
        briefing = await _call_orchestrator(payload)
        latency = time.time() - start_time
        _orchestrator_latency.labels(endpoint="onboard").observe(latency)
        _orchestrator_calls.labels(endpoint="onboard", status="success").inc()
        _quiz_completions.inc()
        _active_conversations.dec()
    except Exception as exc:
        logger.error("Orchestrator error for user %s: %s", update.effective_user.id, exc)
        _orchestrator_calls.labels(endpoint="onboard", status="error").inc()
        _active_conversations.dec()
        await update.message.reply_text(
            "⚠️ *Briefing generation failed.* The orchestrator is unreachable.\n"
            "Your data is saved. Try `/start` again in a moment.",
            parse_mode="Markdown",
        )
        return ConversationHandler.END

    for chunk in _chunk_text(briefing):
        _message_chars_sent.inc(len(chunk))
        await update.message.reply_text(chunk, parse_mode="Markdown")

    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _active_conversations.dec()
    _command_counts.labels(command="cancel", status="success").inc()
    await update.message.reply_text(
        "Mission aborted. Send /start when ready to re-engage.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return ConversationHandler.END


# ---------------------------------------------------------------------------
# /help — explains the bot
# ---------------------------------------------------------------------------

async def metrics_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        metrics_output = generate_latest()
        metrics_text = metrics_output.decode("utf-8")
        _command_counts.labels(command="metrics", status="success").inc()
        await update.message.reply_text(
            f"📊 *8OS Bot Metrics*\n\n```\n{metrics_text[:3500]}\n```",
            parse_mode="Markdown",
        )
    except Exception as exc:
        logger.error("Metrics generation error: %s", exc)
        _command_counts.labels(command="metrics", status="error").inc()
        await update.message.reply_text("⚠️ Could not generate metrics.")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _command_counts.labels(command="help", status="success").inc()
    await update.message.reply_text(
        "🧠 *8OS — Your Personalized Operating System*\n\n"
        "8OS maps your BaZi profile (Chinese metaphysics) to a strategic operating "
        "framework tailored to your archetype.\n\n"
        "*Commands:*\n"
        "`/start` or `/quiz` — Begin onboarding quiz (≈2 min)\n"
        "`/status` — View your current OS config\n"
        "`/metrics` — View bot operational metrics\n"
        "`/help` — This message\n"
        "`/cancel` — Abort current flow\n\n"
        "Your briefing is delivered in battlefield intelligence officer tone — "
        "direct, strategic, no fluff.",
        parse_mode="Markdown",
    )


# ---------------------------------------------------------------------------
# /status — show current OS config summary
# ---------------------------------------------------------------------------

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = str(update.effective_user.id)
    headers = {}
    if ORCHESTRATOR_API_KEY:
        headers["X-API-Key"] = ORCHESTRATOR_API_KEY

    try:
        start_time = time.time()
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                f"{ORCHESTRATOR_URL}/briefing/by-telegram/{user_id}",
                headers=headers,
            )
            latency = time.time() - start_time
            _orchestrator_latency.labels(endpoint="status").observe(latency)
            if resp.status_code == 404:
                _orchestrator_calls.labels(endpoint="status", status="not_found").inc()
                _command_counts.labels(command="status", status="not_found").inc()
                await update.message.reply_text(
                    "No OS config found. Run /start to generate yours."
                )
                return
            resp.raise_for_status()
            data = resp.json()
            _orchestrator_calls.labels(endpoint="status", status="success").inc()
            _command_counts.labels(command="status", status="success").inc()

        briefing = data.get("briefing", "")
        header = "📋 *Your current OS briefing:*\n\n"
        footer = "\n\nSend /start to regenerate."
        full_text = header + briefing + footer
        for chunk in _chunk_text(full_text):
            _message_chars_sent.inc(len(chunk))
            await update.message.reply_text(chunk, parse_mode="Markdown")

    except Exception as exc:
        logger.error("Status fetch error for user %s: %s", user_id, exc)
        _orchestrator_calls.labels(endpoint="status", status="error").inc()
        _command_counts.labels(command="status", status="error").inc()
        await update.message.reply_text(
            "⚠️ Could not retrieve status. Try again shortly."
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    app = Application.builder().token(BOT_TOKEN).build()

    quiz_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start), CommandHandler("quiz", start)],
        states={
            NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_name)],
            BIRTHDATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_birthdate)],
            DOMAINS: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_domains)],
            GOAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_goal)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(quiz_handler)
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("metrics", metrics_command))

    logger.info("8OS Telegram bot starting (polling)…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
