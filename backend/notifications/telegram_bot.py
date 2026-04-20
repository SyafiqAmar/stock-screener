"""
Telegram notification module (optional).
Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID env vars.
"""
import logging
import requests

from backend.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)


def send_telegram_alert(message: str) -> bool:
    """Send a message via Telegram bot."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram not configured, skipping alert")
        return False

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
        }
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code == 200:
            logger.info("Telegram alert sent")
            return True
        else:
            logger.warning(f"Telegram error: {resp.status_code} {resp.text}")
            return False
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return False


def format_signal_alert(signal: dict) -> str:
    """Format a signal dict into a Telegram message."""
    signal_type = signal.get("type", "unknown").replace("_", " ").title()
    ticker = signal.get("ticker", "?")
    tf = signal.get("timeframe", "?")
    score = signal.get("confidence_score", 0)

    msg = (
        f"🚨 <b>Signal Detected!</b>\n\n"
        f"📊 <b>{ticker}</b>\n"
        f"📌 Type: <code>{signal_type}</code>\n"
        f"⏰ Timeframe: <code>{tf}</code>\n"
        f"🎯 Confidence: <code>{score:.1%}</code>\n"
    )

    if signal.get("indicator"):
        msg += f"📈 Indicator: <code>{signal['indicator']}</code>\n"

    if signal.get("multi_tf_confirmed"):
        tfs = ", ".join(signal.get("confirmed_timeframes", []))
        msg += f"✅ Multi-TF confirmed: <code>{tfs}</code>\n"

    return msg


def send_scan_complete_alert(total_tickers: int, total_signals: int):
    """Send summary alert when a scan completes."""
    msg = (
        f"✅ <b>Scan Complete!</b>\n\n"
        f"📊 Tickers scanned: <code>{total_tickers}</code>\n"
        f"🚨 Signals found: <code>{total_signals}</code>\n"
    )
    return send_telegram_alert(msg)
