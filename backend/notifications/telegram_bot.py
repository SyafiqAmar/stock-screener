"""
Telegram notification module (Asynchronous).
Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID env vars.
"""
import logging
import aiohttp
import asyncio

from backend.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)


async def send_telegram_alert(message: str) -> bool:
    """Send a message via Telegram bot (Asynchronous)."""
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
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as resp:
                if resp.status == 200:
                    logger.info("Telegram alert sent (Async)")
                    return True
                else:
                    text = await resp.text()
                    logger.warning(f"Telegram error: {resp.status} {text}")
                    return False
    except Exception as e:
        logger.error(f"Telegram send failed (Async): {e}")
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


async def send_scan_complete_alert(total_tickers: int, total_signals: int):
    """Send summary alert when a scan completes."""
    msg = (
        f"✅ <b>Scan Complete!</b>\n\n"
        f"📊 Tickers scanned: <code>{total_tickers}</code>\n"
        f"🚨 Signals found: <code>{total_signals}</code>\n"
    )
    return await send_telegram_alert(msg)


async def send_top_signals_summary(top_signals: list[dict]):
    """Send a consolidated Top 5 summary alert."""
    if not top_signals:
        return await send_telegram_alert("✅ <b>Scan Complete:</b> No high-confidence signals found this hour.")

    count = len(top_signals)
    header = f"🏆 <b>Top {count} Market Setups</b>\n<i>Processed from the entire market.</i>\n\n"
    
    body = ""
    for i, sig in enumerate(top_signals):
        ticker = sig.get("symbol", sig.get("ticker", "?"))
        score = sig.get("confidence_score", 0)
        stype = sig.get("signal_type", sig.get("type", "unknown")).replace("_", " ").title()
        tf = sig.get("timeframe", "?")
        
        # Extract trade setup
        entry = sig.get("entry")
        sl = sig.get("stop_loss")
        tp1 = sig.get("target_1")
        tp2 = sig.get("target_2")
        rr = sig.get("risk_reward_1", 0)
        
        body += (
            f"{i+1}. <b>{ticker}</b> ({stype} @ {tf})\n"
            f"🎯 Score: <code>{score:.1%}</code> | RR: <code>{rr}</code>\n"
            f"📥 Buy Area: <code>{entry}</code>\n"
            f"🛡 SL: <code>{sl}</code> | 🚀 TP1: <code>{tp1}</code> | 🔥 TP2: <code>{tp2}</code>\n\n"
        )

    footer = f"🔗 View details at <a href='http://localhost:8000'>Dashboard</a>"
    
    return await send_telegram_alert(header + body + footer)
