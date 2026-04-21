"""
Multi-timeframe ranking and signal aggregation.
Ranks stocks based on aggregate confidence across all signals and timeframes.
"""
import logging
from collections import defaultdict
import json

from backend.config import MULTI_TF_BONUS_MULTIPLIER

logger = logging.getLogger(__name__)


async def rank_and_store_signals(db, ticker: str, signals: list[dict]):
    """
    Process, rank, and store all signals for a ticker.
    (Asynchronous version)
    """
    if not signals:
        return

    ticker_id = await db.get_or_create_ticker(ticker)

    # Deactivate old signals for this ticker
    await db.deactivate_old_signals(ticker, days_old=30)

    # Group by signal_type
    by_type = defaultdict(list)
    for sig in signals:
        by_type[sig["type"]].append(sig)

    # Apply multi-indicator bonus
    for signal_type, sigs in by_type.items():
        if signal_type in ("bullish_divergence", "hidden_bullish_divergence"):
            # Count unique indicators that detected this pattern
            indicators_per_tf = defaultdict(set)
            for s in sigs:
                tf = s.get("timeframe", "")
                ind = s.get("indicator", "")
                indicators_per_tf[tf].add(ind)

            for s in sigs:
                tf = s.get("timeframe", "")
                num_ind = len(indicators_per_tf.get(tf, set()))
                if num_ind >= 3:
                    s["multi_indicator_bonus"] = 1.0
                elif num_ind >= 2:
                    s["multi_indicator_bonus"] = 0.7
                else:
                    s["multi_indicator_bonus"] = 0.0

                # Recalculate confidence with bonus
                # NOTE: We assume confidence score was already calculated, 
                # but we boost it here for redundancy/agreement.
                old_score = s.get("confidence_score", 0)
                bonus = s["multi_indicator_bonus"] * 0.20
                s["confidence_score"] = min(round(old_score + bonus, 4), 1.0)

    # Apply multi-TF bonus
    for signal_type, sigs in by_type.items():
        unique_tfs = set(s.get("timeframe", "") for s in sigs)
        if len(unique_tfs) >= 2:
            for s in sigs:
                s["confidence_score"] = min(
                    round(s.get("confidence_score", 0) * MULTI_TF_BONUS_MULTIPLIER, 4),
                    1.0,
                )
                s["multi_tf_confirmed"] = True
                s["confirmed_timeframes"] = list(unique_tfs)
        else:
            for s in sigs:
                s["multi_tf_confirmed"] = False

    # Store all signals asynchronously
    for sig in signals:
        await db.store_signal(ticker_id, ticker, sig)

    logger.info(
        f"Stored {len(signals)} signals for {ticker} "
        f"(types: {', '.join(by_type.keys())})"
    )


async def get_ranked_results(db, **filters) -> list[dict]:
    """
    Get ranked screening results, sorted by confidence score.
    (Asynchronous version)
    """
    signals = await db.get_active_signals(**filters)

    # Group by ticker for aggregation
    by_ticker = defaultdict(list)
    for s in signals:
        by_ticker[s["symbol"]].append(s)

    # Build ranked list with best signal per ticker
    ranked = []
    for symbol, ticker_sigs in by_ticker.items():
        best_signal = max(ticker_sigs, key=lambda x: x.get("confidence_score", 0))
        best_signal["signal_count"] = len(ticker_sigs)
        best_signal["all_signal_types"] = list(set(
            s["signal_type"] for s in ticker_sigs
        ))
        best_signal["all_timeframes"] = list(set(
            s["timeframe"] for s in ticker_sigs
        ))
        
        # Metadata logic (flattening already mostly handled by DB but for robustness):
        additional_data = best_signal.get("additional_data", {})
        if isinstance(additional_data, str):
            try: additional_data = json.loads(additional_data)
            except: additional_data = {}
            
        trade_setup = additional_data.get("trade_setup", {}) if isinstance(additional_data, dict) else {}
        
        # Priority mapping to top-level if not already there
        if "entry" not in best_signal or best_signal["entry"] is None:
            best_signal["entry"] = trade_setup.get("entry")
            best_signal["stop_loss"] = trade_setup.get("stop_loss")
            best_signal["target_1"] = trade_setup.get("target_1")
            best_signal["target_2"] = trade_setup.get("target_2")
            best_signal["risk_reward_1"] = trade_setup.get("risk_reward_1")

        ranked.append(best_signal)

    # Sort by confidence score
    ranked.sort(key=lambda x: x.get("confidence_score", 0), reverse=True)

    # Add rank number
    for i, item in enumerate(ranked):
        item["rank"] = i + 1

    return ranked
