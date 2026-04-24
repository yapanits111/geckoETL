from datetime import datetime
from typing import Optional

import requests

from utils.logger import logger


class WebhookNotifier:
    def __init__(self, webhook_url: Optional[str]):
        self.webhook_url = (webhook_url or "").strip()

    def _enabled(self) -> bool:
        return bool(self.webhook_url)

    def send(self, message: str) -> None:
        if not self._enabled():
            logger.info("[WEBHOOK] DISCORD_WEBHOOK_URL not set; skipping webhook message")
            return

        payload = {"content": message}

        try:
            response = requests.post(self.webhook_url, json=payload, timeout=15)
            response.raise_for_status()
            logger.info("[WEBHOOK] Webhook sent successfully")
        except requests.RequestException as exc:
            logger.error(f"[WEBHOOK] Webhook failed: {exc}")

    def pipeline_success(self, records: int, coins: int) -> None:
        self.send(
            "\n".join(
                [
                    "Pipeline complete",
                    f"Records processed: {records}",
                    f"Coins tracked: {coins}",
                    f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                ]
            )
        )

    def price_alert(self, coin: str, change_pct: float) -> None:
        if abs(change_pct) <= 5:
            return

        direction = "UP" if change_pct > 0 else "DOWN"
        self.send(
            "\n".join(
                [
                    f"Price Alert: {coin.upper()}",
                    f"Direction: {direction}",
                    f"Change: {change_pct:.2f}%",
                ]
            )
        )

    def pipeline_failed(self, error: str) -> None:
        self.send(f"Pipeline failed\nError: {error}")
