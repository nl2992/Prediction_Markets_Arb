"""
Polymarket CLOB & Gamma API client.

Public endpoints — no authentication required.

Base URLs:
  CLOB:  https://clob.polymarket.com
  Gamma: https://gamma-api.polymarket.com
"""

from __future__ import annotations

import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)

CLOB_BASE = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"

DEFAULT_TIMEOUT = 15  # seconds


class PolymarketClient:
    """Thin wrapper around the Polymarket REST APIs."""

    def __init__(self, timeout: int = DEFAULT_TIMEOUT) -> None:
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.timeout = timeout

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, base: str, path: str, params: dict | None = None) -> Any:
        url = f"{base}{path}"
        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def _post(self, base: str, path: str, json_body: Any) -> Any:
        url = f"{base}{path}"
        resp = self.session.post(url, json=json_body, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # ------------------------------------------------------------------
    # Market discovery (Gamma API)
    # ------------------------------------------------------------------

    def get_events(
        self,
        limit: int = 20,
        offset: int = 0,
        active: bool = True,
        closed: bool = False,
    ) -> list[dict]:
        """Return a list of events from the Gamma API."""
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "active": str(active).lower(),
            "closed": str(closed).lower(),
        }
        data = self._get(GAMMA_BASE, "/events", params=params)
        return data if isinstance(data, list) else data.get("events", data)

    def get_markets(
        self,
        limit: int = 20,
        offset: int = 0,
        active: bool = True,
        closed: bool = False,
    ) -> list[dict]:
        """Return a list of markets from the Gamma API."""
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "active": str(active).lower(),
            "closed": str(closed).lower(),
        }
        data = self._get(GAMMA_BASE, "/markets", params=params)
        return data if isinstance(data, list) else data.get("markets", data)

    # ------------------------------------------------------------------
    # Order book (CLOB API)
    # ------------------------------------------------------------------

    def get_orderbook(self, token_id: str) -> dict:
        """
        Fetch the full order book for a single token.

        Returns a dict with keys:
          market    – condition ID
          asset_id  – token ID
          bids      – list of {price, size}  (buy side, highest first)
          asks      – list of {price, size}  (sell side, lowest first)
          tick_size
          min_order_size
          hash
        """
        return self._get(CLOB_BASE, "/book", params={"token_id": token_id})

    def get_orderbooks(self, token_ids: list[str]) -> list[dict]:
        """
        Fetch order books for multiple tokens in a single request (up to 500).

        Each element of token_ids should be a plain token ID string.
        """
        body = [{"token_id": tid} for tid in token_ids]
        return self._post(CLOB_BASE, "/books", json_body=body)

    # ------------------------------------------------------------------
    # Prices (CLOB API)
    # ------------------------------------------------------------------

    def get_price(self, token_id: str, side: str = "BUY") -> dict:
        """
        Get the best available price for a single token.

        side: "BUY" (best ask) or "SELL" (best bid)
        """
        return self._get(
            CLOB_BASE, "/price", params={"token_id": token_id, "side": side}
        )

    def get_midpoint(self, token_id: str) -> dict:
        """Return the midpoint price for a token."""
        return self._get(CLOB_BASE, "/midpoint", params={"token_id": token_id})

    def get_spread(self, token_id: str) -> dict:
        """Return the bid-ask spread for a token."""
        return self._get(CLOB_BASE, "/spread", params={"token_id": token_id})

    # ------------------------------------------------------------------
    # Convenience: enrich market list with live order book data
    # ------------------------------------------------------------------

    def enrich_markets_with_orderbooks(
        self, markets: list[dict], batch_size: int = 50
    ) -> list[dict]:
        """
        Given a list of market dicts (from Gamma API), fetch live order books
        for every token and attach them under the key ``orderbook``.

        Markets without a ``clobTokenIds`` field are skipped.
        """
        enriched: list[dict] = []
        token_to_market: dict[str, dict] = {}

        for mkt in markets:
            raw_ids = mkt.get("clobTokenIds")
            if not raw_ids:
                enriched.append({**mkt, "orderbook": None})
                continue
            # clobTokenIds is a JSON-encoded list stored as a string
            if isinstance(raw_ids, str):
                import json
                try:
                    token_ids = json.loads(raw_ids)
                except Exception:
                    token_ids = [raw_ids]
            else:
                token_ids = raw_ids

            # Use the first token (YES side) as the primary
            primary_token = token_ids[0] if token_ids else None
            if primary_token:
                token_to_market[primary_token] = mkt
            enriched.append({**mkt, "_primary_token": primary_token, "orderbook": None})

        # Batch fetch order books
        token_ids_list = [
            m["_primary_token"]
            for m in enriched
            if m.get("_primary_token")
        ]

        books_by_token: dict[str, dict] = {}
        for i in range(0, len(token_ids_list), batch_size):
            chunk = token_ids_list[i : i + batch_size]
            try:
                books = self.get_orderbooks(chunk)
                for book in books:
                    asset_id = book.get("asset_id")
                    if asset_id:
                        books_by_token[asset_id] = book
            except Exception as exc:
                logger.warning("Batch orderbook fetch failed for chunk: %s", exc)

        # Attach books back to markets
        for mkt in enriched:
            token = mkt.pop("_primary_token", None)
            if token and token in books_by_token:
                mkt["orderbook"] = books_by_token[token]

        return enriched
