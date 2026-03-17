# Prediction Market Data Pipeline

A lightweight, dependency-minimal Python pipeline that pulls **buy/sell order book data** from [Polymarket](https://polymarket.com) and [Kalshi](https://kalshi.com) and normalises them into a common, easy-to-use schema. All data is fetched from public, unauthenticated API endpoints.

---

## Key Features

- **Dual Exchange Support**: Fetch data from both Polymarket and Kalshi.
- **Normalised Schema**: Raw data from both sources is mapped to a unified `MarketSnapshot` schema, making cross-exchange analysis simple.
- **Full Order Book Depth**: Fetches and parses the complete order book for both exchanges, not just the top-of-book.
- **Public Data Only**: No API keys or authentication required. All endpoints are public.
- **Lightweight**: The only required dependency is `requests`.
- **Flexible Usage**: Can be run from the command line or imported as a Python module.
- **Clear & Verified Logic**: Includes detailed explanations of how binary market order books are interpreted and normalized.

---

## Project Structure

```
prediction_pipeline/
├── pipeline.py          # Main orchestrator — run this file
├── smoke_test.py        # Quick test to verify connectivity and parsing
├── requirements.txt     # Dependency file
├── polymarket/
│   ├── __init__.py
│   └── client.py        # Polymarket CLOB + Gamma API client
├── kalshi/
│   ├── __init__.py
│   └── client.py        # Kalshi Trade API v2 client
└── output/              # JSON snapshots are written here by default
```

---

## Installation

The only required dependency is the `requests` library.

```bash
pip install -r requirements.txt
```

---

## How It Works

The pipeline queries the public APIs of Polymarket and Kalshi, fetches active markets, retrieves their order books, and normalizes the data into a common format.

### Polymarket

Polymarket data is sourced from two separate public APIs:

1.  **Gamma API** (`https://gamma-api.polymarket.com`): Used for market discovery. The pipeline calls the `GET /markets` endpoint to get a list of active markets. Each market object from this API contains essential metadata like the question, resolution source, and, crucially, the `clobTokenIds`.

2.  **CLOB API** (`https://clob.polymarket.com`): Used for fetching live order book data. For each market found via the Gamma API, the pipeline uses the `clobTokenIds` (which represent the YES and NO outcomes) to query the `POST /books` endpoint. This endpoint efficiently returns the full order book (all bid and ask levels) for multiple tokens in a single batch request.

The `polymarket/client.py` module handles the logic of first fetching markets and then enriching them with their corresponding order books.

### Kalshi

Kalshi data is sourced from their public v2 Trade API (`https://api.elections.kalshi.com/trade-api/v2`).

1.  **Market Discovery**: The pipeline calls `GET /markets` to find active markets. To get meaningful, liquid markets, it's recommended to filter by a `series_ticker` (e.g., `KXARTEMISII` for Artemis II launch markets), as the default sort order returns the newest, often illiquid, markets first.

2.  **Order Book Fetching**: For each market, the pipeline calls the `GET /markets/{ticker}/orderbook` endpoint. **This endpoint is public and does not require authentication**. It returns the full depth of all resting **bids** for both the YES and NO sides of the market.

#### Kalshi Order Book Logic

Kalshi's API provides a unique challenge: it only returns bid levels. For a binary market, the ask levels for one side are derived from the bid levels of the other side. The relationship is:

> **A bid to buy a NO contract at price `Q` is equivalent to an offer (an ask) to sell a YES contract at price `1 - Q`**.

Therefore, the pipeline reconstructs the full order book for the **YES side** as follows:

-   **Bids (to buy YES)**: These are taken directly from the `yes_dollars` array in the order book response.
-   **Asks (to sell YES)**: These are derived from the `no_dollars` array. Each NO bid `[price, size]` is converted to a YES ask at `price = 1.0 - no_bid_price` with the same `size`.

The `kalshi/client.py` and `pipeline.py` modules contain the implementation of this logic.

---

## Data Schema

All data is normalized into a `MarketSnapshot` object, which contains an `OrderBook` object. This provides a consistent structure regardless of the source.

### `MarketSnapshot`

| Field | Type | Description |
|---|---|---|
| `source` | `str` | `"polymarket"` or `"kalshi"` |
| `market_id` | `str` | The unique identifier for the market (Condition ID for Polymarket, Ticker for Kalshi). |
| `event_id` | `str` | The parent event identifier (slug for Polymarket, event_ticker for Kalshi). |
| `title` | `str` | The human-readable question of the market. |
| `status` | `str` | The current status of the market (e.g., `open`, `active`). |
| `close_time` | `str` | The ISO 8601 timestamp for when the market closes. |
| `fetched_at` | `str` | The ISO 8601 UTC timestamp of when the data was fetched. |
| `orderbook` | `OrderBook` | The normalized order book object (see below). |
| `last_price` | `float` | The price of the last trade. |
| `volume_24h` | `float` | The trading volume over the last 24 hours. |
| `extra` | `dict` | A dictionary containing source-specific raw fields for deeper analysis. |

### `OrderBook`

| Property | Type | Description |
|---|---|---|
| `bids` | `list[PriceLevel]` | A list of `PriceLevel(price, size)` objects for the buy side, sorted with the best (highest) price first. |
| `asks` | `list[PriceLevel]` | A list of `PriceLevel(price, size)` objects for the sell side, sorted with the best (lowest) price first. |
| `best_bid` | `float` | The highest bid price. |
| `best_ask` | `float` | The lowest ask price. |
| `mid` | `float` | The midpoint price: `(best_bid + best_ask) / 2`. |
| `spread` | `float` | The difference between the best ask and best bid: `best_ask - best_bid`. |

---

## Usage

You can run the pipeline directly from the command line or import its functions into your own Python scripts.

### Command-Line Interface

The `pipeline.py` script can be run with several arguments to customize its behavior.

**Basic Usage (fetches 20 markets from each):**
```bash
python pipeline.py
```

**Save Output to JSON:**
This will create timestamped JSON files (e.g., `polymarket_20260317T120000Z.json`) in the `output` directory.
```bash
python pipeline.py --output-dir output
```

**Fetch a Specific Kalshi Series:**
This is the recommended way to get meaningful data from Kalshi.
```bash
python pipeline.py --kalshi-series KXARTEMISII --kalshi-limit 5
```

**Full Help:**
```bash
python pipeline.py --help
```

### Programmatic Usage

Import the `run_pipeline` function to integrate the data fetching into your own applications.

```python
from pipeline import run_pipeline

# Fetch 5 markets from the Kalshi Artemis II series and 10 from Polymarket
result = run_pipeline(
    polymarket_limit=10,
    kalshi_limit=5,
    kalshi_series_ticker="KXARTEMISII",
    output_dir="output",
)

# Access the normalized data
print("--- Kalshi Artemis II Markets ---")
for snapshot in result["kalshi"]:
    ob = snapshot.orderbook
    print(f"{snapshot.title[:60]:<60} | Mid: ${ob.mid:.2f}, Spread: ${ob.spread:.2f}")

print("\n--- Polymarket Markets ---")
for snapshot in result["polymarket"]:
    ob = snapshot.orderbook
    print(f"{snapshot.title[:60]:<60} | Mid: ${ob.mid:.2f}, Spread: ${ob.spread:.2f}")
```

---

## Smoke Test

A smoke test is included to quickly verify that your environment is set up correctly and that the APIs are reachable and returning data in the expected format. It fetches a small amount of data from a known liquid market on each exchange and runs a series of assertions.

To run the test:

```bash
python smoke_test.py
```

A successful run will look like this:

```
=== Polymarket smoke test ===
  [PASS] Gamma API: received 3 market(s)
  [PASS] ...

=== Kalshi smoke test ===
  [PASS] GET /markets: received 3 market(s)
  [PASS] ...

=== Summary ===
  Polymarket : PASS
  Kalshi     : PASS

All smoke tests passed.
```

---

## API References

-   **Polymarket**: [docs.polymarket.com](https://docs.polymarket.com/)
-   **Kalshi**: [docs.kalshi.com](https://docs.kalshi.com/)
