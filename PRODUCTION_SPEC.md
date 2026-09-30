# M45 Morning Dashboard — Production Specification

## 1. Architecture

Use a **static website**, not a local Flask server.

- Private GitHub repository stores code.
- GitHub Actions runs the Python data builder on a schedule.
- The builder writes `site/data/dashboard.json`.
- The action commits only the refreshed JSON.
- Cloudflare Pages watches the repository and instantly deploys the static `site/` folder.
- Optional Cloudflare Access protects the `*.pages.dev` site by email login.

This means the end user only opens a normal URL. No PowerShell, Python, localhost, or always-on server is required.

## 2. Data policy

There is no single truly unlimited, free, official, real-time market-data API covering all assets.

Therefore the dashboard is designed as a **Singapore-morning research snapshot**:

- U.S. equities / ETFs: latest available daily bar.
- Asia / futures / FX / commodities: latest available daily/partial daily bar from the public quote feed.
- Treasury curve: official U.S. Treasury.
- Real yields / credit OAS: FRED.
- Term premium: New York Fed ACM model.
- FedWatch: free reconstruction when available; it must never be labeled as the official live CME API.
- CVOL: public CME aggregate when parseable.
- Calendar: BLS / BEA / ISM / Federal Reserve / TreasuryDirect.
- Company filings: SEC EDGAR may be added as a catalyst confirmation source.

## 3. Return calculations — non-negotiable

Never use Yahoo `chartPreviousClose` for multi-month chart queries.

For a clean adjusted daily price series `P[t]`:

- 1D = P[t] / P[t-1] - 1
- 1W = P[t] / P[t-5] - 1
- 1M = P[t] / P[t-21] - 1
- 3M = P[t] / P[t-63] - 1
- 6M = P[t] / P[t-126] - 1
- YTD = P[t] / last trading close of prior calendar year - 1
- 60D annualized vol = stdev(last 60 daily returns) × sqrt(252)

Use adjusted closes for stocks and ETFs to neutralize splits and distributions where the provider supplies them.

## 4. Data-quality gates

The dashboard must prefer “missing” over “wrong”.

1D sanity thresholds trigger a warning and blank the 1D print:
- Broad equity index: 20%
- FX: 10%
- Commodity: 30%
- Futures: 20%
- Crypto: 40%
- ETF: 30%
- Single stock: 65%

Any invalid number is excluded from:
- takeaways,
- regime scores,
- mover rankings,
- tactical allocation,
- commentary.

## 5. Core stock coverage

Always show:
- Compute / memory / equipment:
  NVDA, AMD, AVGO, TSM, MU, ASML, AMAT, LRCX, SK hynix, Samsung.
- Hyperscalers / AI / software:
  MSFT, AMZN, GOOGL, META, ORCL, PLTR, SNOW, NOW, CRM, ADBE.
- Data center / power:
  EQIX, DLR, CRWV, VRT, ETN, GEV, CEG, NRG.

A core name is “material” if any condition is true:
- |1D| >= 1.5%
- |1D relative to benchmark| >= 1.0%
- |1W| >= 4.0%

The name remains visible even when not material.

## 6. Broader market movers

Use a curated liquid large-cap universe across all U.S. sectors.

A name appears only if:
- |1D| >= 3.0%, OR
- |1D relative to sector ETF| >= 2.0%.

Ranking score:

Score = 65% × |1D| + 25% × |sector-relative 1D| + 10% × min(|1W|, 8%)

No random small-cap top-gainers.

## 7. Catalyst rules

Never write filler such as:
“No clear catalyst; check sector / factor move.”

Display a catalyst only when a recent headline can be classified into a concrete category such as:
- Earnings / guidance
- AI / capex
- Analyst action
- Policy / regulatory
- Financing
- M&A / transaction
- Order / customer
- Product

If no supported catalyst exists, leave the catalyst field blank.

A price move and a headline occurring on the same day are not automatically causal. The wording should be:
“Headline linked” / “likely catalyst”, not “caused by”, unless the evidence is very clear.

## 8. Rates logic

Read the curve before explaining equities.

- 2Y leads higher: Fed-path / policy repricing is the first hypothesis.
- 2Y down while 10Y/30Y rise: long-duration pressure; investigate term premium, supply, inflation uncertainty and global duration.
- Both 2Y and 10Y higher:
  - 10Y rises more → bear steepening.
  - 2Y rises more → bear flattening.
- Both lower:
  - 2Y falls more → bull steepening.
  - 10Y falls more → bull flattening.

10Y nominal decomposition:
Nominal 10Y ≈ 10Y real yield + 10Y inflation compensation.

ACM term premium is a separate model estimate that helps explain how much compensation investors require for duration risk. It is not directly observed.

## 9. Credit logic

Credit is a confirmation test.

- Equity down + rates up + IG/HY stable:
  mostly valuation / rates shock.
- Equity down + HY/CCC widening sharply:
  funding / growth / default concern is broadening.
- CCC widening much more than HY, HY more than IG:
  stress is moving down-quality.

FRED OAS is not intraday. Treat it as regime / multi-day confirmation.

## 10. Cross-asset regime

Risk appetite inputs:
- SPY 1M trend
- HYG vs LQD 1M
- VIX level
- 10Y real-yield 1M direction

Growth impulse:
- IWM vs SPY
- XLI vs XLU
- Copper vs Gold

Inflation pressure:
- Brent 1M
- 10Y breakeven change

Missing inputs are excluded and remaining weights are renormalized.

## 11. Tactical OW / Neutral / UW

This is a **systematic tactical market signal**, not a portfolio recommendation.

Composite:
- 50% multi-horizon risk-adjusted trend
- 25% relative strength
- 25% macro/regime alignment

Trend uses 1M / 3M / 6M with 50 / 30 / 20 weights, normalized by 60D annualized volatility.

Signal:
- score >= +25 → OVERWEIGHT
- score <= -25 → UNDERWEIGHT
- otherwise → NEUTRAL

The UI must show the component scores so the label is explainable.

## 12. Commentary engine

Zero-cost production version is **rule-based**, not a live ChatGPT call.

It should generate:
1. Risk tone & breadth
2. Rates & Fed
3. Credit confirmation
4. FX & commodities
5. Sectors & important stocks
6. Tactical allocation
7. Next catalysts / scenario map
8. 60–90 second verbal summary

Rules:
- only validated numbers can enter commentary;
- missing data must lower confidence, not be replaced by guesses;
- no unsupported stock catalyst;
- no repeated textbook filler in the main page;
- concept explanations belong in tooltips / “How to read it” boxes.

If truly LLM-written commentary is desired every morning, that requires a model API and is not an unlimited free service.

## 13. Refresh schedule

Recommended scheduled builds:
- 06:30 SGT — safely after U.S. close year-round
- 08:30 SGT — early Asia session
- 11:30 SGT — Asia mid-session

This is enough for a morning investment dashboard and stays far below free build/minute quotas.

## 14. Failure behavior

If a data source fails:
- keep the last successfully deployed website live;
- the scheduled action does not publish broken output;
- blank the failed metric;
- never substitute an old number without displaying its as-of date;
- never generate commentary from a failed metric.
