# Vercel Hobby as an API relay — count Edge Requests first

[🇮🇩 Bahasa Indonesia](vercel_hobby_quota.md) | 🇬🇧 English

A note for **similar projects** that want to use Vercel (Hobby) as a
proxy/relay to a third-party API (Binance, or any API that blocks
Cloudflare Workers). This is not a deploy guide — that lives in
[`proxy/README.en.md`](../proxy/README.en.md). This is a quota lesson that
already happened in this repo.

## The wrong claim

Hobby is **not** “millions of invocations, fine for a personal cron.”

The tight limit on the Usage Overview dashboard is **Edge Requests**, not
CPU and not bandwidth:

| Metric | What it looked like when quota ran out (this incident) |
|---|---|
| Edge Requests | **1M / 1M (100%)** — this is what stops the proxy |
| Fast Data Transfer | 2.16 GB / 100 GB |
| Fast Origin Transfer | 2.1 GB / 10 GB |
| Edge Request CPU Duration | 2 min / 1 h |

CPU and transfer can look idle while **HTTP hit count** is already maxed.
Do not use CPU/GB as a “still safe” signal.

One Edge Request = **one HTTP hit to the Vercel function**, including
failures (wrong-secret 401, 403, 429, 5xx). Failing over to a second proxy
**does not erase** the failed hit on the primary.

Two Vercel projects on the **same Hobby team** share that 1M pool.
Deploying `PROXY_URL` and `PROXY_URL_2` on the same account **does not**
double the quota.

## Do the math before you deploy

Estimate per month, then compare to **1,000,000**:

```
per_month ≈ calls_per_tick × ticks_per_day × 30
```

Crons that **cannot be cached** (order book, aggTrades, snapshots that
must differ every tick) are the expensive ones. Example in this repo
(`src/shared.ts`, `WALL_SCAN_WATCHLIST`):

```
N pairs × 1 depth/minute × 1,440 minutes/day × 30 days
15 pairs → ~648,000/month
50 pairs → ~2,160,000/month  (Hobby is already blown)
```

Add a heavy pipeline (many endpoints per symbol, many times per day).
Rough entry-alert example: 96 ticks/day × 40 pairs × a dozen Binance
calls per pair → tens of thousands to >1M/month **without** the wall
scan.

If the formula lands above ~700k, Hobby will run out before the billing
cycle ends — or in a few days if the crons are dense.

## WhaleScope incident (2026-09-03)

- Project: `whale-binance-proxy` on the Hobby team **Jaringan Semesta Raya**.
- Proxy deployed ~28–29 Aug 2026.
- **3 Sep 2026** dashboard showed Edge Requests **1M / 1M**.
- Burn rate: **~180–200k requests/day** → a month’s quota gone in ~5 days.

Drivers:

1. 1-minute wall scan × 15 pairs, depth `NO_CACHE` (by design).
2. Entry-alert every 15 minutes × 40 pairs (klines, OI, depth, ratios, …).
3. Primary 401s: every call still hits Vercel first, then failovers.
   Failed requests still count.

Effect: MCP tools and crons that go through this proxy can fail or
flap. The Cloudflare Worker **does not** share this quota; the path to
Binance is what dies.

Reset follows the **Vercel billing cycle**, not calendar day 1.

## Checklist for a new project

Before choosing Hobby as the relay:

1. List every cron: interval, symbol count, calls per symbol, which paths
   are `NO_CACHE`.
2. Run the formula above. If it exceeds 1M, **do not** treat Hobby as
   enough.
3. Keep the proxy secret identical on Vercel and on the worker. Repeated
   401s burn quota with no data.
4. Failover: put the second proxy on a **different Vercel account/team**,
   or on a VPS — not a second project on the same Hobby team.
5. Upgrade (Pro) **after** cutting waste, not instead of counting. Pro
   raises the cap; it does not fix 401s or a 1-minute cron you don’t
   need.
6. Watch Usage Overview: Edge Requests first, then Origin Transfer (Hobby
   also has a 10 GB cap that can become the second ceiling if responses
   are large).

If you are already maxed: fix 401s → lower frequency or N for `NO_CACHE`
paths → only then Pro / VPS / a second account.

## In this repo

- Proxy deploy: [`proxy/README.en.md`](../proxy/README.en.md)
- Wall-scan cut 50→15 pairs and the quota rationale: `WALL_SCAN_WATCHLIST`
  comment in `src/shared.ts`
- 401/403/429 failover: `src/binanceProxyClient.ts` (`FAILOVER_STATUS`)
