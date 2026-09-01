// Entry alert (Telegram) buat top-N pair Binance Futures USDT-M by 24h
// quote volume (ENTRY_WATCHLIST_SIZE, entryWatchlist.ts) -- dijalankan Cron
// Trigger terpisah
// (ENTRY_ALERT_CRON, lihat src/index.ts scheduled handler + wrangler.toml),
// offset dari grid `*/5`/`*/15` yang sudah ada supaya gak numpuk rate-limit
// proxy internal (rateLimiter.ts) di tick yang sama.
//
// Reuse LANGSUNG runPipelineForSymbol (src/tools/fullPipeline.ts) -- decision
// chain yang sama persis dengan whalescope_full_pipeline (LONG grid only,
// TRADE/WATCH/NO_TRADE), bukan logic baru. Dedup alert (TRADE dan WATCH,
// NO_TRADE gak pernah alert): kunci composite encode *alert-worthy band*
// (WATCH_MUTED vs WATCH vs WATCH_HIGH_RISK, DCA_WATCH_MUTED vs DCA_WATCH),
// bukan cuma label keputusan engine. Kirim pas TRANSISI band, saat
// lastAlertAt masih null (belum pernah kirim), ATAU cooldown 4 jam sejak
// alert terakhir (reminder, bukan spam tiap tick).
import {
  runTriplePipelineForSymbol,
  type PipelineOpts,
  type SymbolPipelineResult,
  type PrefetchedTickerFunding,
  type TriplePipelineResult,
  type DcaOpts,
} from "../tools/fullPipeline.js";
import { DCA_MODAL_DEFAULT_USD, type DcaHeadResult } from "../dcaPipelineEngine.js";
import type { DcaSmartMoneyResult } from "./dcaSmartMoneyAdapter.js";
import type { TraditionalFuturesResult } from "./traditionalPipelineEngine.js";
import * as binanceProxy from "../binanceProxyClient.js";
import * as d1Client from "../d1Client.js";
import { sendTelegramAlert, escapeMarkdown, formatTraditionalFuturesAlert, type TelegramEnv } from "../telegram.js";
import { selectUsdtPerpetualWatchlist } from "../entryWatchlist.js";
import { rankEntryCandidates, DEFAULT_ENTRY_TOP_N, type EntryRankingInput } from "../entryRanking.js";
import * as kvConfig from "../kvConfig.js";
import { mapWithConcurrency } from "../concurrency.js";
import { TRADE_RANKING_SCORE_THRESHOLD } from "../pipelineEngine.js";
import { toPipelineDecisionLogRow } from "../pipelineDecisionLog.js";
import * as pacing from "../pacing.js";
import { fmtPrice } from "../format.js";
import * as riskCircuit from "../engine/riskCircuitBreaker.js";

// KV key buat tuning N pre-filter Wave 1 TANPA redeploy code (tulis via
// dashboard KV / `wrangler kv key put`). Unset -> DEFAULT_ENTRY_TOP_N.
const ENTRY_TOP_N_KV_KEY = "entry_alert:top_n";
// KV key buat modal referensi head DCA (capital-solve base-order margin).
// Alert tidak punya konteks saldo akun -- ini cuma angka acuan yang
// user-scale. Unset -> DCA_MODAL_DEFAULT_USD ($200).
const ENTRY_DCA_MODAL_KV_KEY = "entry_alert:dca_modal_usd";

async function resolveEntryTopN(): Promise<number> {
  try {
    const raw = await kvConfig.getJson<number>(ENTRY_TOP_N_KV_KEY);
    return typeof raw === "number" && Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : DEFAULT_ENTRY_TOP_N;
  } catch {
    return DEFAULT_ENTRY_TOP_N;
  }
}

async function resolveDcaModalUsd(): Promise<number> {
  try {
    const raw = await kvConfig.getJson<number>(ENTRY_DCA_MODAL_KV_KEY);
    return typeof raw === "number" && Number.isFinite(raw) && raw > 0 ? raw : DCA_MODAL_DEFAULT_USD;
  } catch {
    return DCA_MODAL_DEFAULT_USD;
  }
}

const COOLDOWN_MS = 4 * 60 * 60 * 1000;

// Concurrency rendah (bukan default 6 whalescope_full_pipeline) -- watchlist
// di sini jauh lebih besar (400 vs maks 20/tool-call), jaga jarak dari
// MAX_REQUESTS_PER_WINDOW (rateLimiter.ts).
//
// 4 -> 3 (2026-08-28): tiap whalescope_full_pipeline internal burst ~8 fetch
// paralel (2-wave). 4 pipeline paralel = ~32 request simultan -> spike rate
// yang trip Binance `-1003` walau rata-rata jauh di bawah limit. 3 nurunin
// peak burst ~25%, wall-clock ~8.7 menit (250 pair / 3, masih < cap 15 menit).
// Bagian mitigasi IP rate-ban ([[project_whalescope_vps_ip_ratelimit]]).
const CONCURRENCY = 3;

// PACING -- ditemukan live 2026-08-25 via wrangler tail: tanpa delay ini,
// 355/400 pair di watchlist gagal dalam 1 tick (346 kena RateLimitError
// self-throttle, sisanya bug parsing terpisah) karena seluruh batch nyoba
// habisin ~12-17 call/symbol SEKALIGUS di awal tick, jauh ngelewatin jatah
// per-menit yang dipakai bareng cron lain (rateLimiter.ts). Delay ini
// nge-pace throughput SENDIRI biar sebar sepanjang siklus 15 menit
// (ENTRY_ALERT_CRON), bukan burst di 60 detik pertama.
//
// Perhitungan (worst-case, hard screen lolos = 17 call/symbol):
// - Target throughput entry-alert sendiri: ~1.100-1.200 call/menit (jauh di
//   bawah limit ASLI Binance per-IP ~2400/menit -- proxy Vercel 1 IP dipakai
//   bareng semua cron, BUKAN cuma limiter internal kita).
// - 4 worker (CONCURRENCY) x 17 call / (network time + delay) <= target
//   -> delay ~4 detik/symbol/worker cukup (network time diasumsikan ~0.5-1s,
//   BELUM diukur presisi -- verifikasi live via wrangler tail setelah deploy,
//   sama seperti langkah verifikasi tiap kenaikan watchlist sebelumnya).
// - Total durasi estimasi: 400 pair / 4 worker = 100 putaran x ~4.8 detik
//   = ~8 menit -- jauh di bawah siklus 15 menit ke tick berikutnya.
//
// 5500 -> 2000ms (2026-08-30): Phase 2 cuma TOP 40 (bukan 250 deep-run).
// 40 / CONCURRENCY=3 ≈ 14 putaran x (I/O + 2s) tetap jauh di bawah 3 menit
// wall. CONCURRENCY tetap 3 supaya peak burst Wave 2 gak trip Binance -1003.
export const ENTRY_ALERT_PACING_DELAY_MS = 2000;

// Mirror default zod schema whalescope_full_pipeline (src/tools/fullPipeline.ts)
// -- alert pakai parameter risiko/leverage yang SAMA dengan yang biasa dipakai
// manual lewat tool itu, supaya konsisten.
const DEFAULT_PIPELINE_OPTS: PipelineOpts = {
  riskUsd: 20,
  marginMode: "ISOLATED",
  maxLeverageOptions: [3, 5, 10],
  lookbackBars: 50,
  atrPeriod: 14,
  atrMult: 1.0,
  slExtraAtr: 1.5,
  slPctBuffer: 1.0,
  minQuoteVolumeUsd: 5_000_000,
  maxAbsFundingRate: 0.0005,
};

const ALERTABLE_DECISIONS = new Set(["TRADE", "WATCH"]);

// High-quality WATCH: rankingScore >= 50. TRADE sudah dijamin
// score >= 55 AND SAFE/MODERATE di decidePipelineOutcome. WATCH SAFE/MODERATE
// tetap di-cap < 55 (defensive -- pipeline nyata tidak emit WATCH di skor itu).
// HIGH_RISK + skor >= 50 adalah jalur notif terpisah (bukan TRADE): sinyal
// Tier-1 kuat tapi setup likuidasi/leverage berisiko. HIGH_RISK skor < 50
// tetap di-mute. Tidak mengubah decidePipelineOutcome / ambang 55.
export const WATCH_MIN_ALERT_SCORE = 50;
// Dispatch floor terpisah dari engine DCA_WATCH_MIN_ALERT_SCORE (50) --
// engine tetap boleh WATCH dari 50, Telegram cuma kirim >= 65.
export const DCA_WATCH_TELEGRAM_MIN_SCORE = 65;

function isHighRiskWatch(result: SymbolPipelineResult): boolean {
  return result.decision === "WATCH" && result.risk?.gridRisk?.status === "HIGH_RISK";
}

function isGridAlertWorthy(result: SymbolPipelineResult): boolean {
  if (!ALERTABLE_DECISIONS.has(result.decision)) return false;
  if (result.decision === "TRADE") return true;
  if (result.rankingScore < WATCH_MIN_ALERT_SCORE) return false;
  if (isHighRiskWatch(result)) return true;
  return result.rankingScore < TRADE_RANKING_SCORE_THRESHOLD;
}

// Dedup slot = decision label + alert band. HQ WATCH tetap "WATCH" supaya
// row D1 lama yang sudah pernah alert tidak di-spam ulang pas deploy.
export function gridDedupSlot(result: SymbolPipelineResult): string {
  if (result.decision === "TRADE") return "TRADE";
  if (result.decision !== "WATCH") return result.decision;
  if (!isGridAlertWorthy(result)) return "WATCH_MUTED";
  if (isHighRiskWatch(result)) return "WATCH_HIGH_RISK";
  return "WATCH";
}

export function dcaDedupSlot(dca: DcaHeadResult, dcaSm?: DcaSmartMoneyResult | null): string {
  const decision = dcaSm?.decision ?? dca.decision;
  if (decision === "DCA_WATCH" && !isDcaAlertWorthy(dca, dcaSm)) return "DCA_WATCH_MUTED";
  return decision;
}

function dcaWatchScore(dca: DcaHeadResult, dcaSm?: DcaSmartMoneyResult | null): number {
  return dcaSm ? dcaSm.timingScore : dca.confidence;
}

// DCA_TRADE selalu alert. DCA_WATCH hanya kalau skor Telegram >= 65.
// PAUSE_HARD / STOP / PAUSE_SOFT di-mute (bukan actionable entry).
function isDcaAlertWorthy(dca: DcaHeadResult, dcaSm?: DcaSmartMoneyResult | null): boolean {
  if (dcaSm) {
    if (dcaSm.decision === "DCA_TRADE") return true;
    if (dcaSm.decision === "DCA_WATCH") return dcaWatchScore(dca, dcaSm) >= DCA_WATCH_TELEGRAM_MIN_SCORE;
    return false;
  }
  if (dca.decision === "DCA_TRADE") return true;
  if (dca.decision === "DCA_WATCH") return dca.confidence >= DCA_WATCH_TELEGRAM_MIN_SCORE;
  return false;
}

// Traditional Futures: HANYA TRAD_TRADE yang alert (bracket lolos quality
// filter RR>=1.5 + skenario valid). TRAD_WATCH sengaja TIDAK alert -- head ini
// baru live, jaga volume notif rendah dulu (bisa dilonggarkan nanti seperti
// yang dilakukan buat WATCH grid/DCA).
function isTradAlertWorthy(trad: TraditionalFuturesResult): boolean {
  return trad.decision === "TRAD_TRADE";
}

function dcaHeadDecision(r: TriplePipelineResult): string {
  return r.dcaSm?.decision ?? r.dca.decision;
}

function countTradeHeads(r: TriplePipelineResult): number {
  let n = 0;
  if (r.grid.decision === "TRADE" && isGridAlertWorthy(r.grid)) n += 1;
  if (dcaHeadDecision(r) === "DCA_TRADE" && isDcaAlertWorthy(r.dca, r.dcaSm)) n += 1;
  if (isTradAlertWorthy(r.trad)) n += 1;
  return n;
}

function classifyAlertHeads(r: TriplePipelineResult, muteTrade: boolean): {
  gridOn: boolean;
  dcaOn: boolean;
  tradOn: boolean;
  alertable: boolean;
} {
  const gridOn = isGridAlertWorthy(r.grid) && !(muteTrade && r.grid.decision === "TRADE");
  const dcaOn = isDcaAlertWorthy(r.dca, r.dcaSm) && !(muteTrade && dcaHeadDecision(r) === "DCA_TRADE");
  const tradOn = isTradAlertWorthy(r.trad) && !muteTrade;
  return { gridOn, dcaOn, tradOn, alertable: gridOn || dcaOn || tradOn };
}

// Penanda Telegram: 🟢 TRADE / 🟡 WATCH HQ / ⚠️ HIGH_RISK bersinyal-kuat; 🔵/🟠 DCA.
const GRID_ICON: Record<string, string> = { TRADE: "🟢", WATCH: "🟡" };
const DCA_ICON: Record<string, string> = { DCA_TRADE: "🔵", DCA_WATCH: "🟠", DCA_PAUSE_SOFT: "⏸️", DCA_PAUSE_HARD: "🧊", DCA_STOP: "🚨" };
const GRID_LABEL: Record<string, string> = {
  TRADE: "GRID TRADE (grid entry, whale-aligned)",
  WATCH: "GRID WATCH (mendekati entry, belum layak)",
};
const GRID_HIGH_RISK_LABEL = "GRID HIGH_RISK (sinyal kuat, setup berisiko — jangan eksekusi)";
const DCA_LABEL: Record<string, string> = {
  DCA_TRADE: "DCA LAYAK ENTRY",
  DCA_WATCH: "DCA TUNGGU",
  DCA_PAUSE_SOFT: "DCA PAUSE SOFT",
  DCA_PAUSE_HARD: "DCA PAUSE HARD",
  DCA_STOP: "DCA PLAN INVALIDATED",
};

function gridHeadIcon(result: SymbolPipelineResult): string {
  if (isHighRiskWatch(result)) return "⚠️";
  return GRID_ICON[result.decision] ?? "";
}

function gridHeadLabel(result: SymbolPipelineResult): string {
  if (isHighRiskWatch(result)) return GRID_HIGH_RISK_LABEL;
  return GRID_LABEL[result.decision] ?? result.decision;
}

function formatEntryAlert(r: TriplePipelineResult, muteTrade = false): string {
  const { grid, dca, trad, dcaSm } = r;
  const { gridOn, dcaOn, tradOn } = classifyAlertHeads(r, muteTrade);
  const sm = grid.tier1?.smartMoney;
  const dcaDir = dca.direction ? ` (${dca.direction})` : "";
  const dcaHeadDecision = dcaSm?.decision ?? dca.decision;
  const dcaHeadLabel = DCA_LABEL[dcaHeadDecision] ?? dcaHeadDecision;

  const headMarkers =
    `${gridOn ? gridHeadIcon(grid) : ""}${dcaOn ? DCA_ICON[dcaHeadDecision] ?? "" : ""}${tradOn ? "⚡" : ""}` || "ℹ️";
  const headParts: string[] = [];
  if (gridOn) headParts.push(escapeMarkdown(gridHeadLabel(grid)));
  if (dcaOn) headParts.push(`${escapeMarkdown(dcaHeadLabel)}${dcaDir}`);
  if (tradOn) headParts.push(`TRADITIONAL FUTURES (${escapeMarkdown(`[SCENARIO: ${trad.scenario}]`)})`);

  const lines = [
    `${headMarkers} *${escapeMarkdown(grid.symbol)}* — ${headParts.join(" · ")}`,
    dcaSm
      ? `📊 Grid ${grid.rankingScore.toFixed(1)}/100 · DCA SM timing ${dcaSm.timingScore.toFixed(0)}/100 · safety ${dcaSm.safetyScore.toFixed(0)}/100 · VolTier ${dca.volTier}`
      : `📊 Grid ${grid.rankingScore.toFixed(1)}/100 · DCA ${dca.confidence}/100 · VolTier ${dca.volTier}`,
  ];
  if (sm) {
    lines.push(
      `🐋 ${escapeMarkdown(sm.condition)} · SM Bias ${escapeMarkdown(sm.smartMoneyBias)} vs Retail ${escapeMarkdown(sm.retailSentiment)}`,
    );
  }

  // ── GRID block ──
  const g = grid.gridBotConfig;
  if (gridOn && g) {
    lines.push(
      "",
      "📈 GRID",
      `   Range ${fmtPrice(g.lower)} – ${fmtPrice(g.upper)} (${escapeMarkdown(g.gridType)}, ${g.gridCount} grid)`,
      `   Lev ${g.leverage ?? "-"}x ${escapeMarkdown(g.marginMode)} · SL ${fmtPrice(g.stopLoss)} · TP ${fmtPrice(g.takeProfit)}`,
    );
  } else if (!gridOn) {
    lines.push("", `GRID: ${escapeMarkdown(grid.decision)}${grid.hardScreen.reasons[0] ? ` (${escapeMarkdown(grid.hardScreen.reasons[0].slice(0, 80))})` : ""}`);
  }

  // ── DCA block ──
  const d = dca.dcaBotConfig;
  if (dcaSm && dcaOn) {
    lines.push(
      "",
      `🔷 DCA Smart Money V3 (${dcaSm.entryCount + 1}/${dcaSm.maxEntries}${dcaDir})`,
      `   Timing ${dcaSm.timingScore.toFixed(0)}/100 · Safety ${dcaSm.safetyScore.toFixed(0)}/100 · Pause ${escapeMarkdown(dcaSm.pauseLevel)}`,
      `   Interval ${dcaSm.intervalPct.toFixed(2)}% · Next trigger ${fmtPrice(dcaSm.nextTriggerPrice)}`,
      dcaSm.pauseReason ? `   ⏸ ${escapeMarkdown(dcaSm.pauseReason)}` : "",
    );
    if (dcaSm.decision === "DCA_STOP") {
      lines.push("   🚨 \\[DCA PLAN INVALIDATED \\- MANUAL REVIEW REQUIRED\\]");
    }
  }
  if (dcaOn && d) {
    lines.push(
      "",
      `🔷 DCA (${d.direction}, Moderate)`,
      `   Price drop step ${d.priceDropStepPct}% · dev ×${d.priceDeviationMultiplier} · maks ${d.maxDcaOrders} order`,
      `   TP/round ${d.takeProfitPerRoundPct}% · Lev ${d.leverage}x`,
      `   Base/DCA order ${d.baseOrderMarginUsd} USDT (modal ref $${d.modalRefUsd})`,
      `   SL ${d.stopLossPct}% (${fmtPrice(d.stopLossPrice)}) · est. liq ~${fmtPrice(d.estLiquidationPrice)} · proj. max loss $${d.projectedMaxLossUsd}`,
      `   Total accumulation Base→Max DCA: ${d.totalAccumulationDistPct}%`,
      "   ⚠️ taker-ratio & wall-persistence di-proxy; " + (dca.effCapAdx1d ? `1D cap ${dca.effCapAdx1d}` : "1D cap n/a"),
    );
  } else if (dcaOn && dca.decision === "DCA_WATCH" && !dcaSm) {
    lines.push("", `🟠 DCA TUNGGU${dcaDir} — confidence ${dca.confidence}/100${dca.rejectReason ? ` (${escapeMarkdown(dca.rejectReason)})` : ""}`);
  } else if (!dcaOn) {
    lines.push(`DCA: Tolak${dca.rejectReason ? ` (${escapeMarkdown(dca.rejectReason)})` : ""}`);
  }

  // ── TRADITIONAL FUTURES block ──
  if (tradOn) {
    lines.push("", formatTraditionalFuturesAlert(grid.symbol, trad, grid, dca));
  }

  return lines.join("\n");
}

export interface AlertCheckOutcome {
  gridDecision: SymbolPipelineResult["decision"];
  dcaDecision: DcaHeadResult["decision"];
  tradDecision: TraditionalFuturesResult["decision"];
  hadError: boolean;
  /** Compact grid decision for pipeline_decision_log -- absent kalau pipeline throw sebelum ada result. */
  decisionLog?: ReturnType<typeof toPipelineDecisionLogRow>;
}

export async function checkEntryAlertForSymbol(
  symbol: string,
  env: TelegramEnv,
  now: number = Date.now(),
  prefetched?: PrefetchedTickerFunding,
  dcaOpts: DcaOpts = { modalAvailableUsd: DCA_MODAL_DEFAULT_USD },
): Promise<AlertCheckOutcome> {
  const r = await runTriplePipelineForSymbol(symbol, DEFAULT_PIPELINE_OPTS, dcaOpts, prefetched);
  // runTriplePipelineForSymbol NEVER throws (catch internal) -- kegagalan masuk
  // lewat grid.error, bukan exception. Log eksplisit supaya kelihatan di tail.
  if (r.grid.error) {
    console.error(`[entry-alert] ${symbol}:`, r.grid.error);
  }
  const previous = await d1Client.getEntryAlertState(symbol);

  // Dedup: composite "gridBand/dcaBand/trad". Transisi = band berubah
  // (WATCH_MUTED→WATCH, WATCH→WATCH_HIGH_RISK, PAUSE_SOFT→DCA_TRADE, dst).
  // lastAlertAt == null + alertable = belum pernah kirim (termasuk row D1
  // racun dari bug lama: label WATCH ditulis padahal skor di bawah floor).
  const composite = `${gridDedupSlot(r.grid)}/${dcaDedupSlot(r.dca, r.dcaSm)}/${r.trad.decision}`;
  const muteTrade = await riskCircuit.isDailyLossCircuitOpen();
  const tradeHeads = countTradeHeads(r);
  if (muteTrade && tradeHeads > 0) {
    await maybeNotifyDailyCircuit(env, now);
  }
  const { alertable } = classifyAlertHeads(r, muteTrade);
  const isTransition = alertable && previous?.lastDecision !== composite;
  const neverAlerted = alertable && previous?.lastAlertAt == null;
  const cooldownExpired =
    alertable && previous?.lastAlertAt != null && now - previous.lastAlertAt > COOLDOWN_MS;

  const outcome: AlertCheckOutcome = {
    gridDecision: r.grid.decision,
    dcaDecision: r.dca.decision,
    tradDecision: r.trad.decision,
    hadError: r.grid.error != null,
    decisionLog: toPipelineDecisionLogRow(r.grid, now, "entry_alert"),
  };

  if (alertable && (isTransition || cooldownExpired || neverAlerted)) {
    await sendTelegramAlert(env, formatEntryAlert(r, muteTrade));
    await d1Client.upsertEntryAlertState({ symbol, lastDecision: composite, lastAlertAt: now });
    if (!muteTrade && tradeHeads > 0) {
      await riskCircuit.recordTradeAlert(DEFAULT_PIPELINE_OPTS.riskUsd, tradeHeads, now);
    }
    await persistDcaActivePlan(symbol, r);
    return outcome;
  }

  await d1Client.upsertEntryAlertState({
    symbol,
    lastDecision: composite,
    lastAlertAt: previous?.lastAlertAt ?? null,
  });
  await persistDcaActivePlan(symbol, r);
  return outcome;
}

async function persistDcaActivePlan(symbol: string, r: TriplePipelineResult): Promise<void> {
  const { dca, dcaSm } = r;
  if (!dcaSm || !dca.direction) return;
  try {
    if (dcaSm.decision === "DCA_STOP") {
      await d1Client.deleteDcaActivePlan(symbol, dca.direction);
      return;
    }
    const existing = await d1Client.getDcaActivePlan(symbol, dca.direction);
    await d1Client.upsertDcaActivePlan({
      symbol,
      side: dca.direction,
      entryCount: existing?.entryCount ?? dcaSm.entryCount,
      maxEntries: dcaSm.maxEntries,
      nextTriggerPrice: dcaSm.nextTriggerPrice,
      intervalPct: dcaSm.intervalPct,
      pauseStatus: dcaSm.pauseLevel === "NONE" ? "NONE" : dcaSm.pauseLevel,
      pauseReason: dcaSm.pauseReason,
      avgEntryPrice: existing?.avgEntryPrice ?? null,
      totalInvested: existing?.totalInvested ?? null,
      lastEntryAt: existing?.lastEntryAt ?? null,
    });
  } catch (err) {
    console.error(`[entry-alert] D1 dca_active_plans ${symbol}:`, (err as Error)?.message ?? String(err));
  }
}

interface WatchlistBundle {
  watchlist: string[];
  prefetched: PrefetchedTickerFunding | undefined;
  tickerBySymbol: Map<string, binanceProxy.Ticker24hr>;
}

async function maybeNotifyDailyCircuit(env: TelegramEnv, now: number): Promise<void> {
  const state = await riskCircuit.getDailyLossCircuit();
  if (!riskCircuit.shouldNotifyDailyLoss(state, now)) return;
  await sendTelegramAlert(
    env,
    `🚨 *Circuit Breaker*: daily loss limit tercapai (count ${state?.count ?? 0} / ${riskCircuit.DAILY_LOSS_COUNT_LIMIT} atau total_loss $${state?.total_loss ?? 0} / $${riskCircuit.DAILY_LOSS_USD_LIMIT}). TRADE alert di-mute sampai window 24 jam roll-off atau \`whalescope_risk_circuit\` reset_daily. High-quality WATCH tetap boleh.`,
  );
  await riskCircuit.markDailyLossNotified(now);
}

async function maybeNotifyMacroPause(env: TelegramEnv, now: number): Promise<void> {
  const state = await riskCircuit.getMacroRiskCircuit();
  if (!riskCircuit.shouldNotifyMacro(state, now)) return;
  const reason = state?.reason ? ` Alasan: ${state.reason}.` : "";
  await sendTelegramAlert(
    env,
    `⏸️ *Macro Risk Switch*: entry-alert Phase 2 di-pause.${reason} Nyalakan lagi lewat \`whalescope_risk_circuit\` action=set_macro active=false.`,
  );
  await riskCircuit.markMacroNotified(now);
}

// Resolve watchlist + prefetch Map ticker24hr/premiumIndex dalam SATU set
// fetch per tick. Dulu 4 subrequest: getFuturesExchangeInfo +
// getAllTicker24hrNative (di getTopUsdtPerpetualWatchlist) + LAGI
// getAllTicker24hrNative + getBulkFundingRatesNative (di fetchBulkTickerFunding
// lama). Sekarang 3: exchangeInfo + ticker24hr + premiumIndex, masing-masing
// SEKALI -- response ticker24hr yang SAMA dipakai buat seleksi watchlist DAN
// Map prefetch.
//
// exchangeInfo + ticker24hr WAJIB sukses: tanpa keduanya tidak ada watchlist
// dan tick tidak bisa jalan (perilaku sama dengan getTopUsdtPerpetualWatchlist
// lama yang throw). premiumIndex TERPISAH try/catch: gagal di situ = prefetched
// undefined -> runPipelineForSymbol jatuh balik ke fetch ticker+funding
// per-symbol (PrefetchedTickerFunding opsional), BUKAN menggagalkan tick.
async function resolveWatchlistAndPrefetch(): Promise<WatchlistBundle> {
  const [exchangeInfo, tickerList] = await Promise.all([
    binanceProxy.getFuturesExchangeInfo(),
    binanceProxy.getAllTicker24hrNative(),
  ]);
  const watchlist = selectUsdtPerpetualWatchlist(exchangeInfo.symbols, tickerList);

  const tickerBySymbol = new Map(tickerList.map((t) => [t.symbol, t]));
  let prefetched: PrefetchedTickerFunding | undefined;
  try {
    const fundingList = await binanceProxy.getBulkFundingRatesNative();
    prefetched = {
      ticker: tickerBySymbol,
      funding: new Map(fundingList.map((f) => [f.symbol, f])),
    };
  } catch (err) {
    console.error(
      "[entry-alert] gagal bulk fetch premiumIndex, fallback ke call per-symbol:",
      (err as Error)?.message ?? String(err),
    );
    prefetched = undefined;
  }
  return { watchlist, prefetched, tickerBySymbol };
}

// Phase 1 — F3 cheap grid score over the 250-volume universe, then TOP-N
// (default 40). Fail-closed: kalau premiumIndex gagal, ranking tetap jalan
// ticker-only (fundingAbs=0 → funding factor netral). JANGAN deep-run 250.
async function runPhase1Prefilter(
  watchlist: string[],
  tickerBySymbol: Map<string, binanceProxy.Ticker24hr>,
  fundingBySymbol: Map<string, binanceProxy.PremiumIndexPoint> | undefined,
  now: number,
): Promise<string[]> {
  const topN = await resolveEntryTopN();
  if (topN >= watchlist.length) return watchlist;

  if (!fundingBySymbol) {
    console.error(
      "[entry-alert] premiumIndex tidak tersedia -- Phase 1 tetap cut top-N pakai ticker-only F3 (fundingAbs=0)",
    );
  }

  const candidates: EntryRankingInput[] = watchlist.map((symbol) => {
    const funding = fundingBySymbol?.get(symbol);
    const ticker = tickerBySymbol.get(symbol);
    const fundingAbs = funding ? Math.abs(parseFloat(funding.lastFundingRate)) : 0;
    const priceChangePct24h = ticker ? parseFloat(ticker.priceChangePercent) : 0;
    const quoteVolumeUsd = ticker ? parseFloat(ticker.quoteVolume) : 0;
    return {
      symbol,
      quoteVolumeUsd: Number.isFinite(quoteVolumeUsd) ? quoteVolumeUsd : 0,
      fundingAbs: Number.isFinite(fundingAbs) ? fundingAbs : 0,
      priceChangePct24h: Number.isFinite(priceChangePct24h) ? priceChangePct24h : 0,
    };
  });

  const selected = rankEntryCandidates(candidates, topN);
  const selectedSet = new Set(selected);
  const skipped = watchlist.filter((s) => !selectedSet.has(s));

  await d1Client
    .insertEntryAlertSkipLog({ runAt: now, skippedSymbols: skipped, topN })
    .catch((err) => console.error("[entry-prefilter] gagal insert entry_alert_skip_log:", (err as Error)?.message ?? String(err)));
  console.log(`[entry-prefilter] phase1 top_n=${topN} analysed=${selected.length} skipped=${skipped.length} skipped_symbols=${skipped.join(",")}`);

  return selected;
}

export async function runEntryAlertCheck(env: TelegramEnv): Promise<void> {
  const now = Date.now();
  if (await riskCircuit.isMacroRiskActive()) {
    await maybeNotifyMacroPause(env, now);
    await d1Client.insertEntryAlertRunLog({
      runAt: now,
      total: 0,
      errors: 0,
      watchCount: 0,
      tradeCount: 0,
      dcaWatchCount: 0,
      dcaTradeCount: 0,
      tradWatchCount: 0,
      tradTradeCount: 0,
    });
    console.log("[entry-alert] macro risk circuit active -- skip Phase 2");
    return;
  }

  const { watchlist, prefetched, tickerBySymbol } = await resolveWatchlistAndPrefetch();
  const analysed = await runPhase1Prefilter(watchlist, tickerBySymbol, prefetched?.funding, now);
  const dcaOpts: DcaOpts = { modalAvailableUsd: await resolveDcaModalUsd() };
  // Phase 2 — triple pipeline (Grid + DCA + Trad) HANYA pada top-N Phase 1.
  const outcomes = await mapWithConcurrency(analysed, CONCURRENCY, async (symbol): Promise<AlertCheckOutcome> => {
    try {
      return await checkEntryAlertForSymbol(symbol, env, now, prefetched, dcaOpts);
    } catch (err) {
      console.error(`[cron] gagal entry-alert check ${symbol}:`, (err as Error)?.message ?? String(err));
      return { gridDecision: "NO_TRADE", dcaDecision: "DCA_NO_TRADE", tradDecision: "TRAD_NO_TRADE", hadError: true };
    } finally {
      await pacing.sleep(ENTRY_ALERT_PACING_DELAY_MS);
    }
  });

  // Rekam tally tick ini -- heartbeatCron.ts (3x/hari) pakai ini buat
  // bedain "market emang sepi" (error rate rendah) vs "backend bermasalah"
  // (error rate tinggi). watch_count/trade_count = GRID (nama kolom lama);
  // dca_* = head DCA; trad_* = head Traditional/Smart-Money futures
  // (kolom trad_* ditambah migration 0009). Semua observability.
  const tradTradeCount = outcomes.filter((o) => o.tradDecision === "TRAD_TRADE").length;
  const tradWatchCount = outcomes.filter((o) => o.tradDecision === "TRAD_WATCH").length;
  console.log(`[entry-alert] trad tally: TRAD_TRADE=${tradTradeCount} TRAD_WATCH=${tradWatchCount}`);
  await d1Client.insertEntryAlertRunLog({
    runAt: now,
    total: outcomes.length,
    errors: outcomes.filter((o) => o.hadError).length,
    watchCount: outcomes.filter((o) => o.gridDecision === "WATCH").length,
    tradeCount: outcomes.filter((o) => o.gridDecision === "TRADE").length,
    dcaWatchCount: outcomes.filter((o) => o.dcaDecision === "DCA_WATCH").length,
    dcaTradeCount: outcomes.filter((o) => o.dcaDecision === "DCA_TRADE").length,
    tradWatchCount,
    tradTradeCount,
  });

  const decisionLogs = outcomes.flatMap((o) => (o.decisionLog ? [o.decisionLog] : []));
  await d1Client
    .insertPipelineDecisionLogs(decisionLogs)
    .catch((err) => console.error("[entry-alert] gagal insert pipeline_decision_log:", (err as Error)?.message ?? String(err)));
}
