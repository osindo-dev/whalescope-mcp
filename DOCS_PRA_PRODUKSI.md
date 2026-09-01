# DOCS_PRA_PRODUKSI: BINANCE FUTURES MCP SERVER

> Status: **PRA-PRODUKSI — belum ada satu pun endpoint trading (signed request) di codebase saat ini.**
> `whalescope-mcp` hari ini murni read-only market data (`fapi.binance.com` public endpoints via `binanceFetcher.ts` / `binanceProxyClient.ts`, tanpa API key, tanpa HMAC signing). Empat tool di dokumen ini (`futures_get_balance`, `futures_get_positions`, `futures_place_order`, `futures_cancel_order`) adalah kapabilitas BARU yang memperkenalkan signed request, API secret, dan — untuk `futures_place_order` — kemampuan memindahkan uang riil. Perlakukan checklist di Bagian 5 sebagai gate wajib, bukan saran.

---

## 1. Ringkasan Arsitektur & Lingkungan (Testnet / Mainnet)

### 1.1 Posisi terhadap codebase eksisting
- Tool baru didaftarkan lewat `registerSafeTool()` (`src/toolWrapper.ts`) seperti tool lain — dapat logging `[tool] name ok/error latency` gratis, dan otomatis tercatat di `binance_get_tool_catalog`.
- Konvensi output pakai `ToolResponseBuilder` (`src/responseBuilder.ts`): `content` markdown + `structuredContent` JSON. Untuk 4 tool ini, `structuredContent` WAJIB berisi field terstruktur (order id, status, harga, dsb) — jangan hanya teks, karena caller (agent AI) butuh parse programatik untuk keputusan lanjut.
- State risk-engine disimpan di Workers KV (`CONFIG_KV`, lewat `kvConfig.ts`) mengikuti pola `src/engine/riskCircuitBreaker.ts` yang sudah ada (`state:daily_loss_circuit`, `state:macro_risk_circuit`). Circuit breaker BARU untuk trading (lihat Bagian 3) menambah key senada, bukan menggantikan yang eksisting.
- Client HTTP baru (`src/binanceTradingClient.ts`, belum ada) terpisah total dari `binanceFetcher.ts`/`binanceProxyClient.ts` yang sekarang — jangan campur endpoint public (unsigned) dengan endpoint trading (signed) dalam satu module, supaya kesalahan konfigurasi tidak bisa membuat request public tiba-tiba ter-sign atau sebaliknya.

### 1.2 Environment: Testnet vs Mainnet
| Aspek | Testnet | Mainnet |
|---|---|---|
| Base URL | `https://testnet.binancefuture.com` | `https://fapi.binance.com` |
| API key/secret | Terpisah total dari mainnet — testnet key TIDAK bisa dipakai di mainnet dan sebaliknya | Terpisah total |
| Wrangler secret binding | `BINANCE_TESTNET_API_KEY`, `BINANCE_TESTNET_API_SECRET` | `BINANCE_MAINNET_API_KEY`, `BINANCE_MAINNET_API_SECRET` |
| Default environment | **`testnet` — hard default, tidak boleh diubah lewat argumen tool** | Hanya aktif jika `TRADING_ENV=mainnet` di-set di level Worker environment (`wrangler.toml` `[env.production.vars]`), bukan per-request |
| Switch mainnet | — | Wajib deploy terpisah (`wrangler deploy --env production`) + minimal 1 reviewer manusia approve perubahan `TRADING_ENV`. Tidak boleh ada tool/argumen MCP yang bisa mengubah environment saat runtime. |

**Aturan keras #1 (arsitektur):** environment (`testnet`/`mainnet`) BUKAN parameter Zod di tool manapun. Kalau ia jadi parameter yang bisa diisi caller (termasuk AI agent), satu halusinasi prompt ("switch ke mainnet dan buka posisi") langsung jadi insiden finansial nyata. Environment ditentukan murni dari deployment/binding, dibaca sekali saat `createServer()`, immutable sepanjang request.

### 1.3 Least privilege API key
- API key Binance yang dipakai server WAJIB dibuat dengan permission **Futures trading only** — tanpa withdrawal permission (Binance API Management), tanpa akses transfer antar-wallet. Kompromi key paling buruk = kerugian trading, bukan pencurian dana lewat withdrawal.
- IP whitelist di sisi Binance API Management diarahkan ke IP egress Cloudflare Worker/proxy yang dipakai (lihat pola `binanceProxyClient.ts` untuk isu IP-block fapi.binance.com yang sudah pernah dialami proyek ini — endpoint signed lebih sensitif terhadap ini).

---

## 2. Definisi MCP Tools & Validasi Zod Schema

Semua schema pakai `zod@^3.23.8` (versi yang sudah ada di `package.json`), style `.strict()` dan pesan error eksplisit agar gagal cepat sebelum request keluar ke Binance.

### 2.1 `futures_get_balance`
Read-only, tidak menyentuh risk engine.

```ts
const FuturesGetBalanceInput = z
  .object({
    asset: z.enum(["USDT", "USDC", "BUSD"]).default("USDT"),
  })
  .strict();
```

### 2.2 `futures_get_positions`
Read-only.

```ts
const FuturesGetPositionsInput = z
  .object({
    symbol: z
      .string()
      .regex(/^[A-Z0-9]{5,20}$/, "symbol harus format Binance futures, mis. BTCUSDT")
      .optional(),
  })
  .strict();
```

### 2.3 `futures_place_order` — tool paling berbahaya, validasi paling ketat

```ts
const FuturesPlaceOrderInput = z
  .object({
    symbol: z.string().regex(/^[A-Z0-9]{5,20}$/, "symbol harus format Binance futures, mis. BTCUSDT"),
    side: z.enum(["BUY", "SELL"]),
    positionSide: z.enum(["LONG", "SHORT", "BOTH"]).default("BOTH"),
    type: z.enum(["MARKET", "LIMIT"]),

    // Kuantitas selalu dalam base asset, BUKAN quote (USDT). Tool tidak
    // menerima "quoteOrderQty" -- ambiguitas unit adalah sumber order
    // salah-ukuran yang paling umum.
    quantity: z.number().positive().finite(),

    price: z.number().positive().finite().optional(),

    leverage: z
      .number()
      .int()
      .min(1)
      .max(20, "leverage di atas 20x diblokir di layer MCP, terlepas dari limit exchange"),

    // WAJIB, bukan optional -- lihat Aturan Keras #4 di Bagian 3.
    stopLossPrice: z.number().positive().finite(),

    takeProfitPrice: z.number().positive().finite().optional(),

    timeInForce: z.enum(["GTC", "IOC", "FOK"]).default("GTC"),

    reduceOnly: z.boolean().default(false),

    // Idempotency key wajib -- lihat Aturan Keras #2.
    clientOrderId: z
      .string()
      .min(8)
      .max(36)
      .regex(/^[A-Za-z0-9_-]+$/, "clientOrderId harus alfanumerik, dash, atau underscore"),

    // Caller (AI agent) harus secara eksplisit menyatakan sudah baca
    // angka risiko -- lihat Aturan Keras #5 (confirmation token).
    riskAcknowledgementToken: z.string().min(1),
  })
  .strict()
  .superRefine((data, ctx) => {
    if (data.type === "LIMIT" && data.price === undefined) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: ["price"], message: "price wajib diisi untuk order LIMIT" });
    }
    if (data.type === "MARKET" && data.price !== undefined) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: ["price"], message: "price tidak boleh diisi untuk order MARKET" });
    }
    if (data.side === "BUY" && data.stopLossPrice >= (data.price ?? Infinity) && data.type === "LIMIT") {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: ["stopLossPrice"], message: "stopLossPrice harus di bawah entry price untuk BUY" });
    }
    if (data.side === "SELL" && data.type === "LIMIT" && data.price !== undefined && data.stopLossPrice <= data.price) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, path: ["stopLossPrice"], message: "stopLossPrice harus di atas entry price untuk SELL" });
    }
  });
```

### 2.4 `futures_cancel_order`

```ts
const FuturesCancelOrderInput = z
  .object({
    symbol: z.string().regex(/^[A-Z0-9]{5,20}$/),
    orderId: z.union([z.number().int().positive(), z.string().min(1)]),
  })
  .strict()
  .refine((data) => typeof data.orderId === "number" || data.orderId.length > 0, {
    message: "orderId atau clientOrderId wajib diisi",
  });
```

### 2.5 Struktur error handling standar MCP

Semua 4 tool mengembalikan bentuk yang sama (`ToolResponseBuilder` + `isError`), dibedakan lewat `structuredContent.errorCode` supaya caller AI bisa switch-case tanpa parsing teks:

```ts
interface McpToolError {
  isError: true;
  content: [{ type: "text"; text: string }]; // ringkas, human-readable, Bahasa Indonesia
  structuredContent: {
    errorCode: FuturesErrorCode; // enum tertutup, lihat Bagian 4
    errorSource: "ZOD_VALIDATION" | "RISK_ENGINE" | "BINANCE_API" | "NETWORK" | "INTERNAL";
    binanceCode?: number;     // hanya diisi kalau errorSource === "BINANCE_API"
    retryable: boolean;
    details?: Record<string, unknown>;
  };
}
```

Prinsip: **Zod validation error dan Risk Engine rejection TIDAK PERNAH mencapai Binance API** — keduanya gagal di dalam Worker sebelum `fetch()` ke `fapi.binance.com` dipanggil. Ini memastikan matriks error Binance di Bagian 4 hanya perlu menangani kegagalan yang sudah lolos validasi lokal.

---

## 3. Layer Risk Engine & Guardrails (Rules Keras yang Wajib Dikodekan)

### 3.1 Serangan terhadap rancangan Tahap 1 (celah yang ditemukan)

1. **Halusinasi order AI** — LLM caller bisa memanggil `futures_place_order` dengan quantity yang salah baca dari konteks (mis. salah kira USDT jadi kontrak, salah symbol yang mirip seperti `1000SHIBUSDT` vs `SHIBUSDT`), atau memanggil tool yang sama berkali-kali karena "tidak yakin apakah order tadi berhasil" (retry storm).
2. **Kehabisan margin** — order bisa diterima Zod (angka valid secara matematis) tapi margin akun tidak cukup, atau cukup tapi menghabiskan >90% available balance sehingga posisi lain jadi rawan liquidation cascade.
3. **Kecelakaan leverage** — `leverage: 100` valid secara tipe data tapi menghancurkan akun dalam pergerakan harga <1%. Leverage adalah field paling murah untuk salah ketik (`20` vs `200`) dengan konsekuensi paling mahal.
4. **API rate-limit Binance** — endpoint order (`/fapi/v1/order`) punya weight tersendiri dan order-rate-limit per detik/menit terpisah dari IP-weight; AI agent yang melakukan retry loop atau "order sweep" multi-symbol bisa kena `418 IP ban` dalam hitungan detik.
5. **Ketiadaan Stop Loss** — desain awal Tahap 1 membuat `stopLossPrice` opsional; itu celah terbesar. Posisi tanpa SL yang dibuka oleh agent otonom, lalu sesi berakhir/agent crash, adalah skenario "unbounded loss unattended" — paling berbahaya dari semua risiko di daftar ini.
6. **Race condition antar order** — dua panggilan `futures_place_order` nyaris bersamaan (mis. dari retry) bisa lolos validasi margin masing-masing secara independen (read margin lama, keduanya "cukup") tapi kombinasi keduanya over-leverage.
7. **Symbol/precision mismatch** — quantity atau price yang tidak dibulatkan ke `stepSize`/`tickSize` exchange info akan ditolak Binance (`-1111`), tapi kalau tidak divalidasi di layer kita dulu, tiap kesalahan presisi = 1 round-trip API gagal yang termakan rate limit.

### 3.2 Aturan Keras (Hard-coded Risk Engine Rules)

Modul baru `src/engine/futuresOrderGuard.ts` (paralel dengan `riskCircuitBreaker.ts` dan `gridRiskEngine.ts` yang sudah ada) menjalankan SEMUA rule di bawah **sebelum** `futures_place_order` memanggil Binance. Hasilnya status tertutup, meniru enum yang sudah dipakai `gridRiskEngine.ts`: `"SAFE" | "MODERATE" | "HIGH_RISK" | "REJECT"`. Hanya `SAFE` dan `MODERATE` (dengan warning eksplisit di response) yang lanjut ke API call; `HIGH_RISK` dan `REJECT` berhenti di Worker.

| # | Rule | Ambang keras | Aksi |
|---|---|---|---|
| R1 | **Stop Loss wajib** | `stopLossPrice` harus ada (sudah dipaksa di Zod schema, Bagian 2.3) DAN jarak SL ke entry ≥ 0.1% (mencegah SL yang "menempel" dan langsung ke-trigger oleh noise) | `REJECT` jika hilang atau jarak <0.1% |
| R2 | **Leverage cap** | Maksimum **20x**, apapun permintaan caller (sudah di Zod `.max(20)`), dan maksimum efektif per-symbol mengikuti `MODERATE` jika >10x | `REJECT` >20x, `MODERATE` warning 10–20x |
| R3 | **Margin utilization cap** | Order tidak boleh membuat total margin terpakai (existing + order baru) melebihi **80% dari wallet balance** setelah dikurangi buffer funding fee estimasi 1 siklus (8 jam) | `REJECT` jika proyeksi >80%; `HIGH_RISK` di 60–80% |
| R4 | **Notional per-order cap** | Notional order tunggal (`quantity × price`) tidak boleh melebihi **min(25% wallet balance, hard cap USD yang dikonfigurasi via KV)** | `REJECT` jika melebihi |
| R5 | **Idempotency / duplicate-order guard** | `clientOrderId` (wajib, Bagian 2.3) di-cek terhadap KV key `trading:seen_client_order_id:{id}` (TTL 24 jam) sebelum request keluar; kalau sudah pernah dilihat, order ditolak tanpa panggil Binance sama sekali | `REJECT` — mencegah retry storm & race condition (celah #1, #6) |
| R6 | **Per-symbol cooldown** | Maksimum 1 order baru per symbol per **10 detik** (state KV `trading:last_order_ts:{symbol}`) — independen dari rate limit Binance sendiri, sebagai lapisan pertahanan kedua | `REJECT` jika dalam cooldown |
| R7 | **Global order-rate budget** | Maksimum **10 order/menit** dan **60 order/jam** total di seluruh server (counter KV, pola sama seperti `queryFrequency.ts` yang sudah ada), jauh di bawah limit resmi Binance supaya selalu ada headroom sebelum kena `418` | `REJECT` jika budget habis |
| R8 | **Precision & filter check** | Quantity/price dibulatkan & divalidasi terhadap `stepSize`, `tickSize`, `minNotional`, `minQty` dari `GET /fapi/v1/exchangeInfo` (cache via `fetchSymbolTradingRules`, pola yang sama dipakai `gridRiskEngine.ts`) SEBELUM request keluar | `REJECT` jika gagal presisi/minNotional, otomatis dibulatkan untuk kasus rounding halus |
| R9 | **Circuit breaker eksternal** | Order baru dicek terhadap `state:daily_loss_circuit` dan `state:macro_risk_circuit` (KV key yang SUDAH ADA di `riskCircuitBreaker.ts`) — kalau salah satu trip, `futures_place_order` ikut diblokir, bukan cuma cron alert | `REJECT` jika circuit aktif |
| R10 | **Explicit risk acknowledgement** | `riskAcknowledgementToken` (Zod, wajib) harus match hash deterministik dari `{symbol, quantity, leverage, stopLossPrice}` yang dihasilkan tool read-only terpisah (mis. dari `futures_get_positions` preview atau tool baru `futures_preview_order` — di luar cakupan 4 tool wajib, tapi direkomendasikan) — mencegah agent AI menembak `place_order` langsung dari halusinasi tanpa pernah "melihat" angka risikonya | `REJECT` jika token tidak cocok/kadaluarsa (TTL 60 detik) |
| R11 | **Testnet-only default** | Selama `TRADING_ENV !== "mainnet"` (Bagian 1.2), SEMUA order — apapun hasil R1–R10 — dieksekusi ke `testnet.binancefuture.com`. Tidak ada jalur kode yang membaca argumen tool untuk menentukan base URL. | Structural, bukan runtime check |

### 3.3 Kenapa `futures_cancel_order` tetap perlu guard (bukan cuma place_order)
- **R5-lite**: cancel yang menyasar `orderId` yang sudah `CANCELED`/`FILLED` harus dikembalikan sebagai response sukses idempoten (bukan error keras) — mencegah AI agent panik retry-loop saat cancel "gagal" padahal order memang sudah selesai.
- **R7** (global rate budget) tetap berlaku untuk cancel, di counter terpisah, karena cancel storm (mis. agent mencoba cancel semua open order berkali-kali) juga memakan order-rate-limit Binance yang sama.

---

## 4. Matriks Penanganan Error Binance (API Code vs MCP Response)

| Binance `code` | Arti | `errorCode` MCP | `retryable` | Perlakuan |
|---|---|---|---|---|
| `-1021` | Timestamp di luar `recvWindow` | `CLOCK_SKEW` | `true` (1x auto-retry dengan resync timestamp) | Resync server time dari `GET /fapi/v1/time`, retry sekali |
| `-1022` | Signature tidak valid | `SIGNATURE_INVALID` | `false` | Log sebagai insiden konfigurasi (secret salah/rotasi gagal), JANGAN retry — bisa jadi API secret bocor/salah |
| `-1111` | Precision salah (stepSize/tickSize) | `PRECISION_ERROR` | `false` | Harusnya sudah tertangkap R8; kalau lolos sampai sini berarti cache `exchangeInfo` stale — invalidate cache |
| `-1013` / `-4164` | Notional di bawah minimum | `MIN_NOTIONAL` | `false` | Harusnya tertangkap R8/R4; tampilkan minNotional aktual ke caller |
| `-2010` | Margin tidak cukup (`insufficient balance`) | `INSUFFICIENT_MARGIN` | `false` | Harusnya tertangkap R3; kalau lolos, kemungkinan race condition margin — trigger review R5/R6 |
| `-2011` | Order tidak ditemukan (cancel target sudah tidak ada) | `ORDER_NOT_FOUND` | `false` | Untuk `futures_cancel_order`: treat sebagai sukses idempoten jika status terakhir yang diketahui adalah `FILLED`/`CANCELED` |
| `-2019` | Margin tidak cukup untuk leverage yang diminta | `LEVERAGE_MARGIN_INSUFFICIENT` | `false` | Sama seperti `-2010` |
| `-1003` | Rate limit terlampaui (weight) | `RATE_LIMIT_WEIGHT` | `true` (backoff sesuai header `Retry-After`) | Set flag KV pause sementara di semua tool trading selama backoff |
| `-1015` | Order-rate-limit terlampaui (bukan IP weight) | `RATE_LIMIT_ORDERS` | `true` (backoff) | Konfirmasi R7 (budget internal) belum cukup ketat — turunkan ambang R7 |
| `418` (HTTP, bukan JSON `code`) | IP banned sementara karena spam rate limit | `IP_BANNED` | `false` sampai `Retry-After` lewat | **Trip circuit breaker global** (matikan semua trading tool) sampai ban selesai; alert manual wajib |
| `403` (HTTP) | WAF/region block (pola yang sudah dialami `binanceProxyClient.ts` untuk endpoint public) | `REGION_BLOCKED` | `false` | Untuk trading endpoint, JANGAN auto-failover ke proxy pihak ketiga seperti pola public data — signed request lewat proxy tidak tepercaya adalah risiko kebocoran secret. Alert manual. |
| `-2015` | API key tidak valid, IP tidak di-whitelist, atau permission kurang | `API_KEY_INVALID` | `false` | Insiden konfigurasi — alert manual, jangan retry |
| `-4046` | `No need to change margin type` | `NOOP_MARGIN_TYPE` | `false` (bukan error fatal) | Treat sebagai info, lanjutkan flow |
| Network timeout / `ECONNRESET` | — | `NETWORK_TIMEOUT` | `true` (maks 1x retry) | **Kecuali untuk `futures_place_order`** — order yang timeout TIDAK BOLEH auto-retry secara membabi buta karena status order di sisi Binance tidak diketahui (bisa jadi sudah tereksekusi). Wajib `GET /fapi/v1/order` (query by `clientOrderId`) dulu untuk konfirmasi status sebelum retry atau melapor gagal. |

**Prinsip matriks:** `retryable: true` hanya untuk kegagalan yang provably safe untuk diulang (clock skew, rate limit, network sebelum ada write). Apapun yang menyentuh state order (`place`/`cancel`) yang statusnya ambigu setelah gagal — WAJIB verifikasi via query order dulu, tidak pernah auto-retry buta.

---

## 5. Checklist Kesiapan untuk Cursor AI (Panduan Eksekusi Codebase)

Urutan implementasi wajib mengikuti nomor di bawah — jangan lompat ke `futures_place_order` sebelum item 1–5 selesai dan punya test.

1. [ ] **Secret & binding**: tambahkan `BINANCE_TESTNET_API_KEY`/`SECRET` (dan `BINANCE_MAINNET_*` terpisah untuk env production) via `wrangler secret put`, JANGAN pernah taruh di `wrangler.toml` plaintext atau `.env` yang ter-commit.
2. [ ] **`src/binanceTradingClient.ts`**: implementasi HMAC-SHA256 signing (`crypto.subtle` di Workers runtime, bukan Node `crypto`), request timestamp + `recvWindow`, base URL dipilih dari `TRADING_ENV` binding saja (lihat Aturan R11) — tulis unit test yang membuktikan base URL TIDAK BISA dipengaruhi oleh argumen tool apa pun.
3. [ ] **`src/engine/futuresOrderGuard.ts`**: implementasi R1–R11 dari Bagian 3, masing-masing rule sebagai fungsi murni yang dites terpisah (pola sama seperti `gridRiskEngine.ts` yang sudah punya `gridRiskEngine.test.ts` granular per-rule).
4. [ ] **Extend `riskCircuitBreaker.ts`**: tambah key KV baru untuk trading (`state:trading_order_rate`, `trading:seen_client_order_id:*`, `trading:last_order_ts:*`) — JANGAN modifikasi semantik `state:daily_loss_circuit`/`state:macro_risk_circuit` yang sudah dipakai cron entry-alert; hanya tambah pembacaan cross-check di R9.
5. [ ] **Zod schema**: implementasikan schema Bagian 2 persis, tambahkan test yang secara eksplisit membuktikan: (a) order tanpa `stopLossPrice` ditolak di layer Zod sebelum masuk fungsi handler, (b) `leverage: 21` ditolak, (c) `environment`/`baseUrl` BUKAN field yang diterima schema manapun.
6. [ ] **`futures_get_balance` & `futures_get_positions`**: register via `registerSafeTool`, read-only, tidak menyentuh guard di poin 3 (guard hanya untuk order mutation).
7. [ ] **`futures_place_order`**: urutan wajib di handler — (a) Zod parse → (b) `futuresOrderGuard` semua rule R1–R10 → (c) jika `REJECT`, return `isError:true` TANPA memanggil Binance sama sekali → (d) jika lolos, call `binanceTradingClient.placeOrder()` → (e) map response/error lewat matriks Bagian 4.
8. [ ] **`futures_cancel_order`**: idempotent terhadap `-2011`, tetap kena R7 (rate budget), register via `registerSafeTool`.
9. [ ] **Test wajib sebelum tool dianggap "selesai"** (ikuti pola `*.test.ts` yang sudah konsisten di seluruh `src/`):
   - [ ] Setiap rule R1–R11 punya test unit yang REJECT pada boundary tepat (mis. leverage=20 lolos, leverage=21 gagal).
   - [ ] Test bahwa duplicate `clientOrderId` dalam window 24 jam ditolak tanpa panggilan network kedua (mock `fetch` dan assert call count).
   - [ ] Test bahwa semua 4 tool, dalam mode default (tanpa `TRADING_ENV=mainnet` di-set), memanggil `testnet.binancefuture.com` — bukan hanya di happy path tapi juga di jalur error.
   - [ ] Test matriks error Bagian 4: setiap `errorCode` punya minimal 1 test yang men-simulate response Binance code aslinya via mock.
10. [ ] **Manual verification di testnet nyata** (bukan cuma mock) sebelum PR untuk 4 tool ini dianggap siap review: buka posisi kecil di testnet, cancel, cek balance/positions reflect dengan benar, cek satu skenario `REJECT` (mis. leverage berlebih) benar-benar berhenti sebelum Binance API.
11. [ ] **Sign-off manusia untuk mainnet**: tidak ada PR yang mengubah `TRADING_ENV` ke `mainnet` di config production yang boleh merge tanpa review eksplisit dari pemilik proyek (bukan auto-merge, bukan approval dari AI agent lain) — dicatat di deskripsi PR.
12. [ ] **`binance_get_tool_catalog`**: pastikan deskripsi 4 tool baru di catalog secara eksplisit menyebutkan "testnet by default" dan "stopLossPrice wajib" — supaya AI agent caller yang membaca catalog sebelum memanggil tool tidak berasumsi salah.
