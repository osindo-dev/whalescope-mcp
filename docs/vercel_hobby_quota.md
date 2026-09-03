# Vercel Hobby sebagai relay API — hitung Edge Requests dulu

🇮🇩 Bahasa Indonesia | [🇬🇧 English](vercel_hobby_quota.en.md)

Catatan untuk **project serupa** yang ingin pakai Vercel (Hobby) sebagai
proxy/relay ke API pihak ketiga (Binance, atau API lain yang memblokir
Cloudflare Workers). Bukan tutorial deploy — itu ada di
[`proxy/README.md`](../proxy/README.md). Ini pelajaran kuota yang sudah
terjadi di repo ini.

## Klaim yang salah

Hobby **bukan** “jutaan invocation, aman untuk cron personal.”

Yang dihitung ketat di dashboard Usage Overview adalah **Edge Requests**,
bukan CPU dan bukan bandwidth:

| Metrik | Yang terlihat saat kuota habis (insiden ini) |
|---|---|
| Edge Requests | **1 juta / 1 juta (100%)** — ini yang mematikan |
| Fast Data Transfer | 2,16 GB / 100 GB |
| Fast Origin Transfer | 2,1 GB / 10 GB |
| Edge Request CPU Duration | 2 menit / 1 jam |

CPU dan transfer bisa kelihatan sepi sementara **jumlah hit HTTP** sudah
mentok. Jangan pakai CPU/GB sebagai sinyal “masih aman.”

Satu Edge Request = **satu HTTP ke function Vercel**, termasuk yang gagal
(401 secret salah, 403, 429, 5xx). Failover ke proxy kedua **tidak
menghapus** hit yang gagal di primary.

Dua project Vercel di **satu tim Hobby yang sama** berbagi kolam 1 juta
itu. Deploy `PROXY_URL` dan `PROXY_URL_2` di akun yang sama **tidak**
menggandakan kuota.

## Rumus sebelum deploy

Hitung per bulan, lalu bandingkan ke **1.000.000**:

```
per_bulan ≈ call_per_tick × tick_per_hari × 30
```

Cron yang **tidak bisa di-cache** (order book, aggTrades, snapshot yang
harus beda tiap tick) adalah yang paling mahal. Contoh di repo ini
(`src/shared.ts`, `WALL_SCAN_WATCHLIST`):

```
N pair × 1 depth/menit × 1.440 menit/hari × 30 hari
15 pair → ~648.000/bulan
50 pair → ~2.160.000/bulan  (langsung tembus Hobby)
```

Tambah pipeline berat (banyak endpoint per symbol, berkali-kali per hari).
Contoh kasar entry-alert: 96 tick/hari × 40 pair × belasan call Binance
per pair → puluhan ribu sampai >1 juta/bulan **tanpa** wall-scan.

Kalau hasil rumus > ~700 ribu, Hobby akan habis sebelum akhir siklus
tagihan — atau dalam beberapa hari kalau cron padat.

## Insiden WhaleScope (2026-09-03)

- Project: `whale-binance-proxy` di tim Vercel Hobby **Jaringan Semesta Raya**.
- Proxy di-deploy ~28–29 Agu 2026.
- **3 Sep 2026** dashboard menunjukkan Edge Requests **1 juta / 1 juta**.
- Perkiraan bakar: **~180–200 ribu request/hari** → kuota sebulan habis
  dalam ~5 hari.

Driver:

1. Wall-scan 1 menit × 15 pair, depth `NO_CACHE` (by design).
2. Entry-alert 15 menit × 40 pair (klines, OI, depth, rasio, dll.).
3. Primary 401: setiap call tetap memukul Vercel dulu, baru failover.
   Request gagal tetap dihitung.

Dampak: tool MCP dan cron yang lewat proxy ini bisa gagal atau
intermiten. Worker Cloudflare **tidak** kena kuota ini; yang macet adalah
jalur ke Binance.

Reset mengikuti **siklus tagihan Vercel**, bukan tanggal 1 kalender.

## Checklist project baru

Sebelum pilih Hobby sebagai relay:

1. Tulis daftar cron: interval, jumlah symbol, call per symbol, path mana
   yang `NO_CACHE`.
2. Jalankan rumus di atas. Kalau > 1 juta, **jangan** anggap Hobby cukup.
3. Samakan secret proxy di Vercel dan di worker. 401 berulang = bakar
   kuota tanpa data.
4. Failover: taruh proxy kedua di **akun/tim Vercel lain**, atau VPS —
   bukan project kedua di tim Hobby yang sama.
5. Naik plan (Pro) **setelah** pemborosan dipotong, bukan sebagai pengganti
   hitungan. Pro menaikkan kuota; dia tidak memperbaiki 401 atau cron 1
   menit yang tidak perlu.
6. Pantau Usage Overview: Edge Requests dulu, baru Origin Transfer (10 GB
   di Hobby juga bisa jadi plafon kedua kalau response besar).

Urutan perbaikan kalau sudah mentok: perbaiki 401 → turunkan frekuensi
atau N pair untuk path `NO_CACHE` → baru Pro / VPS / akun kedua.

## Di repo ini

- Deploy proxy: [`proxy/README.md`](../proxy/README.md)
- Cut wall-scan 50→15 pair dan alasan kuota: komentar `WALL_SCAN_WATCHLIST`
  di `src/shared.ts`
- Failover 401/403/429: `src/binanceProxyClient.ts` (`FAILOVER_STATUS`)
