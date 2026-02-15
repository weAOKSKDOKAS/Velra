# Velra Terminal (RSS + Gemini, Cloud Run + GCS)

Versi ini snapshot-driven:
- **Writer** (Cloud Run Job) bikin `data.json` di **GCS**
- **Web** (Cloud Run Service) cuma baca & render

Tidak ada “generate per request”, jadi biaya Gemini tidak kebakar cuma karena user ngeklik.

## Arsitektur
Cloud Run Job (`node worker.js`)
→ fetch RSS
→ (opsional) Gemini nulis ulang (headline, 3 keypoints, story) **hanya untuk item baru**
→ merge snapshot lama (maks 24 jam)
→ tulis `data.json` ke GCS

Cloud Run Service (`node server.js`)
→ serve `index.html`
→ proxy `GET /data.json` dari GCS

## Environment Variables
**Untuk Service & Job**
- `GCS_BUCKET` = nama bucket (contoh: `velra-terminal-prod`)
- `DATA_OBJECT` = default `data.json` (opsional)

**Untuk Job (writer)**
- `GEMINI_API_KEY` = API key Gemini (kalau kosong: worker tetap jalan, tanpa rewriting LLM)
- `MODE` = `hourly` (default) | `morning` | `full`
- `LOOKBACK_HOURS` = default 1 (berapa jam item dianggap “baru” dan layak di-rewrite LLM)
- `RSS_WINDOW_HOURS` = default 24 (window discovery RSS; sengaja lebih lebar supaya first run tidak kosong)
- `TTL_HOURS` = default 24 (umur maksimum berita)
- `GEMINI_MAX_NEW` = default 14 (maks item baru per region per run yang masuk Gemini)
- `GEMINI_MODEL` = opsional (kalau kosong: coba beberapa model fallback)
- `SECTOR_MAX` = default 10 (maks item per sektor)
- `GENERAL_MAX` = default 18 (jumlah item untuk tampilan "Umum" per region)

## Deploy (Cloud Run)
### Service (web)
- Image: hasil build Dockerfile
- Command: default (jalan `server.js`)
- Env: `GCS_BUCKET`

### Job (writer)
- Image: sama
- Override command: `node worker.js`
- Env: `GCS_BUCKET`, `GEMINI_API_KEY`, dll
- Schedule: Cloud Scheduler tiap jam (minute 0)

## Kontrol biaya Gemini
1) Gemini hanya di worker, bukan di request user.
2) Worker hanya panggil Gemini untuk **item baru** (default window 1 jam).
3) `GEMINI_MAX_NEW` membatasi jumlah item per run.
4) Morning brief (LLM) maksimal **1x per hari per region** (disimpan via `mbMeta`).

Kalau biaya masih bengkak:
- turunin `GEMINI_MAX_NEW` ke 6–8
- cek Cloud Scheduler jangan retry spam
- jangan jalankan `MODE=full` kecuali bootstrap

## No dummy
- Livewire kosong = valid. UI tampil: “Tidak ada update signifikan ≤24 jam.”
- Tidak ada placeholder news.
