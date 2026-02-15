# Velra Terminal v4.0 Deployment Guide

Follow these steps to run the terminal locally or on a server.

### 1. Requirements
- **Node.js 18+** (disarankan, karena writer utama ada di `worker.js`)
- **Python 3.9+** (opsional, hanya untuk server dev sederhana)

### 2. Install Dependencies
```bash
npm install
```

### 3. Set API Key
You need a Gemini API Key. Replace `YOUR_KEY` below:
- **Windows (CMD):** `set GEMINI_API_KEY=YOUR_KEY`
- **Mac/Linux:** `export GEMINI_API_KEY=YOUR_KEY`

### 4. Running the App (Local Dev)
Buka dua terminal:

**Terminal 1 (Writer / Generator):**
Ini yang membuat `data.json` (bisa tulis ke local atau GCS kalau `GCS_BUCKET` di-set).
```bash
npm run worker
```

**Terminal 2 (Web Server - local dev):**
Ini serve UI + file `data.json` lokal.
```bash
pip install flask
python server.py
```

### 5. Access
Open your browser to: **http://localhost:5000**

---
### 5. Cloud Run (ringkas)
- **Cloud Run Service (web)**: pakai Dockerfile ini (jalanin `node server.js`), set env `GCS_BUCKET`.
- **Cloud Run Job (writer)**: jalanin `node worker.js`, set env `GCS_BUCKET` + `GEMINI_API_KEY`, schedule tiap jam.