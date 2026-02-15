import os
import json
import time
import argparse
import sys
from datetime import datetime
import pytz
from google import genai
from google.genai import types

# Configuration
WIB = pytz.timezone('Asia/Jakarta')
DATA_FILE = "data.json"

def get_api_client():
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        print("ERROR: No API key found in environment variables (GEMINI_API_KEY).")
        return None
    return genai.Client(api_key=api_key)

def atomic_write(data):
    temp_file = DATA_FILE + ".tmp"
    with open(temp_file, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(temp_file, DATA_FILE)

def load_data():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                return json.load(f)
        except:
            return None
    return None

def generate_market_data(use_search=True):
    client = get_api_client()
    if not client: return
    
    current_data = load_data()
    now_wib = datetime.now(WIB)
    
    print(f"[{now_wib.isoformat()}] Triggering AI Generation via Gemini...")
    
    prompt = """
    ROLE: Senior Financial Analyst.
    TASK: Generate a market intelligence snapshot in JSON.
    SCHEMA:
    {
      "briefings": {
        "global": {"title": str, "bullets": [str], "what_to_watch": [str]},
        "indonesia": {"title": str, "bullets": [str], "what_to_watch": [str]},
        "usa": {"title": str, "bullets": [str], "what_to_watch": [str]},
        "sectors": {
          "GENERAL": {"title": str, "bullets": [str]},
          "TECHNOLOGY": {"title": str, "bullets": [str]},
          "FINANCE": {"title": str, "bullets": [str]},
          "MINING": {"title": str, "bullets": [str]},
          "HEALTHCARE": {"title": str, "bullets": [str]},
          "REGULATION": {"title": str, "bullets": [str]},
          "CONSUMER": {"title": str, "bullets": [str]}
        }
      },
      "indices": {
        "INDONESIA": [{"symbol": "IHSG", "name": "IDX Composite", "value": float, "change": str, "trend": "UP/DOWN/FLAT"}],
        "USA": [{"symbol": "Nasdaq", "name": "Nasdaq 100", "value": float, "change": str, "trend": "UP/DOWN/FLAT"}],
        "ASIA": [{"symbol": "Nikkei 225", "name": "Nikkei", "value": float, "change": str, "trend": "UP/DOWN/FLAT"}],
        "EUROPE": [{"symbol": "DAX", "name": "DAX", "value": float, "change": str, "trend": "UP/DOWN/FLAT"}],
        "AMERICAS": [{"symbol": "TSX", "name": "TSX", "value": float, "change": str, "trend": "UP/DOWN/FLAT"}]
      },
      "livewire": [{"headline": str, "impact": "HIGH/MEDIUM/LOW", "summary": str}]
    }
    Include REAL news from the last hour.
    """
    
    try:
        tools = [types.Tool(google_search=types.GoogleSearch())] if use_search else []
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
            config=types.GenerateContentConfig(
                tools=tools,
                response_mime_type="application/json"
            )
        )
        
        ai_data = json.loads(response.text)
        
        # Update metadata
        new_news = ai_data.get("livewire", [])
        for item in new_news:
            item["time"] = now_wib.strftime("%H:%M")
            
        old_wire = current_data.get("livewire", []) if current_data else []
        combined_wire = new_news + old_wire
        
        impact_score = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
        combined_wire.sort(key=lambda x: (impact_score.get(x.get("impact", "LOW"), 0), x.get("time", "00:00")), reverse=True)
        
        final_data = {
            "schema_version": 1,
            "generated_at_wib": now_wib.isoformat(),
            "status": {
                "ok": True,
                "last_error": None,
                "last_success_at_wib": now_wib.isoformat()
            },
            "briefings": ai_data.get("briefings", {}),
            "indices": ai_data.get("indices", {}),
            "livewire": combined_wire[:10]
        }
        
        atomic_write(final_data)
        print(f"[{datetime.now().isoformat()}] Market snapshot updated successfully.")
        
    except Exception as e:
        print(f"Generation error: {e}")
        if current_data:
            current_data["status"]["ok"] = False
            current_data["status"]["last_error"] = str(e)
            atomic_write(current_data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    # Startup logic: Generate immediately if file is missing or old (>60m)
    stale = False
    if os.path.exists(DATA_FILE):
        file_age = time.time() - os.path.getmtime(DATA_FILE)
        if file_age > 3600:
            stale = True
            print("Existing data is stale (>1hr). Triggering refresh...")
    
    if not os.path.exists(DATA_FILE) or stale:
        print("Initializing market data snapshot...")
        generate_market_data()

    if args.once:
        sys.exit(0)

    print("Velra Worker active. Monitoring clock for top-of-hour updates...")
    while True:
        now = datetime.now(WIB)
        if now.minute == 0:
            generate_market_data()
            time.sleep(61)
        time.sleep(30)
