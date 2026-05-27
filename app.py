#!/usr/bin/env python3
"""
The Clarity Collective Dashboard - Live Web App
Serves a dashboard with live data from Eventbrite + Facebook Ads APIs.
Data is cached for 10 minutes. Background thread pre-builds data on startup
so the user sees a loading screen for ~30s instead of waiting 3-5 minutes.
"""
import os
import requests
import json
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_EXCEPTION
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, Response, jsonify, request

ET = ZoneInfo("America/New_York")
APP_VERSION = "v2.2-fix-date-ranges"

app = Flask(__name__)

def _eb_request(url, params, timeout=(10, 30), max_retries=5):
    """Make an Eventbrite API request with retry logic for 429 rate limits."""
    for attempt in range(max_retries):
        res = requests.get(url, params=params, timeout=timeout)
        if res.status_code == 429:
            wait = min(2 ** attempt * 2, 30)  # 2s, 4s, 8s, 16s, 30s
            print(f"[EB] 429 rate limit, waiting {wait}s (attempt {attempt+1}/{max_retries})...", flush=True)
            time.sleep(wait)
            continue
        res.raise_for_status()
        return res
    # Final attempt without catching
    res = requests.get(url, params=params, timeout=timeout)
    res.raise_for_status()
    return res

# ====== CONFIG (from environment variables) ======
EB_TOKEN = os.environ.get("EB_TOKEN", "")
EB_ORG_ID = os.environ.get("EB_ORG_ID", "")
FB_TOKEN = os.environ.get("FB_TOKEN", "")
FB_AD_ACCOUNT = os.environ.get("FB_AD_ACCOUNT", "")

# Simple cache: store generated HTML and timestamp
_cache = {"html": None, "time": 0, "building": False, "build_thread": None}
CACHE_TTL = 1800  # 30 minutes
CACHE_FILE = "/tmp/clarity_collective_dashboard_cache.html"
CACHE_TIME_FILE = "/tmp/clarity_collective_dashboard_cache_time.txt"

# Track API errors for dashboard display
_api_errors = []

# Persistent FB data cache — survives FB API timeouts
FB_DATA_CACHE_FILE = "/tmp/clarity_collective_fb_cache.json"
_fb_data_cache = {"period_results": {}, "meta_by_num": {}, "meta_by_city": {}, "time": 0}

def _save_fb_cache(period_results, meta_by_num, meta_by_city):
    """Save FB data to disk so it survives API timeouts and restarts."""
    global _fb_data_cache
    _fb_data_cache = {
        "period_results": period_results,
        "meta_by_num": meta_by_num,
        "meta_by_city": meta_by_city,
        "time": time.time()
    }
    try:
        with open(FB_DATA_CACHE_FILE, "w") as f:
            json.dump(_fb_data_cache, f)
        print(f"[FB_CACHE] Saved FB data cache ({len(period_results)} periods)", flush=True)
    except Exception as e:
        print(f"[FB_CACHE] Failed to save: {e}", flush=True)

def _load_fb_cache():
    """Load cached FB data from disk."""
    global _fb_data_cache
    try:
        with open(FB_DATA_CACHE_FILE, "r") as f:
            data = json.load(f)
        if data and data.get("period_results"):
            _fb_data_cache = data
            age = time.time() - data.get("time", 0)
            print(f"[FB_CACHE] Loaded FB data cache ({len(data['period_results'])} periods, {age:.0f}s old)", flush=True)
            return data
    except Exception:
        pass
    return None

BUILD_TIMEOUT = int(os.environ.get("BUILD_TIMEOUT", "120"))  # 2 min default (reduced from 15 min)
_build_lock = threading.Lock()  # Prevent multiple simultaneous builds

def _save_cache_to_disk(html):
    """Persist cache to disk so it survives Render restarts."""
    try:
        with open(CACHE_FILE, "w") as f:
            f.write(html)
        with open(CACHE_TIME_FILE, "w") as f:
            f.write(str(time.time()))
    except Exception:
        pass  # Best effort — disk cache is optional

def _load_cache_from_disk():
    """Load cached HTML from disk on startup (instant cold start)."""
    try:
        with open(CACHE_FILE, "r") as f:
            html = f.read()
        if not html or len(html) < 1000:
            return None, 0
        # Try to read the timestamp file; fall back to file modification time
        try:
            with open(CACHE_TIME_FILE, "r") as f:
                cache_time = float(f.read().strip())
        except Exception:
            cache_time = os.path.getmtime(CACHE_FILE)
        return html, cache_time
    except Exception:
        pass
    return None, 0

def _build_cache_background():
    """Build the dashboard HTML in a background thread."""
    if not _build_lock.acquire(blocking=False):
        return  # Another build is already running
    _cache["building"] = True
    _cache["build_start"] = time.time()
    _cache["build_thread"] = threading.current_thread()
    start = time.time()
    try:
        html = build_dashboard_html()
        # SAFETY: Only replace cache if new HTML is valid (>5KB = real dashboard)
        if html and len(html) > 5000:
            _cache["html"] = html
            _cache["time"] = time.time()
            _save_cache_to_disk(html)
            print(f"[CACHE] Build succeeded in {time.time()-start:.1f}s ({len(html)} bytes)", flush=True)
        else:
            # Build returned short/empty HTML — keep existing cache
            print(f"[CACHE] Build returned short HTML ({len(html) if html else 0} bytes) in {time.time()-start:.1f}s — keeping existing cache", flush=True)
            # Still mark cache as recently checked to avoid immediate retry
            if _cache["html"] and len(_cache["html"]) > 5000:
                _cache["time"] = time.time()
    except Exception as e:
        import traceback
        print(f"[CACHE] Background build failed after {time.time()-start:.1f}s: {e}", flush=True)
        traceback.print_exc()
        # Try loading disk cache as fallback — never replace good HTML with error HTML
        disk_html, disk_time = _load_cache_from_disk()
        if disk_html and len(disk_html) > 5000 and (not _cache["html"] or len(_cache["html"]) < 5000):
            _cache["html"] = disk_html
            _cache["time"] = disk_time
            print(f"[CACHE] Loaded disk cache as fallback ({len(disk_html)} bytes)", flush=True)
        elif not _cache["html"] or len(_cache["html"]) < 5000:
            _cache["html"] = _build_error_html(str(e))
            _cache["time"] = time.time()
        else:
            # We have good HTML, just mark it as recently checked
            _cache["time"] = time.time()
            print(f"[CACHE] Build failed but keeping existing good cache ({len(_cache['html'])} bytes)", flush=True)
    finally:
        _cache["building"] = False
        _cache["build_thread"] = None
        _build_lock.release()

def _ensure_cache():
    """Trigger a background rebuild if cache is stale. Non-blocking.
    IMPORTANT: Always serves existing stale cache — never blocks the user.
    NOTE: Called on every request — must be fast and avoid print() to prevent
    reentrant stdout errors with gunicorn threads."""
    if _cache["html"] and (time.time() - _cache["time"]) < CACHE_TTL:
        return  # Cache is fresh
    if _cache["building"]:
        # Check if the build thread is still alive
        build_thread = _cache.get("build_thread")
        thread_dead = build_thread is not None and not build_thread.is_alive()
        build_start = _cache.get("build_start", 0)
        elapsed = time.time() - build_start if build_start else 0
        timed_out = build_start and elapsed > BUILD_TIMEOUT
        if thread_dead or timed_out:
            reason = "thread died" if thread_dead else f"timed out after {elapsed:.0f}s"
            print(f"[CACHE] Build stuck ({reason}) — resetting", flush=True)
            _cache["building"] = False
            _cache["build_thread"] = None
            # Force-release the lock if it's held by a dead thread
            try:
                _build_lock.release()
            except RuntimeError:
                pass  # Lock wasn't held
            # SAFETY: If we already have good HTML (>5KB), keep it and just mark as fresh
            if _cache["html"] and len(_cache["html"]) > 5000:
                _cache["time"] = time.time()  # Prevent immediate retry
                print(f"[CACHE] Build timed out but keeping existing good cache ({len(_cache['html'])} bytes)", flush=True)
            else:
                # No good HTML — try disk cache as fallback
                disk_html, disk_time = _load_cache_from_disk()
                if disk_html and len(disk_html) > 5000:
                    _cache["html"] = disk_html
                    _cache["time"] = time.time()
                    print(f"[CACHE] Loaded disk cache as fallback ({len(disk_html)} bytes)", flush=True)
                elif not _cache["html"]:
                    _cache["html"] = _build_error_html(
                        f"Dashboard build {reason}. The APIs may be slow. Try /refresh in a minute.")
                    _cache["time"] = time.time()
            # Don't return — fall through to start a new build
        else:
            return  # Still building, wait
    # Start a background build — existing stale cache continues to be served
    t = threading.Thread(target=_build_cache_background, daemon=True, name="cache-builder")
    t.start()

def _proactive_refresh_loop():
    """Proactively rebuild cache before it expires, so users never wait."""
    refresh_interval = CACHE_TTL - 300  # Rebuild 5 min before TTL expires
    if refresh_interval < 300:
        refresh_interval = 300  # Minimum 5 min between rebuilds
    print(f"[PROACTIVE] Refresh loop started (interval={refresh_interval}s)", flush=True)
    while True:
        time.sleep(refresh_interval)
        try:
            age = time.time() - _cache.get("time", 0)
            if not _cache["building"] and age > (CACHE_TTL * 0.5):
                print(f"[PROACTIVE] Cache is {age:.0f}s old, triggering rebuild...", flush=True)
                _build_cache_background()
            else:
                print(f"[PROACTIVE] Skip: building={_cache['building']}, age={age:.0f}s", flush=True)
        except Exception as e:
            print(f"[PROACTIVE] Error in refresh loop (continuing): {e}", flush=True)
            time.sleep(60)  # Wait a bit after errors

def _build_error_html(error_msg):
    """Return a simple error page that auto-retries."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>The Clarity Collective - Event Dashboard — Error</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 20px; }}
h1 {{ font-size: 28px; font-weight: 700; margin-bottom: 8px; }}
h1 span {{ color: #e91e8c; }}
.error {{ background: #1e293b; border: 1px solid #ef4444; border-radius: 8px; padding: 20px; max-width: 600px; margin: 20px 0; }}
.error p {{ color: #fca5a5; font-size: 14px; margin-bottom: 12px; }}
.btn {{ display: inline-block; background: #e91e8c; color: #ffffff; padding: 10px 24px; border-radius: 6px; text-decoration: none; font-weight: 600; margin-top: 12px; }}
.btn:hover {{ background: #c4167a; }}
.auto {{ color: #64748b; font-size: 13px; margin-top: 16px; }}
</style></head>
<body>
<h1><span>The Clarity Collective</span> Event Dashboard</h1>
<div class="error">
<p><strong>Build Error:</strong> {error_msg}</p>
<a href="/refresh" class="btn">Try Again</a>
</div>
<p class="auto">Auto-retrying in 30 seconds...</p>
<script>setTimeout(function(){{ window.location.href = '/refresh'; }}, 30000);</script>
</body></html>"""

# ====== FETCH EVENTBRITE DATA ======
def fetch_eb_events():
    all_events = []

    print("[EB] Fetching live events...", flush=True)
    # Fetch LIVE events
    url = f"https://www.eventbriteapi.com/v3/organizations/{EB_ORG_ID}/events/"
    params = {"status": "live", "expand": "venue,ticket_classes", "token": EB_TOKEN}
    res = _eb_request(url, params)
    live_events = res.json().get("events", [])
    for e in live_events:
        e["_eb_status"] = "live"
    all_events.extend(live_events)

    print(f"[EB] Got {len(live_events)} live events", flush=True)
    # Fetch ENDED events (recent first, capped at 100)
    url = f"https://www.eventbriteapi.com/v3/organizations/{EB_ORG_ID}/events/"
    params = {
        "status": "ended",
        "expand": "venue,ticket_classes",
        "order_by": "start_desc",
        "token": EB_TOKEN,
        "page": 1
    }
    page_count = 0
    while True:
        params["page"] = page_count + 1
        time.sleep(0.2)  # Brief pace for EB API (retry logic handles 429s)
        res = _eb_request(url, params)
        data = res.json()
        ended_events = data.get("events", [])
        if not ended_events:
            break
        for e in ended_events:
            e["_eb_status"] = "ended"
        all_events.extend(ended_events)
        page_count += 1
        has_more = data.get("pagination", {}).get("has_more_items", False)
        if not has_more or len(all_events) >= 60 or page_count >= 2:
            break

    # Fetch COMPLETED events (recent first, capped)
    time.sleep(0.3)  # Pace EB API calls
    url = f"https://www.eventbriteapi.com/v3/organizations/{EB_ORG_ID}/events/"
    params = {
        "status": "completed",
        "expand": "venue,ticket_classes",
        "order_by": "start_desc",
        "token": EB_TOKEN,
        "page": 1
    }
    page_count = 0
    while True:
        params["page"] = page_count + 1
        time.sleep(0.2)  # Brief pace for EB API (retry logic handles 429s)
        res = _eb_request(url, params)
        data = res.json()
        completed_events = data.get("events", [])
        if not completed_events:
            break
        for e in completed_events:
            e["_eb_status"] = "completed"
        all_events.extend(completed_events)
        page_count += 1
        has_more = data.get("pagination", {}).get("has_more_items", False)
        if not has_more or len(all_events) >= 60 or page_count >= 2:
            break

    print(f"[EB] Total events: {len(all_events)}", flush=True)
    return all_events

def fetch_eb_orders(event_id, since=None):
    all_orders = []
    page = 1
    while True:
        url = f"https://www.eventbriteapi.com/v3/events/{event_id}/orders/"
        params = {"token": EB_TOKEN, "page": page}
        if since:
            params["changed_since"] = since
        res = _eb_request(url, params)
        data = res.json()
        orders = [o for o in data.get("orders", []) if o.get("status") in ("placed", "completed")]
        all_orders.extend(orders)
        if not data.get("pagination", {}).get("has_more_items"):
            break
        page += 1
        if page > 30:
            break
    return all_orders

def fetch_eb_attendees(event_id):
    all_attendees = []
    page = 1
    while True:
        url = f"https://www.eventbriteapi.com/v3/events/{event_id}/attendees/"
        params = {"token": EB_TOKEN, "page": page, "status": "attending"}
        res = _eb_request(url, params)
        data = res.json()
        all_attendees.extend(data.get("attendees", []))
        if not data.get("pagination", {}).get("has_more_items"):
            break
        page += 1
        if page > 30:
            break
    return all_attendees

# ====== FETCH FACEBOOK DATA ======
def fetch_fb_insights(since_date, until_date):
    global _api_errors
    print(f"[FB] Fetching insights {since_date} to {until_date}...", flush=True)
    all_results = []
    url = f"https://graph.facebook.com/v21.0/act_{FB_AD_ACCOUNT}/insights"
    params = {
        "fields": "campaign_name,campaign_id,spend,impressions,reach,actions",
        "level": "campaign",
        "filtering": json.dumps([{"field": "campaign.name", "operator": "CONTAIN", "value": "Clarity Collective"}]),
        "time_range": json.dumps({"since": since_date, "until": until_date}),
        "limit": 200,
        "access_token": FB_TOKEN
    }
    res = requests.get(url, params=params, timeout=(10, 30))
    if res.status_code != 200:
        try:
            err_data = res.json()
            err_msg = err_data.get("error", {}).get("message", f"HTTP {res.status_code}")
            err_code = err_data.get("error", {}).get("code", "")
            err_sub = err_data.get("error", {}).get("error_subcode", "")
        except Exception:
            err_msg = f"HTTP {res.status_code}"
            err_code = ""
            err_sub = ""
        error_str = f"FB Insights API error: {err_msg}"
        if err_code == 190:
            error_str = "Facebook access token has EXPIRED. Please generate a new token in Meta Business Settings and update it in Render environment variables."
        if error_str not in _api_errors:
            _api_errors.append(error_str)
        return []
    data = res.json()
    all_results.extend(data.get("data", []))
    paging = data.get("paging", {})
    while paging.get("next"):
        res = requests.get(paging["next"], timeout=(10, 30))
        if res.status_code != 200:
            break
        data = res.json()
        all_results.extend(data.get("data", []))
        paging = data.get("paging", {})
    print(f"[FB] Got {len(all_results)} Clarity Collective campaigns for {since_date}-{until_date}", flush=True)
    return all_results

# ====== HELPERS ======
def extract_city(event):
    """Extract city from EB event. Tries venue address first, then event name."""
    # Try venue address first (most reliable)
    venue = event.get("venue", {})
    if venue and isinstance(venue, dict):
        addr = venue.get("address", {})
        if addr:
            city = addr.get("city", "")
            if city:
                return city

    # Fallback: search event name for known cities
    name = event.get("name", {}).get("text", "") if isinstance(event, dict) else str(event)
    _known_cities = ["Woodstock", "Atlanta", "Nashville", "Charlotte", "Dallas",
                     "Houston", "Austin", "Denver", "Phoenix", "Chicago", "Miami",
                     "Tampa", "Orlando", "Boston", "New York", "Los Angeles",
                     "San Francisco", "San Diego", "Seattle", "Portland"]
    for c in _known_cities:
        if c.lower() in name.lower():
            return c

    # Last resort: return a cleaned version of the event name
    name_text = name if isinstance(name, str) else str(name)
    # Try to get just the first part before any colon
    if ":" in name_text:
        name_text = name_text.split(":")[0].strip()
    return name_text[:30] if len(name_text) > 30 else name_text

def extract_event_num_from_fb(campaign_name):
    m = re.search(r"Event\s+(\d+)", campaign_name, re.I)
    return int(m.group(1)) if m else None

def extract_city_from_fb(campaign_name):
    """Extract city from FB campaign name.

    Handles patterns like:
    - "JSC - Event 1 The Clarity Collective 2026 JUNE 26-27..."
    - "Clarity Collective Event Woodstock GA 2026 Jun"
    """
    cities = ["Woodstock", "Atlanta", "Nashville", "Charlotte", "Dallas",
              "Houston", "Austin", "Denver", "Phoenix", "Chicago", "Miami",
              "Tampa", "Orlando", "Boston", "New York", "Los Angeles",
              "San Francisco", "San Diego", "Seattle", "Portland",
              "Colorado Springs", "Oklahoma City", "Salt Lake City", "Salt Lake",
              "San Antonio", "Indianapolis", "Las Vegas",
              "West Palm", "Scottsdale", "Charleston",
              "Carlsbad", "Bozeman", "Frisco",
              "Naples", "Boise", "NYC",
              "OKC", "D.C", "ATL", "DC", "LA", "Vancouver", "Alpharetta",
              "Milwaukee", "Sarasota"]

    # Normalize map for city name variants
    city_aliases = {"Salt Lake": "Salt Lake City", "OKC": "Oklahoma City",
                    "D.C": "DC", "LA": "Los Angeles", "ATL": "Atlanta"}

    # Search longest names first to avoid substring matches (e.g. "Austin" in "San Antonio")
    for city in cities:
        if city.lower() in campaign_name.lower():
            return city_aliases.get(city, city)
    return None

def extract_year_month_from_fb(campaign_name):
    """Extract approximate date from FB campaign name for matching to EB events.
    Returns (year, month) tuple or None."""
    months = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,"july":7,"june":6}
    m = re.search(r"(\d{4})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUNE|JUL|JULY|AUG|SEP|OCT|NOV|DEC)", campaign_name, re.I)
    if m:
        return (int(m.group(1)), months.get(m.group(2).lower(), 1))
    return None

def fetch_fb_event_meta():
    """Returns (meta_by_num, meta_by_city).
    meta_by_num: {event_num: {city, type, fb_status, year_month}} — keyed by event number
    meta_by_city: {city: {fb_status}} — fallback for campaigns without event numbers
    """
    global _api_errors
    url = f"https://graph.facebook.com/v21.0/act_{FB_AD_ACCOUNT}/campaigns"
    params = {"fields": "name,status", "limit": 100, "access_token": FB_TOKEN}

    meta_by_num = {}  # event_num -> {city, type, fb_status, year_month}
    meta_by_city = {}  # city -> {fb_status} for numberless campaigns
    cities = ["Woodstock", "Atlanta", "Nashville", "Charlotte", "Dallas",
              "Houston", "Austin", "Denver", "Phoenix", "Chicago", "Miami",
              "Tampa", "Orlando", "Boston", "New York", "Los Angeles",
              "San Francisco", "San Diego", "Seattle", "Portland",
              "Colorado Springs", "Oklahoma City", "Salt Lake City", "Salt Lake",
              "San Antonio", "Indianapolis", "Las Vegas",
              "West Palm", "Scottsdale", "Charleston",
              "Carlsbad", "Bozeman", "Frisco",
              "Naples", "Boise", "NYC",
              "OKC", "D.C", "ATL", "DC", "LA", "Vancouver", "Alpharetta",
              "Milwaukee", "Sarasota"]
    city_aliases = {"Salt Lake": "Salt Lake City", "OKC": "Oklahoma City",
                    "D.C": "DC", "LA": "Los Angeles", "ATL": "Atlanta"}

    page_count = 0
    print(f"[FB META] Fetching campaign metadata...", flush=True)
    while True:
        res = requests.get(url, params=params, timeout=(5, 15))
        if res.status_code != 200:
            try:
                err_data = res.json()
                err_msg = err_data.get("error", {}).get("message", f"HTTP {res.status_code}")
                err_code = err_data.get("error", {}).get("code", "")
            except Exception:
                err_msg = f"HTTP {res.status_code}"
                err_code = ""
            error_str = f"FB Campaigns API error: {err_msg}"
            if err_code == 190:
                error_str = "Facebook access token has EXPIRED. Please generate a new token in Meta Business Settings and update it in Render environment variables."
            if error_str not in _api_errors:
                _api_errors.append(error_str)
            break

        data = res.json()
        for c in data.get("data", []):
            name = c.get("name", "")
            if "clarity collective" not in name.lower():
                continue
            status = c.get("status", "")
            year_month = extract_year_month_from_fb(name)

            # Extract event number (works for both old and new formats)
            m_num = re.search(r"Event\s+(\d+)", name, re.I)
            ev_num = int(m_num.group(1)) if m_num else None

            # Extract type from bracket notation or from name
            m_bracket = re.search(r"\[([^\]]+)\]", name)
            if m_bracket:
                typ_clean = "2-Day" if "2-Day" in m_bracket.group(1) else "1-Day"
            else:
                typ_clean = "2-Day" if "2-day" in name.lower() or "2 day" in name.lower() else "1-Day"

            # Extract city
            city = None
            for ct in cities:
                if ct.lower() in name.lower():
                    city = city_aliases.get(ct, ct)
                    break

            if ev_num:
                # Store by event number — ACTIVE takes priority for same event_num
                # City may be None if not in campaign name (known_events fills it in later)
                entry = {"city": city, "type": typ_clean, "fb_status": status, "year_month": year_month}
                if ev_num not in meta_by_num or status == "ACTIVE" or meta_by_num[ev_num].get("fb_status") != "ACTIVE":
                    meta_by_num[ev_num] = entry
                    print(f"[FB META] Stored event {ev_num}: city={city}, status={status}, ym={year_month}", flush=True)
            elif city:
                # No event number — store by city+year_month as fallback
                # Include year_month so we can match to the correct event
                entry = {"fb_status": status, "year_month": year_month}
                city_key = f"{city}:{year_month[0]}-{year_month[1]:02d}" if year_month else city
                if city_key not in meta_by_city or status == "ACTIVE" or meta_by_city[city_key].get("fb_status") != "ACTIVE":
                    meta_by_city[city_key] = entry

        # Check for more pages
        paging = data.get("paging", {})
        if paging.get("next"):
            params["after"] = paging.get("cursors", {}).get("after", "")
            page_count += 1
            print(f"[FB META] Page {page_count} done, {len(meta_by_num)} events found so far...", flush=True)
            if page_count >= 5:  # Safety limit — 500 campaigns is plenty
                print(f"[FB META] Reached page limit, stopping.", flush=True)
                break
        else:
            break

    print(f"[FB META] Done — {len(meta_by_num)} events by number, {len(meta_by_city)} by city ({page_count+1} pages)", flush=True)
    return meta_by_num, meta_by_city

def build_dashboard_html():
    """Build the full dashboard HTML with live data."""
    global _api_errors
    _api_errors = []  # Reset errors for this build
    print("[BUILD] Starting dashboard build...", flush=True)
    t0 = time.time()

    # Use Eastern Time to match Paul's Facebook ad account timezone (America/New_York)
    now = datetime.now(ET)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    from datetime import timezone as _tz
    today_start_utc = today_start.astimezone(_tz.utc)
    periods = {
        "today": today_start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "yesterday": (today_start_utc - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last2": (today_start_utc - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last7": (today_start_utc - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last30": (today_start_utc - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "all": "2020-01-01T00:00:00Z"
    }

    today_str = today_start.strftime("%Y-%m-%d")
    fb_date_ranges = {
        "today": (today_str, today_str),
        "yesterday": ((today_start - timedelta(days=1)).strftime("%Y-%m-%d"), (today_start - timedelta(days=1)).strftime("%Y-%m-%d")),
        "last2": ((today_start - timedelta(days=1)).strftime("%Y-%m-%d"), today_str),
        "last7": ((today_start - timedelta(days=6)).strftime("%Y-%m-%d"), today_str),
        "last30": ((today_start - timedelta(days=29)).strftime("%Y-%m-%d"), today_str),
        "all": ("2025-05-01", today_str)
    }

    # ===== PHASE 1: Fire ALL independent API calls in parallel =====
    meta_result = [None, None]
    eb_events_result = [[]]
    fb_period_results = {}

    def _fetch_meta():
        try:
            meta_result[0], meta_result[1] = fetch_fb_event_meta()
        except Exception as e:
            print(f"[BUILD] FB meta fetch FAILED: {e}", flush=True)
            meta_result[0], meta_result[1] = {}, {}

    def _fetch_eb():
        try:
            eb_events_result[0] = fetch_eb_events()
        except Exception as e:
            print(f"[BUILD] EB events fetch FAILED: {e}", flush=True)
            eb_events_result[0] = []

    def _fetch_fb_period(pname, since, until):
        try:
            campaigns = fetch_fb_insights(since, until)
            fb_period_results[pname] = campaigns
        except Exception as e:
            print(f"[BUILD] FB insights {pname} FAILED: {e}", flush=True)
            fb_period_results[pname] = []

    # --- Phase 1A: All FB calls in parallel (FB API is fast, high rate limits) ---
    print(f"[BUILD] Phase 1A: FB calls in parallel...", flush=True)
    _p1_pool = ThreadPoolExecutor(max_workers=8)
    try:
        _p1_futures = [_p1_pool.submit(_fetch_meta)]
        for pname, (since, until) in fb_date_ranges.items():
            _p1_futures.append(_p1_pool.submit(_fetch_fb_period, pname, since, until))
        # Wait up to 45s for all FB calls — 7 parallel tasks need sufficient time
        done, not_done = wait(_p1_futures, timeout=45)
        for f in done:
            try:
                f.result()
            except Exception as e:
                print(f"[BUILD] FB task failed: {e}", flush=True)
        if not_done:
            print(f"[BUILD] WARNING: {len(not_done)} FB tasks timed out after 45s, will use cached data for missing periods", flush=True)
            for f in not_done:
                f.cancel()
    finally:
        _p1_pool.shutdown(wait=False)

    # Merge fresh FB data with cached data — per-period, not all-or-nothing
    cached = _fb_data_cache if _fb_data_cache.get("period_results") else _load_fb_cache()
    cached_periods = cached.get("period_results", {}) if cached else {}
    fb_api_worked = len(fb_period_results) > 0 and any(len(v) > 0 for v in fb_period_results.values())
    fb_meta_worked = meta_result[0] is not None and len(meta_result[0]) > 0

    # Fill in any missing periods from cache
    if cached_periods:
        for pname in fb_date_ranges:
            if pname not in fb_period_results or len(fb_period_results.get(pname, [])) == 0:
                if pname in cached_periods and len(cached_periods[pname]) > 0:
                    fb_period_results[pname] = cached_periods[pname]
                    print(f"[BUILD] Using cached FB data for period '{pname}'", flush=True)

    fresh_count = sum(1 for p in fb_period_results.values() if len(p) > 0)
    print(f"[BUILD] Phase 1A done in {time.time()-t0:.1f}s — {len(fb_period_results)} periods ({fresh_count} with data)", flush=True)

    if fb_api_worked or fb_meta_worked:
        # Save merged data to persistent cache
        _save_fb_cache(
            fb_period_results,
            meta_result[0] if fb_meta_worked else _fb_data_cache.get("meta_by_num", {}),
            meta_result[1] if fb_meta_worked else _fb_data_cache.get("meta_by_city", {})
        )
    else:
        # FB API completely failed — try to use cached FB data
        cached = _fb_data_cache if _fb_data_cache.get("period_results") else _load_fb_cache()
        if cached and cached.get("period_results"):
            fb_period_results = cached["period_results"]
            meta_result[0] = cached.get("meta_by_num", {})
            meta_result[1] = cached.get("meta_by_city", {})
            age = time.time() - cached.get("time", 0)
            print(f"[BUILD] Phase 1A done in {time.time()-t0:.1f}s — FB API timed out, using cached data ({len(fb_period_results)} periods, {age:.0f}s old)", flush=True)
        else:
            print(f"[BUILD] Phase 1A done in {time.time()-t0:.1f}s — FB API timed out, no cached data available", flush=True)

    # --- Phase 1B: EB events sequential (EB has strict rate limits) ---
    print(f"[BUILD] Phase 1B: EB events (sequential)...", flush=True)
    _fetch_eb()
    print(f"[BUILD] Phase 1B done in {time.time()-t0:.1f}s", flush=True)

    meta_by_num = meta_result[0] if meta_result[0] is not None else {}
    meta_by_city = meta_result[1] if meta_result[1] is not None else {}
    events = eb_events_result[0] if eb_events_result[0] else []
    print(f"[BUILD] Phase 1 (parallel) done in {time.time()-t0:.1f}s — {len(events)} EB events, {len(fb_period_results)} FB periods, {len(meta_by_num)} meta_by_num, {len(meta_by_city)} meta_by_city", flush=True)

    # Fallback mapping if FB meta API fails to return data
    known_events = {
        1: {"city": "Woodstock", "type": "2-Day", "fb_status": "ACTIVE", "year_month": (2026, 6)},
    }
    for num, info in known_events.items():
        if num not in meta_by_num:
            meta_by_num[num] = info
        else:
            # Merge missing fields from known_events into API-sourced data
            # (e.g., city often missing from campaign names)
            for field in ("city", "type", "year_month"):
                if not meta_by_num[num].get(field) and info.get(field):
                    meta_by_num[num][field] = info[field]
                    print(f"[BUILD] Merged known_events[{num}].{field}={info[field]} into meta_by_num", flush=True)

    # ===== Classify events into active vs past =====
    active_event_ids = []  # (index, eid, city) for events needing attendee/order data
    all_event_data = []
    all_tickets_flat = []

    print(f"[BUILD] meta_by_num before classification: {json.dumps({str(k): v for k, v in meta_by_num.items()})}", flush=True)
    print(f"[BUILD] Starting event classification loop for {len(events)} events...", flush=True)
    for ev_i, event in enumerate(events):
        eid = event["id"]
        name = event["name"]["text"]
        city = extract_city(event)
        print(f"[BUILD] Classifying event {ev_i+1}/{len(events)}: {name[:60]} (city={city}, id={eid})", flush=True)
        capacity = event.get("capacity", 0)
        start_date = event["start"]["local"]
        end_date = event["end"]["local"]
        # Calculate event duration in days (0 = same day = 1-day event, 1+ = multi-day)
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)
        duration_days = (end_dt.date() - start_dt.date()).days + 1  # 1 = one-day, 2 = two-day, etc.
        total_sold = sum(tc.get("quantity_sold", 0) for tc in event.get("ticket_classes", []))
        eb_status = event.get("_eb_status", "live")

        eb_year = int(start_date[:4])
        eb_month = int(start_date[5:7])
        best_num = 0
        best_type = ""
        best_fb_status = "UNKNOWN"
        best_score = 999

        for num, info in meta_by_num.items():
            # City match: required if both have city info, skip if either is missing
            info_city = info.get("city", "")
            if info_city and city and info_city.lower() != city.lower():
                continue
            ym = info.get("year_month")
            if ym:
                dist = abs((eb_year * 12 + eb_month) - (ym[0] * 12 + ym[1]))
            else:
                dist = 50
            # Don't match if the FB campaign date is more than 2 months away from the
            # Eventbrite event — prevents old campaigns (e.g. "Event 30 Las Vegas" from
            # March) from bleeding into a new Eventbrite event for the same city in August.
            if dist > 2:
                continue
            # Don't match a non-ACTIVE FB campaign whose date is BEFORE the EB event.
            # This prevents a completed campaign (e.g. Event 41 Toronto May) from bleeding
            # its spend into a new EB event for the same city in the following month (June).
            if ym and info.get("fb_status") != "ACTIVE":
                fb_months = ym[0] * 12 + ym[1]
                eb_months = eb_year * 12 + eb_month
                if fb_months < eb_months:
                    continue
            if dist < best_score or (dist == best_score and info.get("fb_status") == "ACTIVE"):
                best_score = dist
                best_num = num
                best_type = info.get("type", "")
                best_fb_status = info.get("fb_status", "UNKNOWN")

        print(f"[BUILD]   Classification result for {city}: best_num={best_num}, best_score={best_score}, best_fb_status={best_fb_status}", flush=True)
        if best_num == 0:
            # Try date-specific city key first, then adjacent months
            city_key_exact = f"{city}:{eb_year}-{eb_month:02d}"
            prev_m = eb_month - 1 if eb_month > 1 else 12
            prev_y = eb_year if eb_month > 1 else eb_year - 1
            next_m = eb_month + 1 if eb_month < 12 else 1
            next_y = eb_year if eb_month < 12 else eb_year + 1
            city_key_prev = f"{city}:{prev_y}-{prev_m:02d}"
            city_key_next = f"{city}:{next_y}-{next_m:02d}"
            for ck in [city_key_exact, city_key_prev, city_key_next]:
                if ck in meta_by_city:
                    best_fb_status = meta_by_city[ck].get("fb_status", "UNKNOWN")
                    break

        event_num = best_num
        fb_status = best_fb_status
        # Use EB duration for the display label (FB labels are unreliable for older events)
        duration_label = f"{duration_days}-Day" if duration_days <= 2 else f"{duration_days}-Day"
        display_city = f"{event_num} {city} ({duration_label})" if event_num else city

        event_start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        is_future = event_start > datetime.now(event_start.tzinfo)
        # Classification: active = future event with actual activity
        # Future events with 0 tickets AND no active FB campaign are inactive/placeholder
        has_activity = total_sold > 0 or fb_status == "ACTIVE"
        is_past = not is_future or (is_future and not has_activity)

        idx = len(all_event_data)
        all_event_data.append({
            "city": city, "display_city": display_city, "event_num": event_num,
            "event_id": eid, "name": name, "start_date": start_date,
            "capacity": capacity, "total_sold": total_sold,
            "fill_pct": round(total_sold / capacity * 100) if capacity > 0 else 0,
            "tickets": [], "orders": [],
            "eb_status": eb_status, "fb_status": fb_status, "is_past": is_past,
            "duration_days": duration_days
        })
        if not is_past:
            active_event_ids.append((idx, eid, city))
        print(f"[BUILD]   -> classified: is_past={is_past}, fb_status={fb_status}, total_sold={total_sold}", flush=True)

    print(f"[BUILD] Classification done in {time.time()-t0:.1f}s — {len(all_event_data)} total, {len(active_event_ids)} active", flush=True)

    # ===== PHASE 2: Fetch attendees/orders for active events in parallel =====
    def _fetch_event_detail(idx, eid, city, since_30):
        print(f"[BUILD]   Fetching attendees for {city} (eid={eid})...", flush=True)
        t_att = time.time()
        try:
            attendees = fetch_eb_attendees(eid)
        except Exception as e:
            print(f"[BUILD] EB attendees failed for {city}: {e}", flush=True)
            attendees = []
        print(f"[BUILD]   Got {len(attendees)} attendees for {city} in {time.time()-t_att:.1f}s", flush=True)
        print(f"[BUILD]   Fetching orders for {city}...", flush=True)
        t_ord = time.time()
        try:
            orders = fetch_eb_orders(eid, since=since_30)
        except Exception as e:
            print(f"[BUILD] EB orders failed for {city}: {e}", flush=True)
            orders = []
        print(f"[BUILD]   Got {len(orders)} orders for {city} in {time.time()-t_ord:.1f}s", flush=True)
        return (idx, city, attendees, orders)

    if active_event_ids:
        print(f"[BUILD] Phase 2: fetching attendees/orders for {len(active_event_ids)} active events (sequential, EB rate limit)...", flush=True)
        for p2_i, (evt_idx, eid, city) in enumerate(active_event_ids):
            print(f"[BUILD] Phase 2 event {p2_i+1}/{len(active_event_ids)}: {city} (eid={eid})", flush=True)
            try:
                evt_idx, city, attendees, orders = _fetch_event_detail(evt_idx, eid, city, periods["last30"])
            except Exception as e:
                print(f"[BUILD] Phase 2 failed for {city}: {e}", flush=True)
                continue
            ticket_list = []
            order_list = []
            for a in attendees:
                entry = {
                    "created": a["created"],
                    "name": a.get("profile", {}).get("name", "Unknown"),
                    "order_id": a.get("order_id", ""),
                    "ticket_type": a.get("ticket_class_name", ""),
                    "city": city
                }
                ticket_list.append(entry)
                all_tickets_flat.append(entry)
            for o in orders:
                cost = o.get("costs", {}).get("gross", {}).get("value", 0) / 100
                order_list.append({"created": o["created"], "name": o.get("name", "Unknown"), "amount": cost, "city": city})
            all_event_data[evt_idx]["tickets"] = ticket_list
            all_event_data[evt_idx]["orders"] = order_list
            time.sleep(0.15)  # Brief pace between events

    all_tickets_flat.sort(key=lambda x: x["created"], reverse=True)
    print(f"[BUILD] Phase 2 done in {time.time()-t0:.1f}s ({len(active_event_ids)} active, {len(all_event_data)-len(active_event_ids)} past, {len(all_tickets_flat)} total tickets)", flush=True)

    # ===== Process FB period results =====
    print(f"[BUILD] Processing FB period results ({len(fb_period_results)} periods)...", flush=True)
    def _aggregate_fb(campaigns):
        fb_by_event = {}
        for c in campaigns:
            ev_num = extract_event_num_from_fb(c.get("campaign_name", ""))
            campaign_name = c.get("campaign_name", "")
            if ev_num is not None:
                key = str(ev_num)
            else:
                city = extract_city_from_fb(campaign_name)
                if city:
                    # For city-based fallback, include year_month so we can match
                    # to the correct event and avoid old campaign data bleeding in
                    ym = extract_year_month_from_fb(campaign_name)
                    if ym:
                        key = f"city:{city}:{ym[0]}-{ym[1]:02d}"
                    else:
                        key = f"city:{city}"
                else:
                    key = None
            if key is None:
                continue
            spend = float(c.get("spend", 0))
            impressions = int(c.get("impressions", 0))
            reach = int(c.get("reach", 0))
            purchases = 0
            link_clicks = 0
            for a in c.get("actions", []):
                if a.get("action_type") == "omni_purchase":
                    purchases = int(a.get("value", 0))
                if a.get("action_type") == "link_click":
                    link_clicks = int(a.get("value", 0))
            if key in fb_by_event:
                fb_by_event[key]["spend"] += spend
                fb_by_event[key]["impressions"] += impressions
                fb_by_event[key]["reach"] += reach
                fb_by_event[key]["purchases"] += purchases
                fb_by_event[key]["link_clicks"] += link_clicks
            else:
                fb_by_event[key] = {"spend": spend, "impressions": impressions, "reach": reach, "purchases": purchases, "link_clicks": link_clicks}
        return fb_by_event

    fb_periods = {}
    for pname, campaigns in fb_period_results.items():
        fb_periods[pname] = _aggregate_fb(campaigns)
    print(f"[BUILD] All data ready in {time.time()-t0:.1f}s", flush=True)

    # Split events into active and past, then sort accordingly
    active_events = [e for e in all_event_data if not e["is_past"]]
    past_events = [e for e in all_event_data if e["is_past"]]

    # Deduplicate past events: same city + same start_date = same event
    # Keep the one with the higher total_sold (more complete data)
    seen_past = {}
    deduped_past = []
    for e in past_events:
        key = (e["city"].lower(), e["start_date"][:10])
        if key in seen_past:
            # Keep the one with more tickets sold
            existing = deduped_past[seen_past[key]]
            if e["total_sold"] > existing["total_sold"]:
                deduped_past[seen_past[key]] = e
        else:
            seen_past[key] = len(deduped_past)
            deduped_past.append(e)
    past_events = deduped_past

    active_events.sort(key=lambda x: x["event_num"])
    past_events.sort(key=lambda x: x["start_date"], reverse=True)

    # Generate HTML
    events_json = json.dumps({"active": active_events, "past": past_events})
    tickets_json = json.dumps(all_tickets_flat[:300])
    fb_json = json.dumps(fb_periods)
    generated_time = datetime.now(ET).strftime("%B %d, %Y at %I:%M %p") + " ET"

    # Build API error banner HTML
    api_error_banner = ""
    if _api_errors:
        error_items = "".join(f'<div style="margin:4px 0">&#9888; {e}</div>' for e in _api_errors)
        api_error_banner = f'''<div style="background:rgba(239,68,68,0.15);border:2px solid rgba(239,68,68,0.5);border-radius:12px;padding:16px 24px;margin:16px 32px;color:#fca5a5;font-size:14px;font-weight:500">
            <div style="font-size:16px;font-weight:700;color:#f87171;margin-bottom:8px">&#9888; Facebook Ads Data Unavailable</div>
            {error_items}
            <div style="margin-top:8px;font-size:12px;color:#94a3b8">Amount Spent, Meta Tickets, and Cost/Ticket columns will show $0 or &mdash; until this is resolved.</div>
        </div>'''

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
    <title>The Clarity Collective - Event Dashboard</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; }}
        .header {{ background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%); border-bottom: 1px solid #334155; padding: 20px 32px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px; }}
        .header h1 {{ font-size: 24px; font-weight: 700; color: #f8fafc; }}
        .header h1 span {{ color: #e91e8c; }}
        .generated {{ font-size: 12px; color: #94a3b8; }}
        .refresh-btn {{ padding: 6px 14px; border-radius: 8px; border: 1px solid #334155; background: #1e293b; color: #94a3b8; cursor: pointer; font-size: 12px; transition: all 0.15s; }}
        .refresh-btn:hover {{ border-color: #e91e8c; color: #e91e8c; }}
        .controls {{ padding: 16px 32px; display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }}
        .controls label {{ font-size: 12px; color: #94a3b8; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }}
        .dbtn {{ padding: 8px 16px; border-radius: 8px; border: 1px solid #334155; background: #1e293b; color: #e2e8f0; cursor: pointer; font-size: 13px; font-weight: 500; transition: all 0.15s; }}
        .dbtn:hover {{ border-color: #e91e8c; color: #e91e8c; }}
        .dbtn.active {{ background: #e91e8c; color: #ffffff; border-color: #e91e8c; font-weight: 700; }}
        .dinput {{ padding: 6px 10px; border-radius: 8px; border: 1px solid #334155; background: #1e293b; color: #e2e8f0; font-size: 13px; }}
        .dinput:focus {{ border-color: #e91e8c; outline: none; }}
        .tabs {{ display: flex; gap: 0; padding: 0 32px; border-bottom: 1px solid #334155; }}
        .tab {{ padding: 12px 24px; cursor: pointer; font-size: 14px; font-weight: 600; color: #94a3b8; border-bottom: 2px solid transparent; transition: all 0.15s; }}
        .tab:hover {{ color: #f8fafc; }}
        .tab.active {{ color: #e91e8c; border-bottom-color: #e91e8c; }}
        .tpanel {{ display: none; }}
        .tpanel.active {{ display: block; }}
        .cards {{ padding: 20px 32px; display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; }}
        .card {{ background: #1e293b; border-radius: 12px; padding: 18px; border: 1px solid #334155; }}
        .card .lb {{ font-size: 11px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }}
        .card .vl {{ font-size: 26px; font-weight: 700; color: #f8fafc; }}
        .card .vl.grn {{ color: #4ade80; }}
        .card .vl.amb {{ color: #e91e8c; }}
        .card .vl.red {{ color: #f87171; }}
        .alerts {{ padding: 0 32px 12px; }}
        .alert {{ padding: 12px 18px; border-radius: 10px; margin-bottom: 6px; font-size: 13px; display: flex; align-items: center; gap: 8px; }}
        .alert-warn {{ background: rgba(248,113,113,0.1); border: 1px solid rgba(248,113,113,0.3); color: #fca5a5; }}
        .alert-info {{ background: rgba(96,165,250,0.1); border: 1px solid rgba(96,165,250,0.3); color: #93c5fd; }}
        .section-header {{ padding: 16px 32px; display: flex; align-items: center; gap: 10px; cursor: pointer; user-select: none; }}
        .section-header h2 {{ font-size: 18px; font-weight: 700; color: #f8fafc; }}
        .section-header .arrow {{ color: #94a3b8; transition: transform 0.2s; }}
        .section-header .count {{ font-size: 13px; color: #94a3b8; font-weight: 400; }}
        .section-past {{ border-top: 1px solid #334155; margin-top: 24px; padding-top: 8px; }}
        .section-content {{ display: block; }}
        .section-content.collapsed {{ display: none; }}
        .tbl-wrap {{ padding: 16px 32px; overflow-x: auto; }}
        table {{ width: 100%; border-collapse: collapse; background: #1e293b; border-radius: 12px; overflow: hidden; border: 1px solid #334155; }}
        thead th {{ padding: 12px 14px; text-align: left; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; color: #94a3b8; background: #0f172a; border-bottom: 1px solid #334155; font-weight: 600; white-space: nowrap; }}
        tbody td {{ padding: 12px 14px; font-size: 13px; border-bottom: 1px solid rgba(51,65,85,0.5); }}
        tbody tr:hover {{ background: #334155; }}
        .cn {{ font-weight: 600; color: #f8fafc; }}
        .bar {{ width: 100px; height: 7px; background: #334155; border-radius: 4px; overflow: hidden; display: inline-block; vertical-align: middle; margin-right: 6px; }}
        .bar-fill {{ height: 100%; border-radius: 4px; }}
        .bg {{ background: linear-gradient(90deg, #4ade80, #22c55e); }}
        .bb {{ background: linear-gradient(90deg, #60a5fa, #3b82f6); }}
        .ba {{ background: linear-gradient(90deg, #f08080, #e06060); }}
        .br {{ background: linear-gradient(90deg, #f87171, #ef4444); }}
        .tag {{ display: inline-block; padding: 2px 10px; border-radius: 20px; font-size: 11px; font-weight: 600; }}
        .tag-so {{ background: rgba(74,222,128,0.15); color: #4ade80; }}
        .tag-st {{ background: rgba(96,165,250,0.15); color: #60a5fa; }}
        .tag-mo {{ background: rgba(240,128,128,0.15); color: #f08080; }}
        .tag-sl {{ background: rgba(248,113,113,0.15); color: #f87171; }}
        .oitem {{ display: flex; align-items: center; gap: 14px; padding: 9px 14px; background: #1e293b; border-radius: 8px; border: 1px solid #334155; font-size: 13px; margin-bottom: 4px; }}
        .oitem .oc {{ font-weight: 600; color: #e91e8c; min-width: 110px; }}
        .oitem .ot {{ color: #94a3b8; min-width: 150px; }}
        .oitem .oa {{ color: #4ade80; font-weight: 600; margin-left: auto; }}
        .totrow {{ background: #0f172a !important; font-weight: 700; border-top: 2px solid #e91e8c; }}
        .totrow td {{ color: #e91e8c; }}
    </style>
</head>
<body>
    <div class="header">
        <h1><span>The Clarity Collective</span> Event Dashboard</h1>
        <div style="display:flex;align-items:center;gap:12px">
            <div class="generated">Data pulled: {generated_time}</div>
            <button class="refresh-btn" onclick="window.location.href='/refresh'">&#x21BB; Refresh Data</button>
        </div>
    </div>
    {api_error_banner}
    <div class="controls">
        <label>Period:</label>
        <button class="dbtn active" onclick="setPeriod('last7',this)">Last 7 Days</button>
        <button class="dbtn" onclick="setPeriod('today',this)">Today</button>
        <button class="dbtn" onclick="setPeriod('yesterday',this)">Yesterday</button>
        <button class="dbtn" onclick="setPeriod('last2',this)">Last 2 Days</button>
        <button class="dbtn" onclick="setPeriod('last30',this)">Last 30 Days</button>
        <button class="dbtn" onclick="setPeriod('all',this)">All Time</button>
        <span style="margin-left:12px;border-left:1px solid #334155;padding-left:12px">
            <label>Custom:</label>
            <input type="date" id="customStart" class="dinput" onchange="applyCustomRange()">
            <span style="color:#94a3b8;font-size:12px">to</span>
            <input type="date" id="customEnd" class="dinput" onchange="applyCustomRange()">
        </span>
        <span style="margin-left:12px;border-left:1px solid #334155;padding-left:12px">
            <label>Event Type:</label>
            <select id="eventTypeFilter" class="dinput" onchange="render()" style="cursor:pointer">
                <option value="all">All Events</option>
                <option value="1">1-Day Only</option>
                <option value="2">2-Day Only</option>
            </select>
        </span>
        <div id="periodLabel" style="width:100%;font-size:13px;color:#94a3b8;margin-top:4px;padding-left:2px"></div>
    </div>
    <div class="tabs">
        <div class="tab active" onclick="showTab('overview',this)">Overview</div>
        <div class="tab" onclick="showTab('combined',this)">Ads + Tickets</div>
        <div class="tab" onclick="showTab('orders',this)">Ticket Sales</div>
    </div>
    <div class="tpanel active" id="p-overview">
        <div class="cards" id="summaryCards"></div>
        <div class="alerts" id="alertBox"></div>
        <div id="activeSection">
            <div class="section-header" onclick="toggleSection('activePastTable')">
                <h2>Active Events</h2>
                <span class="count" id="activeCount"></span>
            </div>
            <div class="tbl-wrap"><table><thead><tr>
                <th>City</th><th>Event Date</th><th>Days Out</th><th>Amount Spent</th><th>Link Clicks</th><th>Tickets Sold (EB)</th><th>Conv Rate</th><th>Cost/Ticket (EB)</th><th>Tickets Sold (Meta)</th><th>Cost/Ticket (Meta)</th><th>Total Sold</th><th>Capacity</th><th>Fill %</th><th>Period Revenue</th><th>Status</th>
            </tr></thead><tbody id="tblBody"></tbody></table></div>
        </div>
        <div id="pastSection" class="section-past">
            <div class="section-header" onclick="toggleSection('pastTable')">
                <span class="arrow" id="pastArrow">&#9656;</span>
                <h2>Past Events</h2>
                <span class="count" id="pastCount"></span>
            </div>
            <div class="tbl-wrap section-content collapsed" id="pastTable"><table><thead><tr>
                <th>City</th><th>Event Date</th><th>Ended</th><th>Total Ad Spend</th><th>Link Clicks</th><th>Tickets Sold (EB)</th><th>Conv Rate</th><th>Cost/Ticket (EB)</th><th>Tickets Sold (Meta)</th><th>Cost/Ticket (Meta)</th><th>Total Sold</th><th>Capacity</th><th>Fill %</th><th>Total Revenue</th><th>Status</th>
            </tr></thead><tbody id="tblBodyPast"></tbody></table></div>
        </div>
    </div>
    <div class="tpanel" id="p-combined">
        <div style="padding:20px 32px 0">
            <h2 style="font-size:18px;color:#f8fafc;margin-bottom:4px">Real Tickets (EB) vs Ad Spend (Meta)</h2>
            <p style="font-size:13px;color:#94a3b8;margin-bottom:16px">EB = source of truth for sales (no attribution delay). Meta = source of truth for spend.</p>
        </div>
        <div id="activeAdSection">
            <div class="section-header">
                <h2>Active Events</h2>
                <span class="count" id="activeAdCount"></span>
            </div>
            <div class="tbl-wrap"><table><thead><tr>
                <th>City</th><th>Tickets Sold (EB)</th><th>Revenue</th><th>Ad Spend</th><th>Link Clicks</th><th>Conv Rate</th><th>Cost/Ticket (EB)</th><th>ROAS</th><th>Tickets Sold (Meta)</th><th>Impressions</th><th>Reach</th>
            </tr></thead><tbody id="cmbBody"></tbody></table></div>
        </div>
        <div id="pastAdSection" class="section-past">
            <div class="section-header" onclick="toggleSection('pastAdTable')">
                <span class="arrow" id="pastAdArrow">&#9656;</span>
                <h2>Past Events</h2>
                <span class="count" id="pastAdCount"></span>
            </div>
            <div class="tbl-wrap section-content collapsed" id="pastAdTable"><table><thead><tr>
                <th>City</th><th>Tickets Sold (EB)</th><th>Revenue</th><th>Ad Spend</th><th>Link Clicks</th><th>Conv Rate</th><th>Cost/Ticket (EB)</th><th>ROAS</th><th>Tickets Sold (Meta)</th><th>Impressions</th><th>Reach</th>
            </tr></thead><tbody id="cmbBodyPast"></tbody></table></div>
        </div>
    </div>
    <div class="tpanel" id="p-orders">
        <div style="padding:20px 32px">
            <h2 style="font-size:18px;color:#f8fafc;margin-bottom:12px">Recent Ticket Sales <span style="font-size:13px;color:#94a3b8">(within selected period)</span></h2>
            <div id="orderList"></div>
        </div>
    </div>

    <script>
    const eventData = {events_json};
    const events = eventData.active || [];
    const pastEvents = eventData.past || [];
    const allTickets = {tickets_json};
    const fbData = {fb_json};
    let period = 'last7';

    const periodStarts = {{
        'today': new Date(new Date().setHours(0,0,0,0)),
        'yesterday': new Date(new Date().setHours(0,0,0,0) - 86400000),
        'last2': new Date(new Date().setHours(0,0,0,0) - 2*86400000),
        'last7': new Date(new Date().setHours(0,0,0,0) - 7*86400000),
        'last30': new Date(new Date().setHours(0,0,0,0) - 30*86400000),
        'all': new Date('2020-01-01')
    }};
    const periodEnds = {{
        'today': new Date(new Date().setHours(23,59,59,999)),
        'yesterday': new Date(new Date().setHours(0,0,0,0) - 1),
        'last2': new Date(new Date().setHours(23,59,59,999)),
        'last7': new Date(new Date().setHours(23,59,59,999)),
        'last30': new Date(new Date().setHours(23,59,59,999)),
        'all': new Date(new Date().setHours(23,59,59,999))
    }};

    function formatDate(d) {{
        return d.toLocaleDateString('en-US', {{weekday:'short', month:'long', day:'numeric', year:'numeric'}});
    }}

    function toggleSection(sectionId) {{
        const section = document.getElementById(sectionId);
        const arrowId = sectionId === 'pastTable' ? 'pastArrow' : sectionId === 'pastAdTable' ? 'pastAdArrow' : null;
        if (section) {{
            section.classList.toggle('collapsed');
            if (arrowId) {{
                const arrow = document.getElementById(arrowId);
                if (arrow) {{
                    arrow.textContent = section.classList.contains('collapsed') ? '\\u25b6' : '\\u25bc';
                }}
            }}
        }}
    }}

    function updatePeriodLabel() {{
        const lbl = document.getElementById('periodLabel');
        const now = new Date();
        const todayMid = new Date(now.getFullYear(), now.getMonth(), now.getDate());
        const labels = {{
            'today': `Today: ${{formatDate(todayMid)}}`,
            'yesterday': `Yesterday: ${{formatDate(new Date(todayMid - 86400000))}}`,
            'last2': `Last 2 Days: ${{formatDate(new Date(todayMid - 2*86400000))}} \\u2014 ${{formatDate(todayMid)}}`,
            'last7': `Last 7 Days: ${{formatDate(new Date(todayMid - 7*86400000))}} \\u2014 ${{formatDate(todayMid)}}`,
            'last30': `Last 30 Days: ${{formatDate(new Date(todayMid - 30*86400000))}} \\u2014 ${{formatDate(todayMid)}}`,
            'all': 'All Time'
        }};
        if (period === 'custom') {{
            const s = document.getElementById('customStart').value;
            const e = document.getElementById('customEnd').value;
            if (s && e) lbl.textContent = `Custom Range: ${{formatDate(new Date(s+'T00:00:00'))}} \\u2014 ${{formatDate(new Date(e+'T00:00:00'))}}`;
            else lbl.textContent = 'Select start and end dates';
        }} else {{
            lbl.textContent = labels[period] || '';
        }}
    }}

    function setPeriod(p, el) {{
        period = p;
        document.querySelectorAll('.dbtn').forEach(b=>b.classList.remove('active'));
        if(el) el.classList.add('active');
        if(p !== 'custom') {{
            document.getElementById('customStart').value = '';
            document.getElementById('customEnd').value = '';
        }}
        updatePeriodLabel();
        render();
    }}

    function applyCustomRange() {{
        const s = document.getElementById('customStart').value;
        const e = document.getElementById('customEnd').value;
        if (!s || !e) return;
        document.querySelectorAll('.dbtn').forEach(b=>b.classList.remove('active'));
        periodStarts['custom'] = new Date(s + 'T00:00:00');
        periodEnds['custom'] = new Date(e + 'T23:59:59.999');
        period = 'custom';
        updatePeriodLabel();
        // Fetch FB data for custom range, then render
        fetchCustomFb(s, e).then(() => render());
    }}
    function showTab(t, el) {{
        document.querySelectorAll('.tab').forEach(b=>b.classList.remove('active'));
        document.querySelectorAll('.tpanel').forEach(b=>b.classList.remove('active'));
        el.classList.add('active');
        document.getElementById('p-'+t).classList.add('active');
    }}

    function filterOrders(orders) {{
        const start = periodStarts[period];
        const end = periodEnds[period];
        return orders.filter(o => {{
            const d = new Date(o.created);
            return d >= start && d <= end;
        }});
    }}

    function daysOut(dateStr) {{
        return Math.ceil((new Date(dateStr) - new Date()) / 86400000);
    }}

    function daysOutLabel(days, isPast) {{
        if (isPast) {{
            if (days === 0) return '<span style="color:#94a3b8">Today</span>';
            if (days === -1) return '<span style="color:#94a3b8">1d ago</span>';
            return `<span style="color:#94a3b8">${{Math.abs(days)}}d ago</span>`;
        }}
        if (days <= 0) return '<span style="color:#e91e8c">TODAY</span>';
        return days + 'd';
    }}

    function fmt(n) {{ return n.toLocaleString('en-US', {{minimumFractionDigits:2, maximumFractionDigits:2}}); }}

    function barColor(pct) {{
        if(pct>=75) return 'bg';
        if(pct>=40) return 'bb';
        if(pct>=20) return 'ba';
        return 'br';
    }}

    function statusTag(pct, days, isPast, fbStatus) {{
        if (isPast) {{
            const label = fbStatus === 'ACTIVE' ? 'Ended' : (fbStatus === 'UNKNOWN' ? 'Ended' : fbStatus);
            return `<span class="tag tag-mo">${{label}}</span>`;
        }}
        if (fbStatus === 'UNKNOWN') return '<span class="tag tag-mo">No Ads</span>';
        if (fbStatus === 'PAUSED') return '<span class="tag tag-mo">Paused</span>';
        if(pct>=95) return '<span class="tag tag-so">Nearly Sold Out</span>';
        if(pct>=60) return '<span class="tag tag-st">Strong</span>';
        if(pct>=30||days>30) return '<span class="tag tag-mo">Moderate</span>';
        return '<span class="tag tag-sl">Needs Attention</span>';
    }}

    function filterTickets(tickets) {{
        const start = periodStarts[period];
        const end = periodEnds[period];
        return tickets.filter(t => {{
            const d = new Date(t.created);
            return d >= start && d <= end;
        }});
    }}

    let _customFbCache = {{}};
    let _customFbLoading = false;

    function getFbForPeriod() {{
        if (period !== 'custom') return fbData[period] || {{}};
        // For custom ranges, return cached result (fetched async by applyCustomRange)
        return _customFbCache;
    }}

    function fetchCustomFb(since, until) {{
        _customFbLoading = true;
        _customFbCache = {{}};
        return fetch(`/api/fb_custom?since=${{since}}&until=${{until}}`)
            .then(r => r.json())
            .then(data => {{
                _customFbCache = data;
                _customFbLoading = false;
                return data;
            }})
            .catch(() => {{
                _customFbLoading = false;
                return {{}};
            }});
    }}

    function filterByType(arr) {{
        const typeVal = document.getElementById('eventTypeFilter').value;
        if (typeVal === 'all') return arr;
        const dur = parseInt(typeVal);
        return arr.filter(e => e.duration_days === dur);
    }}

    function render() {{
        const fb = getFbForPeriod();
        let totalPeriodTickets=0, totalMetaTickets=0, totalPeriodRev=0, totalSold=0, totalCap=0, totalSpend=0, totalLinkClicks=0, drySpells=0;
        let rows='', pastRows='', cmbRows='', pastCmbRows='', alerts=[];

        const filteredEvents = filterByType(events);
        const filteredPast = filterByType(pastEvents);

        // Render active events
        filteredEvents.forEach(e => {{
            const pTickets = filterTickets(e.tickets);
            const pOrders = filterOrders(e.orders);
            const pRev = pOrders.reduce((s,o)=>s+o.amount,0);
            const days = daysOut(e.start_date);
            const d = new Date(e.start_date);
            const dateStr = d.toLocaleDateString('en-US',{{weekday:'short',month:'short',day:'numeric'}});

            // Match FB data: first by event number, then by city+date, then by city alone
            let fbd = null;
            if (e.event_num) fbd = fb[String(e.event_num)] || null;
            if (!fbd) {{
                // Try date-specific city key first (e.g. "city:Orlando:2026-06")
                const ed = new Date(e.start_date);
                const ym = ed.getFullYear() + '-' + String(ed.getMonth()+1).padStart(2,'0');
                fbd = fb['city:' + e.city + ':' + ym] || null;
                // Also check adjacent months (campaign may say MAY but event is in JUN)
                if (!fbd) {{
                    const prev = new Date(ed); prev.setMonth(prev.getMonth()-1);
                    const next = new Date(ed); next.setMonth(next.getMonth()+1);
                    const ymPrev = prev.getFullYear() + '-' + String(prev.getMonth()+1).padStart(2,'0');
                    const ymNext = next.getFullYear() + '-' + String(next.getMonth()+1).padStart(2,'0');
                    fbd = fb['city:' + e.city + ':' + ymPrev] || fb['city:' + e.city + ':' + ymNext] || null;
                }}
            }}
            const spend = fbd ? fbd.spend : 0;
            const metaTickets = fbd ? fbd.purchases : 0;
            const linkClicks = fbd ? (fbd.link_clicks || 0) : 0;
            const overviewCpt = pTickets.length>0&&spend>0 ? spend/pTickets.length : 0;
            const metaCpt = metaTickets>0&&spend>0 ? spend/metaTickets : 0;
            const convRate = linkClicks>0 ? (pTickets.length/linkClicks*100) : 0;

            totalPeriodTickets += pTickets.length;
            totalMetaTickets += metaTickets;
            totalLinkClicks += linkClicks;
            totalPeriodRev += pRev;
            totalSold += e.total_sold;
            totalCap += e.capacity;
            totalSpend += spend;

            if(days>2 && e.total_sold>5) {{
                const recent = e.tickets.filter(t=>(new Date()-new Date(t.created))<48*3600000);
                if(recent.length===0) {{ drySpells++; alerts.push({{type:'warn',text:`${{e.city}}: No ticket sales in last 48 hours (${{days}} days out, ${{e.fill_pct}}% full)`}}); }}
            }}
            if(days<=30 && days>0 && e.fill_pct<30) alerts.push({{type:'warn',text:`${{e.city}}: Only ${{e.fill_pct}}% full with ${{days}} days to go`}});
            if(e.fill_pct>=90 && e.fill_pct<100) alerts.push({{type:'info',text:`${{e.city}}: ${{e.fill_pct}}% full &#8212; only ${{e.capacity-e.total_sold}} tickets remaining!`}});

            const ebCptColor = overviewCpt>300?'#f87171':overviewCpt>200?'#f08080':overviewCpt>0?'#4ade80':'#94a3b8';
            const metaCptColor = metaCpt>300?'#f87171':metaCpt>200?'#f08080':metaCpt>0?'#60a5fa':'#94a3b8';
            const convRateColor = convRate>=10?'#4ade80':convRate>=5?'#f08080':convRate>0?'#f87171':'#94a3b8';
            rows += `<tr>
                <td class="cn">${{e.display_city||e.city}}</td>
                <td style="color:#94a3b8">${{dateStr}}</td>
                <td>${{daysOutLabel(days, false)}}</td>
                <td style="color:#e91e8c">${{spend>0?'$'+fmt(spend):'$0.00'}}</td>
                <td style="color:#94a3b8">${{linkClicks>0?linkClicks.toLocaleString():'&#8212;'}}</td>
                <td style="font-weight:600;color:#4ade80">${{pTickets.length}}</td>
                <td style="font-weight:600;color:${{convRateColor}}">${{convRate>0?convRate.toFixed(1)+'%':'&#8212;'}}</td>
                <td style="font-weight:600;color:${{ebCptColor}}">${{overviewCpt>0?'$'+fmt(overviewCpt):'&#8212;'}}</td>
                <td style="color:#60a5fa">${{metaTickets}}</td>
                <td style="font-weight:600;color:${{metaCptColor}}">${{metaCpt>0?'$'+fmt(metaCpt):'&#8212;'}}</td>
                <td>${{e.total_sold}}</td>
                <td>${{e.capacity}}</td>
                <td><div class="bar"><div class="bar-fill ${{barColor(e.fill_pct)}}" style="width:${{e.fill_pct}}%"></div></div>${{e.fill_pct}}%</td>
                <td style="color:#4ade80">$${{fmt(pRev)}}</td>
                <td>${{statusTag(e.fill_pct, days, false, e.fb_status)}}</td>
            </tr>`;

            const fbPurch = fbd ? fbd.purchases : 0;
            const impr = fbd ? fbd.impressions : 0;
            const reach = fbd ? fbd.reach : 0;
            const cpt = pTickets.length>0&&spend>0 ? spend/pTickets.length : 0;
            const roas = spend>0 ? pRev/spend : 0;

            const cptColor = cpt>300?'#f87171':cpt>200?'#f08080':cpt>0?'#4ade80':'#94a3b8';
            const roasColor = roas>=1?'#4ade80':'#f87171';
            const cmbConvRate = linkClicks>0 ? (pTickets.length/linkClicks*100) : 0;
            const cmbConvColor = cmbConvRate>=10?'#4ade80':cmbConvRate>=5?'#f08080':cmbConvRate>0?'#f87171':'#94a3b8';
            cmbRows += `<tr>
                <td class="cn">${{e.display_city||e.city}}</td>
                <td style="font-weight:600;color:#4ade80">${{pTickets.length}}</td>
                <td style="color:#4ade80">$${{fmt(pRev)}}</td>
                <td style="color:#e91e8c">$${{fmt(spend)}}</td>
                <td style="color:#94a3b8">${{linkClicks>0?linkClicks.toLocaleString():'&#8212;'}}</td>
                <td style="font-weight:600;color:${{cmbConvColor}}">${{cmbConvRate>0?cmbConvRate.toFixed(1)+'%':'&#8212;'}}</td>
                <td style="font-weight:700;color:${{cptColor}}">${{cpt>0?'$'+fmt(cpt):'&#8212;'}}</td>
                <td style="color:${{roasColor}}">${{roas>0?roas.toFixed(2)+'x':'&#8212;'}}</td>
                <td style="color:#94a3b8">${{fbPurch}}</td>
                <td style="color:#94a3b8">${{impr.toLocaleString()}}</td>
                <td style="color:#94a3b8">${{reach.toLocaleString()}}</td>
            </tr>`;
        }});

        // Render past events - always use "all time" FB data for spend
        const fbAll = fbData['all'] || {{}};
        filteredPast.forEach(e => {{
            const pTickets = filterTickets(e.tickets);
            const pOrders = filterOrders(e.orders);
            const pRev = pOrders.reduce((s,o)=>s+o.amount,0);
            const days = daysOut(e.start_date);
            const d = new Date(e.start_date);
            const dateStr = d.toLocaleDateString('en-US',{{weekday:'short',month:'short',day:'numeric',year:'numeric'}});

            // Match FB data: first by event number, then by city+date, then by city alone
            let fbd = null;
            if (e.event_num) fbd = fbAll[String(e.event_num)] || null;
            if (!fbd) {{
                const ed = new Date(e.start_date);
                const ym = ed.getFullYear() + '-' + String(ed.getMonth()+1).padStart(2,'0');
                fbd = fbAll['city:' + e.city + ':' + ym] || null;
                if (!fbd) {{
                    const prev = new Date(ed); prev.setMonth(prev.getMonth()-1);
                    const next = new Date(ed); next.setMonth(next.getMonth()+1);
                    const ymPrev = prev.getFullYear() + '-' + String(prev.getMonth()+1).padStart(2,'0');
                    const ymNext = next.getFullYear() + '-' + String(next.getMonth()+1).padStart(2,'0');
                    fbd = fbAll['city:' + e.city + ':' + ymPrev] || fbAll['city:' + e.city + ':' + ymNext] || null;
                }}
            }}
            const spend = fbd ? fbd.spend : 0;
            const metaTickets = fbd ? fbd.purchases : 0;
            const pastLinkClicks = fbd ? (fbd.link_clicks || 0) : 0;
            const ebSold = e.total_sold || pTickets.length;
            const overviewCpt = ebSold>0&&spend>0 ? spend/ebSold : 0;
            const metaCpt = metaTickets>0&&spend>0 ? spend/metaTickets : 0;
            const pastConvRate = pastLinkClicks>0 ? (ebSold/pastLinkClicks*100) : 0;

            const ebCptColor = overviewCpt>300?'#f87171':overviewCpt>200?'#f08080':overviewCpt>0?'#4ade80':'#94a3b8';
            const metaCptColor = metaCpt>300?'#f87171':metaCpt>200?'#f08080':metaCpt>0?'#60a5fa':'#94a3b8';
            const pastConvColor = pastConvRate>=10?'#4ade80':pastConvRate>=5?'#f08080':pastConvRate>0?'#f87171':'#94a3b8';
            pastRows += `<tr>
                <td class="cn">${{e.display_city||e.city}}</td>
                <td style="color:#94a3b8">${{dateStr}}</td>
                <td>${{daysOutLabel(days, true)}}</td>
                <td style="color:#e91e8c">${{spend>0?'$'+fmt(spend):'$0.00'}}</td>
                <td style="color:#94a3b8">${{pastLinkClicks>0?pastLinkClicks.toLocaleString():'&#8212;'}}</td>
                <td style="font-weight:600;color:#4ade80">${{ebSold}}</td>
                <td style="font-weight:600;color:${{pastConvColor}}">${{pastConvRate>0?pastConvRate.toFixed(1)+'%':'&#8212;'}}</td>
                <td style="font-weight:600;color:${{ebCptColor}}">${{overviewCpt>0?'$'+fmt(overviewCpt):'&#8212;'}}</td>
                <td style="color:#60a5fa">${{metaTickets}}</td>
                <td style="font-weight:600;color:${{metaCptColor}}">${{metaCpt>0?'$'+fmt(metaCpt):'&#8212;'}}</td>
                <td>${{e.total_sold}}</td>
                <td>${{e.capacity}}</td>
                <td><div class="bar"><div class="bar-fill ${{barColor(e.fill_pct)}}" style="width:${{e.fill_pct}}%"></div></div>${{e.fill_pct}}%</td>
                <td style="color:#4ade80">$${{fmt(pRev)}}</td>
                <td>${{statusTag(e.fill_pct, days, true, e.fb_status)}}</td>
            </tr>`;

            const fbPurch = fbd ? fbd.purchases : 0;
            const impr = fbd ? fbd.impressions : 0;
            const reach = fbd ? fbd.reach : 0;
            const cpt = ebSold>0&&spend>0 ? spend/ebSold : 0;
            const roas = spend>0 ? pRev/spend : 0;

            const cptColor = cpt>300?'#f87171':cpt>200?'#f08080':cpt>0?'#4ade80':'#94a3b8';
            const roasColor = roas>=1?'#4ade80':'#f87171';
            const pastCmbConvRate = pastLinkClicks>0 ? (ebSold/pastLinkClicks*100) : 0;
            const pastCmbConvColor = pastCmbConvRate>=10?'#4ade80':pastCmbConvRate>=5?'#f08080':pastCmbConvRate>0?'#f87171':'#94a3b8';
            pastCmbRows += `<tr>
                <td class="cn">${{e.display_city||e.city}}</td>
                <td style="font-weight:600;color:#4ade80">${{ebSold}}</td>
                <td style="color:#4ade80">$${{fmt(pRev)}}</td>
                <td style="color:#e91e8c">$${{fmt(spend)}}</td>
                <td style="color:#94a3b8">${{pastLinkClicks>0?pastLinkClicks.toLocaleString():'&#8212;'}}</td>
                <td style="font-weight:600;color:${{pastCmbConvColor}}">${{pastCmbConvRate>0?pastCmbConvRate.toFixed(1)+'%':'&#8212;'}}</td>
                <td style="font-weight:700;color:${{cptColor}}">${{cpt>0?'$'+fmt(cpt):'&#8212;'}}</td>
                <td style="color:${{roasColor}}">${{roas>0?roas.toFixed(2)+'x':'&#8212;'}}</td>
                <td style="color:#94a3b8">${{fbPurch}}</td>
                <td style="color:#94a3b8">${{impr.toLocaleString()}}</td>
                <td style="color:#94a3b8">${{reach.toLocaleString()}}</td>
            </tr>`;
        }});

        const tCpt = totalPeriodTickets>0&&totalSpend>0 ? totalSpend/totalPeriodTickets : 0;
        const tRoas = totalSpend>0 ? totalPeriodRev/totalSpend : 0;

        const totalConvRate = totalLinkClicks>0 ? (totalPeriodTickets/totalLinkClicks*100) : 0;
        const totalConvColor = totalConvRate>=10?'#4ade80':totalConvRate>=5?'#f08080':totalConvRate>0?'#f87171':'#94a3b8';
        rows += `<tr class="totrow">
            <td>TOTALS</td>
            <td></td><td></td>
            <td>$${{fmt(totalSpend)}}</td>
            <td>${{totalLinkClicks>0?totalLinkClicks.toLocaleString():'&#8212;'}}</td>
            <td>${{totalPeriodTickets}}</td>
            <td style="color:${{totalConvColor}}">${{totalConvRate>0?totalConvRate.toFixed(1)+'%':'&#8212;'}}</td>
            <td style="color:${{tCpt>300?'#f87171':tCpt>200?'#f08080':'#4ade80'}}">${{tCpt>0?'$'+fmt(tCpt):'&#8212;'}}</td>
            <td>${{totalMetaTickets}}</td>
            <td></td>
            <td>${{totalSold}}</td>
            <td>${{totalCap}}</td>
            <td></td>
            <td style="color:#4ade80">$${{fmt(totalPeriodRev)}}</td>
            <td></td>
        </tr>`;

        cmbRows += `<tr class="totrow">
            <td>TOTALS</td>
            <td style="color:#4ade80">${{totalPeriodTickets}}</td>
            <td style="color:#4ade80">$${{fmt(totalPeriodRev)}}</td>
            <td>$${{fmt(totalSpend)}}</td>
            <td>${{totalLinkClicks>0?totalLinkClicks.toLocaleString():'&#8212;'}}</td>
            <td style="color:${{totalConvColor}}">${{totalConvRate>0?totalConvRate.toFixed(1)+'%':'&#8212;'}}</td>
            <td style="color:${{tCpt>300?'#f87171':tCpt>200?'#f08080':'#4ade80'}}">${{tCpt>0?'$'+fmt(tCpt):'&#8212;'}}</td>
            <td style="color:${{tRoas>=1?'#4ade80':'#f87171'}}">${{tRoas>0?tRoas.toFixed(2)+'x':'&#8212;'}}</td>
            <td></td><td></td><td></td>
        </tr>`;

        document.getElementById('tblBody').innerHTML = rows;
        document.getElementById('tblBodyPast').innerHTML = pastRows;
        document.getElementById('cmbBody').innerHTML = cmbRows;
        document.getElementById('cmbBodyPast').innerHTML = pastCmbRows;

        document.getElementById('activeCount').textContent = `(${{filteredEvents.length}})`;
        document.getElementById('pastCount').textContent = `(${{filteredPast.length}})`;
        document.getElementById('activeAdCount').textContent = `(${{filteredEvents.length}})`;
        document.getElementById('pastAdCount').textContent = `(${{filteredPast.length}})`;

        // Hide past section if no past events
        const pastSection = document.getElementById('pastSection');
        const pastAdSection = document.getElementById('pastAdSection');
        if (filteredPast.length === 0) {{
            pastSection.style.display = 'none';
            pastAdSection.style.display = 'none';
        }} else {{
            pastSection.style.display = 'block';
            pastAdSection.style.display = 'block';
        }}

        const fillPct = totalCap>0?Math.round(totalSold/totalCap*100):0;
        const avgCpt = totalPeriodTickets>0&&totalSpend>0 ? totalSpend/totalPeriodTickets : 0;
        document.getElementById('summaryCards').innerHTML = `
            <div class="card"><div class="lb">Ticket Sales (Period)</div><div class="vl">${{totalPeriodTickets}}</div></div>
            <div class="card"><div class="lb">Period Revenue</div><div class="vl grn">$${{fmt(totalPeriodRev)}}</div></div>
            <div class="card"><div class="lb">Period Ad Spend</div><div class="vl amb">$${{fmt(totalSpend)}}</div></div>
            <div class="card"><div class="lb">Avg Cost/Ticket</div><div class="vl ${{avgCpt>300?'red':avgCpt>200?'amb':'grn'}}">${{avgCpt>0?'$'+fmt(avgCpt):'&#8212;'}}</div></div>
            <div class="card"><div class="lb">Total Sold (All Time)</div><div class="vl">${{totalSold}} / ${{totalCap}}</div></div>
            <div class="card"><div class="lb">Overall Fill Rate</div><div class="vl ${{fillPct>50?'grn':'amb'}}">${{fillPct}}%</div></div>
            <div class="card"><div class="lb">Dry Spell Alerts</div><div class="vl ${{drySpells>0?'red':'grn'}}">${{drySpells>0?drySpells+' cities':'None'}}</div></div>
        `;

        document.getElementById('alertBox').innerHTML = alerts.map(a=>`<div class="alert alert-${{a.type}}">${{a.type==='warn'?'\\u26A0':'\\u2139'}} ${{a.text}}</div>`).join('');

        const pTicketsAll = filterTickets(allTickets);
        if(pTicketsAll.length===0) {{
            document.getElementById('orderList').innerHTML = '<div style="text-align:center;padding:40px;color:#94a3b8">No ticket sales in selected period</div>';
        }} else {{
            document.getElementById('orderList').innerHTML = pTicketsAll.slice(0,150).map(t => {{
                const d = new Date(t.created);
                return `<div class="oitem">
                    <div class="oc">${{t.city}}</div>
                    <div class="ot">${{d.toLocaleDateString('en-US',{{weekday:'short',month:'short',day:'numeric'}})}} at ${{d.toLocaleTimeString('en-US',{{hour:'numeric',minute:'2-digit'}})}}</div>
                    <div>${{t.name}}</div>
                    <div style="color:#94a3b8;font-size:12px;margin-left:auto">${{t.ticket_type}}</div>
                </div>`;
            }}).join('');
        }}
    }}

    render();
    updatePeriodLabel();
    </script>
</body>
</html>"""
    print(f"[BUILD] Dashboard build complete in {time.time()-t0:.1f}s", flush=True)
    return html

LOADING_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>The Clarity Collective - Event Dashboard — Loading</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; display: flex; flex-direction: column; align-items: center; justify-content: center; }
        .loader-wrap { text-align: center; }
        h1 { font-size: 28px; font-weight: 700; margin-bottom: 8px; }
        h1 span { color: #e91e8c; }
        .sub { color: #94a3b8; font-size: 15px; margin-bottom: 32px; }
        .spinner { width: 48px; height: 48px; border: 4px solid #334155; border-top-color: #e91e8c; border-radius: 50%; animation: spin 0.8s linear infinite; margin: 0 auto 24px; }
        @keyframes spin { to { transform: rotate(360deg); } }
        .status { color: #64748b; font-size: 13px; }
        .dot { animation: blink 1.4s infinite; }
        .dot:nth-child(2) { animation-delay: 0.2s; }
        .dot:nth-child(3) { animation-delay: 0.4s; }
        @keyframes blink { 0%,80%,100% { opacity: 0; } 40% { opacity: 1; } }
    </style>
</head>
<body>
    <div class="loader-wrap">
        <div class="spinner"></div>
        <h1><span>The Clarity Collective</span> Event Dashboard</h1>
        <p class="sub">Pulling live data from Eventbrite &amp; Facebook Ads</p>
        <p class="status">Loading fresh data<span class="dot">.</span><span class="dot">.</span><span class="dot">.</span></p>
    </div>
    <script>
        (function poll() {
            fetch('/api/status').then(r => r.json()).then(d => {
                if (d.ready) { window.location.reload(); }
                else { setTimeout(poll, 2000); }
            }).catch(() => setTimeout(poll, 3000));
        })();
    </script>
</body>
</html>"""

REFRESH_WAIT_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>The Clarity Collective - Event Dashboard — Refreshing</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; display: flex; flex-direction: column; align-items: center; justify-content: center; }
        .loader-wrap { text-align: center; }
        h1 { font-size: 28px; font-weight: 700; margin-bottom: 8px; }
        h1 span { color: #e91e8c; }
        .sub { color: #94a3b8; font-size: 15px; margin-bottom: 32px; }
        .spinner { width: 48px; height: 48px; border: 4px solid #334155; border-top-color: #e91e8c; border-radius: 50%; animation: spin 0.8s linear infinite; margin: 0 auto 24px; }
        @keyframes spin { to { transform: rotate(360deg); } }
        .status { color: #64748b; font-size: 13px; }
        .elapsed { color: #475569; font-size: 12px; margin-top: 12px; }
        .dot { animation: blink 1.4s infinite; }
        .dot:nth-child(2) { animation-delay: 0.2s; }
        .dot:nth-child(3) { animation-delay: 0.4s; }
        @keyframes blink { 0%,80%,100% { opacity: 0; } 40% { opacity: 1; } }
    </style>
</head>
<body>
    <div class="loader-wrap">
        <div class="spinner"></div>
        <h1><span>The Clarity Collective</span> Event Dashboard</h1>
        <p class="sub">Refreshing live data from Eventbrite &amp; Facebook Ads</p>
        <p class="status">Pulling fresh numbers<span class="dot">.</span><span class="dot">.</span><span class="dot">.</span></p>
        <p class="elapsed" id="timer"></p>
    </div>
    <script>
        var start = Date.now();
        var timerEl = document.getElementById('timer');
        setInterval(function() {
            var s = Math.floor((Date.now() - start) / 1000);
            timerEl.textContent = s + 's elapsed — usually takes about 90 seconds';
        }, 1000);
        (function poll() {
            fetch('/build_status').then(function(r) { return r.json(); }).then(function(d) {
                if (d.done) { window.location.href = '/'; }
                else { setTimeout(poll, 3000); }
            }).catch(function() { setTimeout(poll, 4000); });
        })();
    </script>
</body>
</html>"""

@app.route("/api/status")
def api_status():
    # Check for stuck builds on every status poll
    if _cache["building"]:
        build_start = _cache.get("build_start", 0)
        if build_start and (time.time() - build_start) > BUILD_TIMEOUT:
            _cache["building"] = False
            _cache["html"] = _build_error_html("Dashboard build timed out. Try /refresh in a minute.")
            _cache["time"] = time.time()
    ready = _cache["html"] is not None and (time.time() - _cache["time"]) < CACHE_TTL
    return jsonify({"ready": ready, "building": _cache["building"]})

@app.route("/api/fb_custom")
def api_fb_custom():
    """On-demand FB insights for custom date ranges. Avoids loading all daily data on startup."""
    since = request.args.get("since")
    until = request.args.get("until")
    if not since or not until:
        return jsonify({"error": "Missing since/until params"}), 400
    campaigns = fetch_fb_insights(since, until)
    fb_by_event = {}
    for c in campaigns:
        ev_num = extract_event_num_from_fb(c.get("campaign_name", ""))
        campaign_name = c.get("campaign_name", "")
        if ev_num is not None:
            key = str(ev_num)
        else:
            city = extract_city_from_fb(campaign_name)
            key = f"city:{city}" if city else None
        if key is None:
            continue
        spend = float(c.get("spend", 0))
        impressions = int(c.get("impressions", 0))
        reach = int(c.get("reach", 0))
        purchases = 0
        link_clicks = 0
        for a in c.get("actions", []):
            if a.get("action_type") == "omni_purchase":
                purchases = int(a.get("value", 0))
            if a.get("action_type") == "link_click":
                link_clicks = int(a.get("value", 0))
        if key in fb_by_event:
            fb_by_event[key]["spend"] += spend
            fb_by_event[key]["impressions"] += impressions
            fb_by_event[key]["reach"] += reach
            fb_by_event[key]["purchases"] += purchases
            fb_by_event[key]["link_clicks"] += link_clicks
        else:
            fb_by_event[key] = {"spend": spend, "impressions": impressions, "reach": reach, "purchases": purchases, "link_clicks": link_clicks}
    return jsonify(fb_by_event)

@app.route("/")
def dashboard():
    _ensure_cache()  # Trigger rebuild if stale (non-blocking)

    # Always serve cached HTML if we have it — even if stale
    if _cache["html"]:
        return Response(_cache["html"], content_type="text/html; charset=utf-8")

    # Only show loading page if we have NO cached data at all (first-ever load)
    return Response(LOADING_HTML, content_type="text/html; charset=utf-8")

@app.route("/refresh")
def refresh():
    """Force refresh the cache. Keeps serving old data while rebuilding."""
    _cache["time"] = 0  # Mark as stale so next _ensure_cache triggers rebuild
    # Only reset building if the build thread is actually dead (not just slow)
    build_thread = _cache.get("build_thread")
    if build_thread and not build_thread.is_alive():
        _cache["building"] = False
        try:
            _build_lock.release()
        except RuntimeError:
            pass
    _ensure_cache()
    # Return a page that shows "Refreshing..." and auto-reloads when the build is done
    return Response(REFRESH_WAIT_HTML, content_type="text/html; charset=utf-8")

@app.route("/build_status")
def build_status():
    """JSON endpoint for the refresh page to poll build progress."""
    build_start = _cache.get("build_start", 0)
    cache_time = _cache.get("time", 0)
    # Build is "done" only when cache was updated AFTER the most recent build started
    # This prevents false "done" from stale cache that existed before the refresh
    fresh = cache_time > build_start and len(_cache["html"] or "") > 2000
    return jsonify({
        "building": _cache["building"],
        "done": fresh,
        "elapsed": round(time.time() - build_start) if build_start else 0,
    })

@app.route("/cache_state")
def cache_state():
    """Debug endpoint to check raw cache state."""
    import threading
    disk_exists = os.path.exists(CACHE_FILE)
    disk_size = os.path.getsize(CACHE_FILE) if disk_exists else 0
    build_thread = _cache.get("build_thread")
    return jsonify({
        "version": APP_VERSION,
        "building": _cache["building"],
        "html_exists": _cache["html"] is not None,
        "html_len": len(_cache["html"]) if _cache["html"] else 0,
        "cache_time": _cache["time"],
        "build_start": _cache.get("build_start", 0),
        "age_seconds": time.time() - _cache["time"] if _cache["time"] else None,
        "build_elapsed": time.time() - _cache.get("build_start", 0) if _cache.get("build_start") else None,
        "build_thread_alive": build_thread.is_alive() if build_thread else None,
        "ttl": CACHE_TTL,
        "disk_cache_exists": disk_exists,
        "disk_cache_size": disk_size,
        "active_threads": [t.name for t in threading.enumerate()],
        "thread_count": threading.active_count(),
        "now": time.time()
    })

@app.route("/debug")
def debug():
    """Diagnostic endpoint to check API connectivity."""
    results = {"timestamp": datetime.now(ET).isoformat(), "checks": {}}

    # Check Eventbrite
    try:
        url = f"https://www.eventbriteapi.com/v3/organizations/{EB_ORG_ID}/events/"
        params = {"status": "live", "token": EB_TOKEN}
        res = requests.get(url, params=params, timeout=10)
        if res.status_code == 200:
            count = len(res.json().get("events", []))
            results["checks"]["eventbrite"] = {"status": "OK", "events_found": count}
        else:
            results["checks"]["eventbrite"] = {"status": "ERROR", "http_code": res.status_code, "response": res.text[:500]}
    except Exception as e:
        results["checks"]["eventbrite"] = {"status": "ERROR", "message": str(e)}

    # Check Facebook
    try:
        url = f"https://graph.facebook.com/v21.0/act_{FB_AD_ACCOUNT}/campaigns"
        params = {"fields": "name", "limit": 1, "access_token": FB_TOKEN}
        res = requests.get(url, params=params, timeout=10)
        if res.status_code == 200:
            results["checks"]["facebook"] = {"status": "OK", "sample": res.json().get("data", [])[:1]}
        else:
            err = res.json() if res.headers.get("content-type", "").startswith("application/json") else {"raw": res.text[:500]}
            results["checks"]["facebook"] = {"status": "ERROR", "http_code": res.status_code, "error": err}
    except Exception as e:
        results["checks"]["facebook"] = {"status": "ERROR", "message": str(e)}

    # Config check (mask tokens)
    results["config"] = {
        "EB_TOKEN": ("set (" + EB_TOKEN[:6] + "...)" ) if EB_TOKEN else "NOT SET",
        "EB_ORG_ID": EB_ORG_ID if EB_ORG_ID else "NOT SET",
        "FB_TOKEN": ("set (" + FB_TOKEN[:6] + "...)" ) if FB_TOKEN else "NOT SET",
        "FB_AD_ACCOUNT": FB_AD_ACCOUNT if FB_AD_ACCOUNT else "NOT SET",
    }

    results["recent_errors"] = _api_errors

    return Response(json.dumps(results, indent=2), content_type="application/json")

# ====== KEEP-ALIVE SELF-PING ======
# Prevents Render free tier from spinning down. Pings itself every 8 minutes.
def _keep_alive():
    """Ping our own /api/status endpoint to keep the server warm."""
    import urllib.request
    url = os.environ.get("RENDER_EXTERNAL_URL", "https://clarity-collective-dashboard.onrender.com")
    if not url:
        print("[KEEPALIVE] No RENDER_EXTERNAL_URL set, skipping keep-alive", flush=True)
        return
    ping_url = f"{url}/api/status"
    print(f"[KEEPALIVE] Starting keep-alive loop, pinging {ping_url} every 8 min", flush=True)
    while True:
        time.sleep(480)  # 8 minutes
        try:
            urllib.request.urlopen(ping_url, timeout=10)
        except Exception:
            pass  # Best effort

# On startup: load disk cache immediately so first request is instant
print(f"[STARTUP] Checking disk cache at {CACHE_FILE}...", flush=True)
print(f"[STARTUP] File exists: {os.path.exists(CACHE_FILE)}, size: {os.path.getsize(CACHE_FILE) if os.path.exists(CACHE_FILE) else 0}", flush=True)
_disk_html, _disk_time = _load_cache_from_disk()
if _disk_html:
    _cache["html"] = _disk_html
    _cache["time"] = _disk_time
    print(f"[STARTUP] Loaded cache from disk ({len(_disk_html)} bytes, {time.time()-_disk_time:.0f}s old)", flush=True)
else:
    print(f"[STARTUP] No valid disk cache found — first request will see loading page until build completes", flush=True)

# Load FB data cache on startup
_load_fb_cache()

# Start background data build on import (covers both gunicorn and direct run)
_ensure_cache()

# Start keep-alive thread
_ka = threading.Thread(target=_keep_alive, daemon=True)
_ka.start()

# Start proactive refresh loop — rebuilds cache before TTL expires
_pr = threading.Thread(target=_proactive_refresh_loop, daemon=True)
_pr.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
