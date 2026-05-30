#!/usr/bin/env python3
"""
The Clarity Collective Dashboard — live-API edition.

Serves a static HTML shell at / and exposes JSON endpoints that the browser
fetches on demand. No precomputed HTML. No background build threads. No disk
cache. If an upstream API hiccups, the page still renders; only the affected
section shows an error.

Hardening (matches the client-dashboard-builder skill's Layer 3 defenses):
  - In-memory cache per endpoint, 60s TTL
  - Per-call timeout + retry on 429/5xx (Eventbrite has strict rate limits)
  - Deep /api/health that actually probes both upstreams and classifies errors
  - Startup token validation (loud ████ banner in logs if a key is dead)
  - Never-crash error handlers
  - Structured JSON logging
"""
import os
import re
import json
import time
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Flask, Response, jsonify, request, send_from_directory

# ─── Config ──────────────────────────────────────────────────────────────────
APP_VERSION = "v3.0-live-api"
ET = ZoneInfo("America/New_York")

EB_TOKEN = os.environ.get("EB_TOKEN", "")
EB_ORG_ID = os.environ.get("EB_ORG_ID", "")
FB_TOKEN = os.environ.get("FB_TOKEN", "")
FB_AD_ACCOUNT = os.environ.get("FB_AD_ACCOUNT", "")

CACHE_TTL = int(os.environ.get("CACHE_TTL_SECONDS", "60"))
STALE_THRESHOLD = int(os.environ.get("STALE_THRESHOLD_SECONDS", "300"))
DATA_CACHE_TTL = int(os.environ.get("DATA_CACHE_SECONDS", "300"))  # /api/data is expensive; cache 5m

BRAND_NAME = "The Clarity Collective"
FB_CAMPAIGN_FILTER = "Clarity Collective"

app = Flask(__name__, static_folder="static", static_url_path="/static")

# ─── Structured logging ──────────────────────────────────────────────────────
def log(level, msg, **extra):
    try:
        print(json.dumps({"ts": datetime.utcnow().isoformat() + "Z", "level": level, "msg": msg, **extra}), flush=True)
    except Exception:
        print(f"[{level}] {msg}", flush=True)

# ─── In-memory cache ─────────────────────────────────────────────────────────
_cache: dict = {}

def cache_get(key):
    entry = _cache.get(key)
    if not entry:
        return None
    if time.time() - entry["ts"] > entry["ttl"]:
        _cache.pop(key, None)
        return None
    return entry["value"]

def cache_set(key, value, ttl=CACHE_TTL):
    _cache[key] = {"value": value, "ts": time.time(), "ttl": ttl}

# ─── Health state (last success / error per upstream) ────────────────────────
state = {
    "eb": {"last_success_at": None, "last_error": None, "org_id": EB_ORG_ID},
    "fb": {"last_success_at": None, "last_error": None, "account": FB_AD_ACCOUNT},
}

def classify_error(msg: str) -> str:
    m = (msg or "").lower()
    if "expired" in m or "invalid oauth" in m or "code 190" in m: return "TOKEN_EXPIRED"
    if "429" in m or "rate limit" in m or "throttle" in m: return "RATE_LIMITED"
    if "timeout" in m or "timed out" in m: return "TIMEOUT"
    if " 403" in m or "forbidden" in m: return "FORBIDDEN"
    if " 401" in m or "unauthorized" in m: return "UNAUTHORIZED"
    return "GENERIC"

def is_stale(ts: float) -> bool:
    return bool(ts) and (time.time() - ts) > STALE_THRESHOLD

# ─── Upstream HTTP helpers ───────────────────────────────────────────────────
def _eb_request(url, params, timeout=(10, 30), max_retries=5):
    """Eventbrite request with retry on 429 (their rate limits are strict)."""
    last_err = None
    for attempt in range(max_retries):
        try:
            res = requests.get(url, params=params, timeout=timeout)
            if res.status_code == 429:
                wait = min(2 ** attempt * 2, 30)
                log("warn", "eb_429", attempt=attempt + 1, wait_s=wait)
                time.sleep(wait)
                continue
            res.raise_for_status()
            return res
        except Exception as e:
            last_err = e
            if attempt < max_retries - 1:
                time.sleep(min(2 ** attempt, 10))
                continue
    raise last_err if last_err else Exception("eb_request exhausted retries")

def _fb_get(url, params=None, timeout=(10, 30), retries=2):
    """Facebook Graph request with timeout + retry on 5xx/network."""
    last_err = None
    for attempt in range(retries + 1):
        try:
            res = requests.get(url, params=params, timeout=timeout)
            if res.status_code >= 500 and attempt < retries:
                time.sleep(0.5 * (2 ** attempt))
                continue
            return res
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(0.5 * (2 ** attempt))
                continue
    raise last_err if last_err else Exception("fb_get exhausted retries")

# ─── Eventbrite fetches ──────────────────────────────────────────────────────
def fetch_eb_events():
    all_events = []
    base = f"https://www.eventbriteapi.com/v3/organizations/{EB_ORG_ID}/events/"

    # Live
    res = _eb_request(base, {"status": "live", "expand": "venue,ticket_classes", "token": EB_TOKEN})
    live = res.json().get("events", [])
    for e in live:
        e["_eb_status"] = "live"
    all_events.extend(live)
    log("info", "eb_live_events", count=len(live))

    # Ended + Completed (recent, capped)
    for status in ["ended", "completed"]:
        page = 1
        page_count = 0
        while True:
            res = _eb_request(base, {
                "status": status, "expand": "venue,ticket_classes",
                "order_by": "start_desc", "token": EB_TOKEN, "page": page,
            })
            data = res.json()
            events = data.get("events", [])
            if not events:
                break
            for e in events:
                e["_eb_status"] = status
            all_events.extend(events)
            page += 1
            page_count += 1
            if not data.get("pagination", {}).get("has_more_items"):
                break
            if len(all_events) >= 60 or page_count >= 2:
                break
            time.sleep(0.2)

    log("info", "eb_total_events", count=len(all_events))
    return all_events

def fetch_eb_orders(event_id):
    all_orders = []
    page = 1
    while True:
        res = _eb_request(f"https://www.eventbriteapi.com/v3/events/{event_id}/orders/",
                          {"token": EB_TOKEN, "page": page})
        data = res.json()
        all_orders.extend([o for o in data.get("orders", []) if o.get("status") in ("placed", "completed")])
        if not data.get("pagination", {}).get("has_more_items"):
            break
        page += 1
        if page > 30: break
    return all_orders

def fetch_eb_attendees(event_id):
    all_attendees = []
    page = 1
    while True:
        res = _eb_request(f"https://www.eventbriteapi.com/v3/events/{event_id}/attendees/",
                          {"token": EB_TOKEN, "page": page, "status": "attending"})
        data = res.json()
        all_attendees.extend(data.get("attendees", []))
        if not data.get("pagination", {}).get("has_more_items"):
            break
        page += 1
        if page > 30: break
    return all_attendees

# ─── Facebook fetches ────────────────────────────────────────────────────────
def fetch_fb_insights(since_date, until_date):
    url = f"https://graph.facebook.com/v21.0/act_{FB_AD_ACCOUNT}/insights"
    params = {
        "fields": "campaign_name,campaign_id,spend,impressions,reach,actions",
        "level": "campaign",
        "filtering": json.dumps([{"field": "campaign.name", "operator": "CONTAIN", "value": FB_CAMPAIGN_FILTER}]),
        "time_range": json.dumps({"since": since_date, "until": until_date}),
        "limit": 200,
        "access_token": FB_TOKEN,
    }
    all_results = []
    res = _fb_get(url, params)
    if res.status_code != 200:
        body = res.json() if res.headers.get("content-type","").startswith("application/json") else {}
        err_msg = body.get("error", {}).get("message", f"HTTP {res.status_code}")
        err_code = body.get("error", {}).get("code", "")
        if err_code == 190:
            raise Exception(f"FB token expired: {err_msg}")
        raise Exception(f"FB insights {since_date}→{until_date}: {err_msg}")
    data = res.json()
    all_results.extend(data.get("data", []))
    paging = data.get("paging", {})
    while paging.get("next"):
        r2 = _fb_get(paging["next"])
        if r2.status_code != 200: break
        d2 = r2.json()
        all_results.extend(d2.get("data", []))
        paging = d2.get("paging", {})
    return all_results

def fetch_fb_campaigns_meta():
    """Returns (meta_by_num, meta_by_city). Used to match FB campaigns to EB events."""
    url = f"https://graph.facebook.com/v21.0/act_{FB_AD_ACCOUNT}/campaigns"
    params = {"fields": "name,status", "limit": 100, "access_token": FB_TOKEN}

    meta_by_num = {}
    meta_by_city = {}
    page_count = 0
    while True:
        res = _fb_get(url, params=params, timeout=(5, 15))
        if res.status_code != 200:
            body = res.json() if res.headers.get("content-type","").startswith("application/json") else {}
            err_msg = body.get("error", {}).get("message", f"HTTP {res.status_code}")
            raise Exception(f"FB campaigns: {err_msg}")
        data = res.json()
        for c in data.get("data", []):
            name = c.get("name", "")
            if FB_CAMPAIGN_FILTER.lower() not in name.lower():
                continue
            status = c.get("status", "")
            year_month = extract_year_month_from_fb(name)
            ev_num_m = re.search(r"Event\s+(\d+)", name, re.I)
            ev_num = int(ev_num_m.group(1)) if ev_num_m else None
            bracket_m = re.search(r"\[([^\]]+)\]", name)
            typ_clean = "2-Day" if (bracket_m and "2-Day" in bracket_m.group(1)) or "2-day" in name.lower() or "2 day" in name.lower() else "1-Day"
            city = extract_city_from_campaign_name(name)
            if ev_num:
                entry = {"city": city, "type": typ_clean, "fb_status": status, "year_month": year_month}
                if ev_num not in meta_by_num or status == "ACTIVE" or meta_by_num[ev_num].get("fb_status") != "ACTIVE":
                    meta_by_num[ev_num] = entry
            elif city:
                entry = {"fb_status": status, "year_month": year_month}
                city_key = f"{city}:{year_month[0]}-{year_month[1]:02d}" if year_month else city
                if city_key not in meta_by_city or status == "ACTIVE" or meta_by_city[city_key].get("fb_status") != "ACTIVE":
                    meta_by_city[city_key] = entry
        paging = data.get("paging", {})
        if not paging.get("next"):
            break
        params["after"] = paging.get("cursors", {}).get("after", "")
        page_count += 1
        if page_count >= 5: break
    log("info", "fb_meta_done", by_num=len(meta_by_num), by_city=len(meta_by_city))
    return meta_by_num, meta_by_city

# ─── Matching helpers ────────────────────────────────────────────────────────
CITIES = ["Woodstock","Atlanta","Nashville","Charlotte","Dallas","Houston","Austin","Denver","Phoenix","Chicago","Miami",
          "Tampa","Orlando","Boston","New York","Los Angeles","San Francisco","San Diego","Seattle","Portland",
          "Colorado Springs","Oklahoma City","Salt Lake City","Salt Lake","San Antonio","Indianapolis","Las Vegas",
          "West Palm","Scottsdale","Charleston","Carlsbad","Bozeman","Frisco","Naples","Boise","NYC","OKC",
          "D.C","ATL","DC","LA","Vancouver","Alpharetta","Milwaukee","Sarasota"]
CITY_ALIASES = {"Salt Lake":"Salt Lake City","OKC":"Oklahoma City","D.C":"DC","LA":"Los Angeles","ATL":"Atlanta"}

def extract_city_from_campaign_name(name):
    """Word-boundary match for short city codes to avoid 'LA' inside 'cLArity'."""
    for ct in CITIES:
        if len(ct) <= 3:
            if re.search(r'\b' + re.escape(ct) + r'\b', name, re.I):
                return CITY_ALIASES.get(ct, ct)
        elif ct.lower() in name.lower():
            return CITY_ALIASES.get(ct, ct)
    return None

def extract_event_num_from_fb(campaign_name):
    m = re.search(r"Event\s+(\d+)", campaign_name, re.I)
    return int(m.group(1)) if m else None

def extract_year_month_from_fb(campaign_name):
    months = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12,"june":6,"july":7}
    m = re.search(r"(\d{4})\s+(JAN|FEB|MAR|APR|MAY|JUN|JUNE|JUL|JULY|AUG|SEP|OCT|NOV|DEC)", campaign_name, re.I)
    if m:
        return (int(m.group(1)), months.get(m.group(2).lower(), 1))
    return None

def extract_city_from_event(event):
    venue = event.get("venue", {})
    if isinstance(venue, dict):
        addr = venue.get("address", {})
        if addr and addr.get("city"):
            return addr["city"]
    name = event.get("name", {}).get("text", "") if isinstance(event, dict) else str(event)
    for c in CITIES:
        if c.lower() in name.lower():
            return CITY_ALIASES.get(c, c)
    name = name.split(":")[0].strip() if ":" in name else name
    return name[:30] if len(name) > 30 else name

# ─── Aggregate FB rows into a {key: aggregates} map keyed by event_num / city ─
def aggregate_fb_by_event(campaigns):
    by_event = {}
    for c in campaigns:
        name = c.get("campaign_name", "")
        ev_num = extract_event_num_from_fb(name)
        if ev_num is not None:
            key = str(ev_num)
        else:
            city = extract_city_from_campaign_name(name)
            ym = extract_year_month_from_fb(name)
            if city and ym:
                key = f"city:{city}:{ym[0]}-{ym[1]:02d}"
            elif city:
                key = f"city:{city}"
            else:
                continue
        spend = float(c.get("spend", 0) or 0)
        impressions = int(c.get("impressions", 0) or 0)
        reach = int(c.get("reach", 0) or 0)
        purchases = 0
        link_clicks = 0
        for a in c.get("actions", []) or []:
            t = a.get("action_type")
            if t == "omni_purchase":
                purchases = int(float(a.get("value", 0) or 0))
            elif t == "link_click":
                link_clicks = int(float(a.get("value", 0) or 0))
        if key in by_event:
            agg = by_event[key]
            agg["spend"] += spend
            agg["impressions"] += impressions
            agg["reach"] += reach
            agg["purchases"] += purchases
            agg["link_clicks"] += link_clicks
        else:
            by_event[key] = {"spend": spend, "impressions": impressions, "reach": reach,
                             "purchases": purchases, "link_clicks": link_clicks}
    return by_event

# ─── Top-level data assembly (the one expensive call) ────────────────────────
def compute_dashboard_data():
    """Fetch + assemble everything the frontend needs. Cached at the caller."""
    t0 = time.time()

    # 1. Fetch EB events + FB campaign metadata in parallel
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_events = pool.submit(fetch_eb_events)
        f_fb_meta = pool.submit(fetch_fb_campaigns_meta)
        events_raw = f_events.result()
        try:
            meta_by_num, meta_by_city = f_fb_meta.result()
        except Exception as e:
            log("warn", "fb_meta_failed_continuing", error=str(e))
            meta_by_num, meta_by_city = {}, {}

    # 2. Fetch FB insights for all 6 periods in parallel
    today = datetime.now(ET).date()
    fb_ranges = {
        "today": (today.isoformat(), today.isoformat()),
        "yesterday": ((today - _days(1)).isoformat(), (today - _days(1)).isoformat()),
        "last2": ((today - _days(2)).isoformat(), today.isoformat()),
        "last7": ((today - _days(7)).isoformat(), (today - _days(1)).isoformat()),
        "last30": ((today - _days(30)).isoformat(), (today - _days(1)).isoformat()),
        "all": ("2024-01-01", today.isoformat()),
    }
    fb_data = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futs = {pool.submit(fetch_fb_insights, s, u): name for name, (s, u) in fb_ranges.items()}
        for fut, name in futs.items():
            try:
                rows = fut.result(timeout=45)
                fb_data[name] = aggregate_fb_by_event(rows)
            except Exception as e:
                log("warn", "fb_period_failed", period=name, error=str(e))
                fb_data[name] = {}
    state["fb"]["last_success_at"] = time.time()
    state["fb"]["last_error"] = None

    # 3. Classify events as active vs past
    active = []
    past = []
    active_to_enrich = []  # (idx_in_active, event_id, city)
    for event in events_raw:
        eid = event["id"]
        name = event["name"]["text"]
        city = extract_city_from_event(event)
        start_date = event["start"]["local"]
        end_date = event["end"]["local"]
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)
        duration_days = (end_dt.date() - start_dt.date()).days + 1
        capacity = event.get("capacity", 0) or 0
        total_sold = sum(tc.get("quantity_sold", 0) for tc in event.get("ticket_classes", []))
        eb_status = event.get("_eb_status", "live")

        # Match to FB by event num, with date proximity
        eb_year = start_dt.year
        eb_month = start_dt.month
        best_num = 0
        best_score = 999
        best_fb_status = "UNKNOWN"
        for num, info in meta_by_num.items():
            info_city = info.get("city", "")
            if info_city and city and info_city.lower() != city.lower():
                continue
            ym = info.get("year_month")
            dist = abs((eb_year * 12 + eb_month) - (ym[0] * 12 + ym[1])) if ym else 50
            if dist > 2:
                continue
            if ym and info.get("fb_status") != "ACTIVE":
                fb_months = ym[0] * 12 + ym[1]
                eb_months = eb_year * 12 + eb_month
                if fb_months < eb_months: continue
            if dist < best_score or (dist == best_score and info.get("fb_status") == "ACTIVE"):
                best_score = dist
                best_num = num
                best_fb_status = info.get("fb_status", "UNKNOWN")
        if best_num == 0:
            city_key_exact = f"{city}:{eb_year}-{eb_month:02d}"
            for ck in [city_key_exact]:
                if ck in meta_by_city:
                    best_fb_status = meta_by_city[ck].get("fb_status", "UNKNOWN")
                    break

        event_num = best_num
        fb_status = best_fb_status
        duration_label = f"{duration_days}-Day"
        display_city = f"{event_num} {city} ({duration_label})" if event_num else city
        is_future = start_dt.tzinfo and start_dt > datetime.now(start_dt.tzinfo) or False
        has_activity = total_sold > 0 or fb_status == "ACTIVE"
        is_past = not is_future or (is_future and not has_activity)

        ev_payload = {
            "city": city, "display_city": display_city, "event_num": event_num,
            "event_id": eid, "name": name, "start_date": start_date,
            "capacity": capacity, "total_sold": total_sold,
            "fill_pct": round(total_sold / capacity * 100) if capacity > 0 else 0,
            "tickets": [], "orders": [],
            "eb_status": eb_status, "fb_status": fb_status, "is_past": is_past,
            "duration_days": duration_days,
        }
        if is_past:
            past.append(ev_payload)
        else:
            active.append(ev_payload)
            active_to_enrich.append((len(active) - 1, eid, city))

    # 4. Enrich active events with orders + attendees (sequentially — EB rate limits)
    all_tickets_flat = []
    for idx, eid, city in active_to_enrich:
        try:
            orders = fetch_eb_orders(eid)
            attendees = fetch_eb_attendees(eid)
            simplified_orders = [
                {"id": o.get("id"), "name": o.get("name", ""),
                 "amount": float(o.get("costs", {}).get("gross", {}).get("major_value", 0) or 0),
                 "created": o.get("created"), "event_id": eid}
                for o in orders
            ]
            simplified_tickets = [
                {"id": a.get("id"), "order_id": a.get("order_id"), "name": (a.get("profile", {}) or {}).get("name", ""),
                 "created": a.get("created"), "event_id": eid, "city": city}
                for a in attendees
            ]
            active[idx]["orders"] = simplified_orders
            active[idx]["tickets"] = simplified_tickets
            all_tickets_flat.extend(simplified_tickets)
        except Exception as e:
            log("warn", "active_enrich_failed", city=city, eid=eid, error=str(e))

    state["eb"]["last_success_at"] = time.time()
    state["eb"]["last_error"] = None

    log("info", "compute_done", elapsed_s=round(time.time() - t0, 1),
        active=len(active), past=len(past), tickets=len(all_tickets_flat))

    return {
        "eventData": {"active": active, "past": past},
        "allTickets": all_tickets_flat,
        "fbData": fb_data,
        "generatedAt": datetime.now(ET).isoformat(),
        "version": APP_VERSION,
    }

def _days(n):
    from datetime import timedelta
    return timedelta(days=n)

# ─── Routes ──────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/api/config")
def api_config():
    return jsonify({
        "brandName": BRAND_NAME,
        "version": APP_VERSION,
        "cacheTtlSeconds": DATA_CACHE_TTL,
        "staleThresholdSeconds": STALE_THRESHOLD,
    })

@app.route("/api/health")
def api_health():
    out = {
        "eb": {
            "status": "unknown", "error": None, "errorClass": None,
            "lastSuccessAt": state["eb"]["last_success_at"],
            "stale": is_stale(state["eb"]["last_success_at"] or 0),
        },
        "fb": {
            "status": "unknown", "error": None, "errorClass": None,
            "lastSuccessAt": state["fb"]["last_success_at"],
            "stale": is_stale(state["fb"]["last_success_at"] or 0),
        },
    }

    # EB live probe
    try:
        if not EB_TOKEN or not EB_ORG_ID:
            raise Exception("EB env vars missing")
        res = _eb_request(
            f"https://www.eventbriteapi.com/v3/organizations/{EB_ORG_ID}/events/",
            {"status": "live", "token": EB_TOKEN, "page_size": 1}, timeout=(5, 10), max_retries=1
        )
        res.raise_for_status()
        out["eb"]["status"] = "ok"
    except Exception as e:
        out["eb"]["status"] = "error"
        out["eb"]["error"] = str(e)[:300]
        out["eb"]["errorClass"] = classify_error(str(e))

    # FB live probe
    try:
        if not FB_TOKEN or not FB_AD_ACCOUNT:
            raise Exception("FB env vars missing")
        res = _fb_get(
            f"https://graph.facebook.com/v21.0/act_{FB_AD_ACCOUNT}",
            {"fields": "name", "access_token": FB_TOKEN}, timeout=(5, 10), retries=1,
        )
        body = res.json() if res.headers.get("content-type","").startswith("application/json") else {}
        if res.status_code != 200 or body.get("error"):
            raise Exception(body.get("error", {}).get("message", f"HTTP {res.status_code}"))
        out["fb"]["status"] = "ok"
    except Exception as e:
        out["fb"]["status"] = "error"
        out["fb"]["error"] = str(e)[:300]
        out["fb"]["errorClass"] = classify_error(str(e))

    resp = jsonify(out)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    return resp

@app.route("/api/data")
def api_data():
    """The one expensive endpoint. Cached for DATA_CACHE_TTL (default 5 min).
    Bypass cache with ?force=1."""
    force = request.args.get("force") == "1"
    cache_key = "data"
    if not force:
        cached = cache_get(cache_key)
        if cached:
            return jsonify({**cached, "cached": True})
    try:
        payload = compute_dashboard_data()
        cache_set(cache_key, payload, ttl=DATA_CACHE_TTL)
        return jsonify({**payload, "cached": False})
    except Exception as e:
        log("error", "api_data_failed", error=str(e), trace=traceback.format_exc()[-500:])
        state["eb"]["last_error"] = {"ts": time.time(), "message": str(e)[:300]}
        # Serve stale-cached data if available (Layer 3 partial-success philosophy)
        stale = _cache.get(cache_key)
        if stale:
            return jsonify({**stale["value"], "cached": True, "stale": True,
                            "error": f"Live fetch failed, serving stale data: {str(e)[:200]}"})
        return jsonify({"error": str(e)[:300], "errorClass": classify_error(str(e))}), 500

# ─── Startup validation ──────────────────────────────────────────────────────
def validate_on_startup():
    log("info", "startup_validation_begin")

    if EB_TOKEN and EB_ORG_ID:
        try:
            res = requests.get(
                f"https://www.eventbriteapi.com/v3/organizations/{EB_ORG_ID}/events/",
                params={"status": "live", "token": EB_TOKEN, "page_size": 1}, timeout=10,
            )
            if res.status_code == 200:
                log("info", "eb_ok_on_startup")
            else:
                log("error", "eb_invalid_on_startup", status=res.status_code, body=res.text[:200])
                if res.status_code in (401, 403):
                    print("\n████████████████████████████████████████████████████")
                    print("██  EB_TOKEN UNAUTHORIZED — fix in Render env vars ██")
                    print("████████████████████████████████████████████████████\n", flush=True)
        except Exception as e:
            log("warn", "eb_startup_network_error", error=str(e))
    else:
        log("warn", "eb_not_configured")

    if FB_TOKEN and FB_AD_ACCOUNT:
        try:
            res = requests.get(
                f"https://graph.facebook.com/v21.0/act_{FB_AD_ACCOUNT}",
                params={"fields": "name", "access_token": FB_TOKEN}, timeout=10,
            )
            body = res.json() if res.status_code != 500 else {}
            if res.status_code == 200 and not body.get("error"):
                log("info", "fb_ok_on_startup", account=body.get("name"))
            else:
                err = body.get("error", {})
                log("error", "fb_invalid_on_startup", status=res.status_code, error=err)
                if err.get("code") == 190:
                    print("\n████████████████████████████████████████████████████")
                    print("██  FB_TOKEN EXPIRED — rotate System User token   ██")
                    print("████████████████████████████████████████████████████\n", flush=True)
        except Exception as e:
            log("warn", "fb_startup_network_error", error=str(e))
    else:
        log("warn", "fb_not_configured")

    log("info", "startup_validation_complete")

# Run startup validation in a background thread so it doesn't block worker boot
import threading
threading.Thread(target=validate_on_startup, daemon=True).start()

# ─── Never-crash handlers ────────────────────────────────────────────────────
@app.errorhandler(Exception)
def handle_unexpected(e):
    log("error", "unhandled_exception", error=str(e), trace=traceback.format_exc()[-500:])
    return jsonify({"error": "internal error", "detail": str(e)[:200]}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
