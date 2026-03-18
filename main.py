from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pandas as pd
import requests
import math
import io
import json
import time
import os
from datetime import datetime, timezone
from typing import Optional

app = FastAPI(title="Ottrd API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

KEEPA_API_KEY = os.environ.get("KEEPA_API_KEY", "")
KEEPA_BASE    = "https://api.keepa.com/product"
KEEPA_EPOCH   = datetime(2011, 1, 1, tzinfo=timezone.utc)

# ── Keepa helpers ─────────────────────────────────────────────────────────────

def keepa_time_to_dt(kt):
    try:
        return datetime.fromtimestamp(KEEPA_EPOCH.timestamp() + kt * 60, tz=timezone.utc)
    except:
        return None

def kp(val):
    return round(val / 100, 2) if (val and val > 0) else None

def validate_price(p, max_price=5000):
    return p if (p and p <= max_price) else None

def get_all_prices(prod):
    stats = prod.get("stats") or {}
    def bb_from_avg(arr):
        if isinstance(arr, list) and len(arr) > 18:
            v = arr[18]
            return kp(v) if (v and v > 0) else None
        return None

    current = None
    bb = stats.get("buyBoxPrice")
    if isinstance(bb, list):
        v = next((x for x in bb if isinstance(x, (int, float)) and x >= 0), None)
        if v: current = kp(v)
    if not current:
        csv_data = prod.get("csv") or []
        for ci in (18, 1):
            if len(csv_data) > ci and isinstance(csv_data[ci], list):
                arr = csv_data[ci]
                for j in range(len(arr) - 1, 0, -2):
                    val = arr[j]
                    if isinstance(val, (int, float)) and val > 0:
                        current = kp(val)
                        break
            if current: break

    return {
        "current": validate_price(current),
        "avg30":   validate_price(bb_from_avg(stats.get("avg30"))),
        "avg90":   validate_price(bb_from_avg(stats.get("avg90"))),
        "avg180":  validate_price(bb_from_avg(stats.get("avg180"))),
        "avg365":  validate_price(bb_from_avg(stats.get("avg365"))),
    }

def parse_monthly_sales(prod):
    now = datetime.now(tz=timezone.utc)
    monthly = {}
    history = prod.get("monthlySoldHistory")
    if history and isinstance(history, list) and len(history) >= 2:
        for i in range(0, len(history) - 1, 2):
            kt, count = history[i], history[i + 1]
            if kt is None or count is None or count < 0: continue
            dt = keepa_time_to_dt(kt)
            if dt and (now - dt).days <= 395:
                monthly[dt.strftime("%Y-%m")] = int(count)
        if monthly: return monthly
    ms = prod.get("monthlySold")
    if ms and ms > 0:
        monthly[now.strftime("%Y-%m")] = int(ms)
    return monthly

def calc_fees(price, referral_pct=15.0, pick_and_pack=None):
    if not price: return None, None, None
    ref = round(price * (referral_pct / 100.0), 2)
    if pick_and_pack is not None:
        ful = pick_and_pack
    else:
        if price < 10:   ful = 2.47
        elif price < 15: ful = 3.22
        elif price < 20: ful = 4.75
        elif price < 40: ful = 5.85
        elif price < 75: ful = 7.17
        else:            ful = 9.73
    return ref, round(ful, 2), round(ref + ful, 2)

def clean_upc(val):
    s = str(val).strip()
    if 'e' in s.lower():
        try: s = str(int(float(s)))
        except: return [s]
    s = ''.join(c for c in s if c.isdigit())
    if not s: return [str(val)]
    if len(s) == 11: s = '0' + s
    variants = [s]
    if len(s) == 12: variants.append('0' + s)
    elif len(s) == 13 and s.startswith('0'): variants.append(s[1:])
    return variants

def clean_cost(val):
    try: return float(str(val).replace("$","").replace(",","").strip())
    except: return 0.0

def auto_col(df, candidates):
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        for key, real in cols_lower.items():
            if cand in key: return real
    return None

def last_12_months():
    now = datetime.now()
    months = []
    for i in range(11, -1, -1):
        m = now.month - i
        y = now.year
        while m <= 0: m += 12; y -= 1
        months.append(f"{y}-{m:02d}")
    return months

def fetch_keepa_batch(upcs, retries=3):
    params = {
        "key": KEEPA_API_KEY, "domain": 1,
        "code": ",".join(upcs), "stats": 365,
        "history": 1, "offers": 20, "buybox": 1, "rating": 0,
    }
    last_err = None
    for attempt in range(retries):
        try:
            resp = requests.get(KEEPA_BASE, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                raise Exception(data["error"].get("message", str(data["error"])))
            result = {}
            for prod in (data.get("products") or []):
                if not prod: continue
                all_codes = set()
                for c in (prod.get("upcList") or []): all_codes.add(str(c).strip())
                for c in (prod.get("eanList") or []): all_codes.add(str(c).strip())
                codes = prod.get("code") or []
                if isinstance(codes, str): codes = [codes]
                for c in codes: all_codes.add(str(c).strip())
                for c in all_codes:
                    if c not in result: result[c] = []
                    result[c].append(prod)
            return result
        except requests.exceptions.Timeout:
            last_err = f"Timeout attempt {attempt+1}"
            time.sleep(2 * (attempt + 1))
        except Exception as e:
            raise e
    raise Exception(f"Keepa timed out: {last_err}")

def process_item(item, prod, settings):
    overhead    = settings["overhead"] / 100.0
    min_roi     = settings["min_roi"]
    min_profit  = settings["min_profit"]
    price_basis = settings["price_basis"]
    pb_map      = settings["pb_map"]
    active_months = settings["active_months"]
    order_basis = settings["order_basis"]
    order_pct   = settings["order_pct"] / 100.0

    monthly     = parse_monthly_sales(prod)
    ever_hit    = any(v >= settings["threshold"] for v in monthly.values())
    filtered    = {k: v for k, v in monthly.items() if int(k.split("-")[1]) in active_months}
    peak        = max(filtered.values()) if filtered else 0
    peak_all    = max(monthly.values()) if monthly else 0
    avg_filtered = round(sum(filtered.values()) / len(filtered), 1) if filtered else 0

    all_prices  = get_all_prices(prod)
    asin        = prod.get("asin", "")
    title       = (prod.get("title") or item["name"] or item["upc"])[:70]

    referral_pct = float(prod.get("referralFeePercent") or prod.get("referralFeePercentage") or 15.0)
    pick_and_pack = None
    fba_data = prod.get("fbaFees")
    if fba_data and isinstance(fba_data, dict):
        pp = fba_data.get("pickAndPackFee")
        if pp and pp > 0: pick_and_pack = round(pp / 100, 2)

    if price_basis == "min_selected":
        candidates = [v for k, v in all_prices.items() if pb_map.get(k) and v and v > 0]
        amz_price  = round(min(candidates), 2) if candidates else None
    else:
        amz_price = all_prices.get(price_basis)

    ref_fee, pp_fee, fee = calc_fees(amz_price, referral_pct, pick_and_pack)
    fee_source  = "Keepa" if pick_and_pack is not None else "Est."
    true_cost   = round(item["cost"] * (1 + overhead), 2)
    net_sale    = round(amz_price - fee, 2) if (amz_price and fee) else None
    net         = round(net_sale - true_cost, 2) if net_sale is not None else None
    roi         = round((net / true_cost) * 100, 1) if (net is not None and true_cost > 0) else None

    if net_sale is not None:
        target_true_cost = round(net_sale / (1 + min_roi / 100), 2)
        target_supplier  = round(target_true_cost / (1 + overhead), 2)
        price_gap        = round(item["cost"] - target_supplier, 2)
    else:
        target_supplier = price_gap = None

    if ever_hit and roi is not None and roi >= min_roi:
        decision = "Buy"
    elif ever_hit and roi is not None and roi >= (min_roi * 0.5):
        decision = "Review"
    elif ever_hit:
        decision = "Review"
    else:
        decision = "Pass"

    low_profit = decision in ("Buy","Review") and net is not None and net < min_profit

    if order_basis == "avg" and avg_filtered > 0:
        suggested_qty = max(math.ceil(avg_filtered * order_pct), 1)
        qty_basis_str = f"avg({avg_filtered:.0f})×{int(order_pct*100)}%"
    elif peak > 0:
        suggested_qty = max(math.ceil(peak * 1.5), 6)
        qty_basis_str = f"peak({peak})"
    else:
        suggested_qty = 0
        qty_basis_str = "—"

    pct_off = None
    if target_supplier is not None and item["cost"] > 0:
        if price_gap is not None and price_gap <= 0:
            pct_off = 0.0
        else:
            pct_off = round(((item["cost"] - target_supplier) / item["cost"]) * 100, 1)

    return {
        "sku": item["sku"], "upc": item["upc"], "asin": asin, "title": title,
        "cost": item["cost"], "true_cost": true_cost,
        "price_current": all_prices.get("current"),
        "price_avg30": all_prices.get("avg30"),
        "price_avg90": all_prices.get("avg90"),
        "price_avg180": all_prices.get("avg180"),
        "price_avg365": all_prices.get("avg365"),
        "amz_price": amz_price, "referral_pct": referral_pct,
        "referral_fee": ref_fee, "pp_fee": pp_fee,
        "fba_fee": fee, "fee_source": fee_source,
        "net_sale": net_sale, "net_profit": net, "roi": roi,
        "monthly": monthly, "ever_hit": ever_hit,
        "peak_all": peak_all, "peak_filtered": peak,
        "avg_filtered": avg_filtered,
        "target_supplier": target_supplier, "price_gap": price_gap,
        "pct_off": pct_off, "decision": decision,
        "low_profit": low_profit,
        "suggested_qty": suggested_qty, "qty_basis_str": qty_basis_str,
        "found": True, "error": ""
    }

# ── API Routes ────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "Ottrd API"}

@app.post("/analyze")
async def analyze(
    file: UploadFile = File(...),
    settings: str = Form(...),
):
    if not KEEPA_API_KEY:
        raise HTTPException(500, "Keepa API key not configured")

    s = json.loads(settings)

    # Parse file
    content = await file.read()
    ext = file.filename.split(".")[-1].lower()
    try:
        if ext == "csv":
            df = pd.read_csv(io.BytesIO(content), dtype=str)
        elif ext in ("xlsx", "xls"):
            df = pd.read_excel(io.BytesIO(content), dtype=str)
        else:
            raise HTTPException(400, "Unsupported file type. Use CSV or Excel.")
    except Exception as e:
        raise HTTPException(400, f"Could not read file: {e}")

    upc_col  = auto_col(df, ["upc","barcode","ean","gtin","code"])
    cost_col = auto_col(df, ["cost","price","unit cost","wholesale","buy"])
    name_col = auto_col(df, ["name","title","product","description","item"])
    sku_col  = auto_col(df, ["sku","item #","part","model","item no"])

    if not upc_col:  raise HTTPException(400, "No UPC column found in file")
    if not cost_col: raise HTTPException(400, "No cost column found in file")

    items = []
    for _, row in df.iterrows():
        raw_upc  = str(row[upc_col]).strip()
        variants = clean_upc(raw_upc)
        cost     = clean_cost(row[cost_col])
        if not variants or cost <= 0: continue
        items.append({
            "upc": variants[0], "variants": variants, "cost": cost,
            "name": str(row[name_col]).strip() if name_col else "",
            "sku":  str(row[sku_col]).strip()  if sku_col  else "",
        })

    if not items:
        raise HTTPException(400, "No valid UPC + cost rows found")

    # Process in batches
    BATCH = 50
    results = []
    for i in range(0, len(items), BATCH):
        batch = items[i:i+BATCH]
        all_codes = list(dict.fromkeys(
            code for item in batch for code in item.get("variants", [item["upc"]])
        ))
        try:
            keepa_data = fetch_keepa_batch(all_codes)
        except Exception as e:
            for item in batch:
                results.append({"upc": item["upc"], "found": False, "error": str(e), "decision": "Pass"})
            continue

        seen = set()
        for item in batch:
            prods = []
            for v in item.get("variants", [item["upc"]]):
                for p in (keepa_data.get(v) or []):
                    ak = p.get("asin","")
                    if ak and ak not in seen:
                        seen.add(ak)
                        prods.append(p)
            if not prods:
                results.append({"upc": item["upc"], "found": False, "error": "Not found", "decision": "Pass"})
                continue
            for prod in prods:
                results.append(process_item(item, prod, s))

        if i + BATCH < len(items):
            time.sleep(1)

    return {"results": results, "total": len(results)}

@app.get("/tokens")
def check_tokens():
    if not KEEPA_API_KEY:
        raise HTTPException(500, "No API key")
    resp = requests.get(f"https://api.keepa.com/token?key={KEEPA_API_KEY}", timeout=10)
    data = resp.json()
    return {"tokens_left": data.get("tokensLeft"), "refill_rate": data.get("refillRate")}
