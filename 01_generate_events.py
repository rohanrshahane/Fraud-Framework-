# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate card-transaction events
# MAGIC
# MAGIC Simulates merchants sending card authorization events. Each simulated **day** lands as one JSON-lines
# MAGIC file in the `landing.events` volume, the way a payment gateway drops files or a Kafka sink writes them.
# MAGIC
# MAGIC Three feeds are written:
# MAGIC * `transactions/` – one event per card authorization (what the framework scores)
# MAGIC * `labels/` – the **chargeback feed**: transactions later confirmed as fraud (used only to train and grade)
# MAGIC * `reference/` – customers, cards and merchants (master data)
# MAGIC
# MAGIC All data is synthetic and seeded, so every run produces the same events. Five fraud patterns are injected:
# MAGIC card testing, account takeover, impossible travel, high-risk merchant cash-outs and subtle fraud,
# MAGIC alongside genuine look-alikes (trips, big purchases, new phones, VPN users, evening splurges).
# MAGIC
# MAGIC **Widgets:** `emit_from_day` / `emit_to_day` choose which days to drop (1 = Oct 1 … 32 = Nov 1).
# MAGIC Run once with 1–30 for the main demo; the student exercise later streams days 31–32.
# MAGIC `seconds_between_days` adds a pause between files so you can watch them arrive.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import json, math, time
from datetime import datetime, timedelta
import numpy as np

EMIT_FROM = int(get_param("emit_from_day", 1, "Emit from day (1-32)"))
EMIT_TO = int(get_param("emit_to_day", 30, "Emit to day (1-32)"))
PAUSE = float(get_param("seconds_between_days", 0, "Seconds between day files"))

N_CUSTOMERS, DAYS, SEED = 2000, 32, 42
START = datetime(2026, 10, 1)

CITIES = {
    "Newark, NJ": (40.7357, -74.1724), "New York, NY": (40.7128, -74.0060),
    "Philadelphia, PA": (39.9526, -75.1652), "Boston, MA": (42.3601, -71.0589),
    "Washington, DC": (38.9072, -77.0369), "Chicago, IL": (41.8781, -87.6298),
    "Atlanta, GA": (33.7490, -84.3880), "Miami, FL": (25.7617, -80.1918),
    "Dallas, TX": (32.7767, -96.7970), "Denver, CO": (39.7392, -104.9903),
    "Phoenix, AZ": (33.4484, -112.0740), "Los Angeles, CA": (34.0522, -118.2437),
    "San Francisco, CA": (37.7749, -122.4194), "Seattle, WA": (47.6062, -122.3321),
    "Teaneck, NJ": (40.8976, -74.0160),
}
HOME_W = np.array([10, 12, 6, 4, 4, 5, 4, 4, 4, 3, 3, 5, 3, 3, 8], float)
# category: (amount multiplier, online share, risk tier)
CATEGORIES = {
    "grocery": (1.0, 0.10, "low"), "restaurant": (0.8, 0.15, "low"), "gas_station": (0.9, 0.00, "low"),
    "retail": (1.4, 0.45, "low"), "pharmacy": (0.7, 0.15, "low"), "streaming_digital": (0.35, 1.00, "medium"),
    "electronics": (3.0, 0.60, "medium"), "travel_airline": (6.0, 0.90, "medium"), "furniture": (5.0, 0.50, "low"),
    "jewelry": (4.0, 0.40, "high"), "gift_cards": (1.5, 0.80, "high"), "crypto_exchange": (4.0, 1.00, "high"),
    "money_transfer": (3.0, 1.00, "high"),
}
COUNTS = {"grocery": 12, "restaurant": 20, "gas_station": 8, "retail": 15, "pharmacy": 5, "streaming_digital": 8,
          "electronics": 8, "travel_airline": 5, "furniture": 5, "jewelry": 4, "gift_cards": 4,
          "crypto_exchange": 3, "money_transfer": 3}
EVERYDAY = ["grocery", "restaurant", "gas_station", "retail", "pharmacy", "streaming_digital"]
FOREIGN_IP = ["RU", "NG", "BR", "VN", "RO", "UA", "CN"]
FIRST = ["Maya", "Leo", "Ana", "Sam", "Jo", "Priya", "Ravi", "Noah", "Emma", "Liam", "Olivia", "Aarav", "Sofia",
         "Ethan", "Mia", "Lucas", "Zoe", "Omar", "Chloe", "Daniel", "Isha", "Mateo", "Grace", "Kenji", "Nina"]
LAST = ["Patel", "Smith", "Garcia", "Kim", "Johnson", "Nguyen", "Brown", "Lopez", "Shah", "Davis", "Chen",
        "Wilson", "Martinez", "Khan", "Taylor", "Rossi", "Singh", "Lee", "Clark", "Ali"]
AGE_BANDS, AGE_W = ["18-29", "30-44", "45-64", "65+"], [0.28, 0.30, 0.27, 0.15]


def km(a, b):
    (la1, lo1), (la2, lo2) = a, b
    p1, p2 = math.radians(la1), math.radians(la2)
    h = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lo2 - lo1) / 2) ** 2
    return 2 * 6371 * math.asin(math.sqrt(h))


def generate():
    rng = np.random.default_rng(SEED)
    merchants, by_cat = [], {}
    mid = 1
    for cat, n in COUNTS.items():
        for k in range(n):
            m = {"merchant_id": f"M{mid:04d}", "merchant_name": f"{cat.replace('_', ' ').title()} Co #{k + 1}",
                 "merchant_category": cat, "risk_tier": CATEGORIES[cat][2]}
            merchants.append(m)
            by_cat.setdefault(cat, []).append(m["merchant_id"])
            mid += 1
    cat_of = {m["merchant_id"]: m["merchant_category"] for m in merchants}
    fav_pool = [m["merchant_id"] for m in merchants if m["merchant_category"] in EVERYDAY + ["electronics"]]
    city_names = list(CITIES)
    end = START + timedelta(days=DAYS)
    tx, customers, cards = [], [], []

    def add(c, ts, mid_, amount, card_present, city, device, ip="US", pattern=None):
        lat, lon = CITIES[city]
        tx.append({"event_ts": ts, "card_id": c["card_id"], "merchant_id": mid_,
                   "amount": round(float(max(amount, 0.5)), 2), "currency": "USD",
                   "channel": "POS" if card_present else "ONLINE", "card_present": bool(card_present),
                   "device_id": None if card_present else device, "ip_country": None if card_present else ip,
                   "txn_city": city, "txn_lat": lat, "txn_lon": lon, "_pattern": pattern})

    for i in range(N_CUSTOMERS):
        home = city_names[rng.choice(len(city_names), p=HOME_W / HOME_W.sum())]
        age_band = str(rng.choice(AGE_BANDS, p=AGE_W))
        c = {"customer_id": f"C{i + 1:05d}", "card_id": f"CARD-{rng.integers(10**7, 10**8)}", "home": home,
             "mu": rng.normal(3.7, 0.45), "sigma": rng.uniform(0.45, 0.75),
             "peak": 23 if rng.random() < 0.05 else int(np.clip(rng.normal(14, 3), 8, 21)),
             "online_bias": rng.uniform(0.6, 1.4) * (1.3 if age_band == "18-29" else 1.0),
             "devices": [f"DEV-{rng.integers(10**6, 10**7)}"], "fav": list(rng.choice(fav_pool, 8, replace=False))}
        customers.append({"customer_id": c["customer_id"], "first_name": str(rng.choice(FIRST)),
                          "last_name": str(rng.choice(LAST)), "age_band": age_band, "home_city": home})
        cards.append({"card_id": c["card_id"], "customer_id": c["customer_id"],
                      "card_type": str(rng.choice(["credit", "debit"], p=[0.6, 0.4])),
                      "issued_date": (START - timedelta(days=int(rng.integers(60, 2500)))).strftime("%Y-%m-%d")})
        c["_i"] = i
        cust_state.append(c)

        n = rng.poisson(26 * DAYS / 30)
        times = sorted(START + timedelta(seconds=float(s)) for s in rng.uniform(0, DAYS * 86400, n))
        times = sorted(t.replace(hour=int(np.clip(rng.normal(c["peak"], 2.5), 0, 23))) for t in times)
        trip = None
        if rng.random() < 0.08:
            d0 = int(rng.integers(3, DAYS - 6))
            dest = str(rng.choice([x for x in city_names if km(CITIES[x], CITIES[home]) > 800]))
            trip = (START + timedelta(days=d0), START + timedelta(days=d0 + int(rng.integers(2, 5))), dest)
        new_phone = START + timedelta(days=float(rng.uniform(5, DAYS - 3))) if rng.random() < 0.10 else None
        big_buy = START + timedelta(days=float(rng.uniform(2, DAYS - 1))) if rng.random() < 0.04 else None
        for t in times:
            if trip and trip[0] <= t <= trip[0] + timedelta(hours=6):
                continue
            city = trip[2] if trip and trip[0] <= t <= trip[1] else home
            mid_ = rng.choice(c["fav"]) if rng.random() < 0.85 else rng.choice(by_cat[str(rng.choice(EVERYDAY))])
            cat = cat_of[mid_]
            online = rng.random() < min(CATEGORIES[cat][1] * c["online_bias"], 1.0)
            if new_phone and t >= new_phone and len(c["devices"]) == 1:
                c["devices"].append(f"DEV-{rng.integers(10**6, 10**7)}")
            ip = str(rng.choice(FOREIGN_IP)) if (online and rng.random() < 0.01) else "US"
            add(c, t, mid_, math.exp(rng.normal(c["mu"], c["sigma"])) * CATEGORIES[cat][0], not online,
                home if online else city, c["devices"][-1], ip)
        if big_buy:
            cat = str(rng.choice(["electronics", "furniture", "travel_airline", "jewelry"]))
            add(c, big_buy.replace(hour=int(rng.integers(10, 20))), rng.choice(by_cat[cat]),
                math.exp(c["mu"]) * rng.uniform(8, 20), rng.random() < 0.5, home, c["devices"][-1])

    def rand_time():
        return START + timedelta(days=float(rng.uniform(1, DAYS - 0.2)))

    def new_dev():
        return f"DEV-{rng.integers(10**6, 10**7)}"

    victims = iter(rng.choice(N_CUSTOMERS, size=520, replace=False))
    for _ in range(48):    # card testing
        c = cust_state[next(victims)]
        t0, dev, ip = rand_time(), new_dev(), str(rng.choice(FOREIGN_IP + ["US", "US"]))
        for k in range(int(rng.integers(4, 9))):
            cat = str(rng.choice(["streaming_digital", "gift_cards"]))
            add(c, t0 + timedelta(minutes=float(k * rng.uniform(0.5, 4))), rng.choice(by_cat[cat]),
                rng.uniform(0.99, 4.99), False, c["home"], dev, ip, "card_testing")
        if rng.random() < 0.5:
            add(c, t0 + timedelta(minutes=float(rng.uniform(30, 120))), rng.choice(by_cat["electronics"]),
                math.exp(c["mu"]) * rng.uniform(6, 15), False, c["home"], dev, ip, "card_testing")
    for _ in range(118):   # account takeover
        c = cust_state[next(victims)]
        cat = str(rng.choice(["electronics", "jewelry", "gift_cards", "retail"]))
        add(c, rand_time().replace(hour=int(rng.integers(0, 5))), rng.choice(by_cat[cat]),
            math.exp(c["mu"]) * rng.uniform(5, 18), False, c["home"], new_dev(),
            str(rng.choice(FOREIGN_IP)) if rng.random() < 0.75 else "US", "account_takeover")
    for _ in range(90):    # impossible travel
        c = cust_state[next(victims)]
        t = rand_time()
        add(c, t, rng.choice(by_cat["grocery"]), math.exp(rng.normal(c["mu"], 0.3)), True, c["home"], None)
        far = str(rng.choice([x for x in city_names if km(CITIES[x], CITIES[c["home"]]) > 1000]))
        cat = str(rng.choice(["electronics", "retail", "jewelry", "gas_station"]))
        add(c, t + timedelta(minutes=float(rng.uniform(20, 150))), rng.choice(by_cat[cat]),
            math.exp(c["mu"]) * rng.uniform(1.5, 8), True, far, None, pattern="impossible_travel")
    for _ in range(85):    # high-risk merchant cash-out
        c = cust_state[next(victims)]
        cat = str(rng.choice(["crypto_exchange", "money_transfer", "gift_cards"]))
        add(c, rand_time(), rng.choice(by_cat[cat]), rng.uniform(400, 2500), False, c["home"],
            c["devices"][-1] if rng.random() < 0.5 else new_dev(), "US", "high_risk_merchant")
    for _ in range(960):   # genuine look-alikes: evening splurges at new shops
        c = cust_state[int(rng.integers(N_CUSTOMERS))]
        cat = str(rng.choice(["retail", "electronics", "restaurant", "pharmacy"]))
        add(c, rand_time().replace(hour=int(rng.integers(9, 24))), rng.choice(by_cat[cat]),
            math.exp(c["mu"]) * rng.uniform(2.0, 4.5), False, c["home"],
            c["devices"][-1] if rng.random() < 0.8 else new_dev())
    for _ in range(51):    # subtle fraud: quiet spree at new shops
        c = cust_state[next(victims)]
        t = rand_time().replace(hour=int(rng.integers(12, 22)))
        dev = c["devices"][-1] if rng.random() < 0.5 else new_dev()
        for k in range(int(rng.integers(2, 5))):
            cat = str(rng.choice(["retail", "electronics", "restaurant", "pharmacy"]))
            pool = [m for m in by_cat[cat] if m not in c["fav"]] or by_cat[cat]
            add(c, t + timedelta(minutes=float(k * rng.uniform(20, 90))), rng.choice(pool),
                math.exp(c["mu"]) * rng.uniform(2.0, 4.5), False, c["home"], dev, "US", "subtle_fraud")

    tx = sorted((r for r in tx if r["event_ts"] < end), key=lambda r: r["event_ts"])
    for k, r in enumerate(tx, 1):
        r["transaction_id"] = f"TXN-{k:07d}"
        r["event_type"] = "card_authorization"
    return tx, customers, cards, merchants


cust_state = []
transactions, customers, cards, merchants = generate()
print(f"Generated {len(transactions):,} transactions over {DAYS} days for {len(customers):,} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write reference data and drop the event files

# COMMAND ----------

def write_jsonl(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")

write_jsonl(f"{REF_PATH}/customers/customers.json", customers)
write_jsonl(f"{REF_PATH}/cards/cards.json", cards)
write_jsonl(f"{REF_PATH}/merchants/merchants.json", merchants)

TXN_FIELDS = ["transaction_id", "event_type", "event_ts", "card_id", "merchant_id", "amount", "currency",
              "channel", "card_present", "device_id", "ip_country", "txn_city", "txn_lat", "txn_lon"]
written = 0
for d in range(EMIT_FROM, EMIT_TO + 1):
    day = START + timedelta(days=d - 1)
    tag = day.strftime("%Y-%m-%d")
    txn_file, label_file = f"{TXN_PATH}/txn_{tag}.json", f"{LABEL_PATH}/chargebacks_{tag}.json"
    if os.path.exists(txn_file):
        print(f"  {tag}: already landed, skipped")
        continue
    rows = [r for r in transactions if r["event_ts"].date() == day.date()]
    write_jsonl(label_file, [{"transaction_id": r["transaction_id"], "label": "confirmed_fraud",
                              "fraud_pattern": r["_pattern"],
                              "reported_ts": (r["event_ts"] + timedelta(days=3)).isoformat()}
                             for r in rows if r["_pattern"]])
    write_jsonl(txn_file, [{k: (r[k].isoformat() if k == "event_ts" else r[k]) for k in TXN_FIELDS} for r in rows])
    written += 1
    print(f"  {tag}: {len(rows):,} transactions landed")
    if PAUSE:
        time.sleep(PAUSE)
print(f"Done: {written} new day file(s) in {TXN_PATH}")
