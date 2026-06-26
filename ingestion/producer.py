"""
Clickstream Event Producer
Simulates e-commerce user behavior events and publishes to Kafka.

Events: page_view, product_view, add_to_cart, remove_from_cart,
        checkout_start, purchase, session_start, session_end
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone
from faker import Faker
from confluent_kafka import Producer
from config import KAFKA_CONFIG, TOPIC_NAME

fake = Faker()

# ── Product catalog ──────────────────────────────────────────────
PRODUCTS = [
    {"product_id": f"P{str(i).zfill(4)}", "name": fake.catch_phrase(),
     "category": random.choice(["Electronics", "Clothing", "Home", "Sports", "Beauty"]),
     "price": round(random.uniform(9.99, 499.99), 2)}
    for i in range(1, 51)
]

PAGES = ["home", "search", "category", "product", "cart", "checkout", "confirmation", "account"]
DEVICES = ["desktop", "mobile", "tablet"]
BROWSERS = ["chrome", "safari", "firefox", "edge"]
CHANNELS = ["organic", "paid_search", "email", "social", "direct", "referral"]


# ── Event builders ───────────────────────────────────────────────
def base_event(event_type: str, session_id: str, user_id: str, device: str, browser: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "session_id": session_id,
        "user_id": user_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_type": device,
        "browser": browser,
        "ip_address": fake.ipv4(),
        "user_agent": fake.user_agent(),
        "country": fake.country_code(),
        "city": fake.city(),
    }


def session_start_event(session_id, user_id, device, browser):
    e = base_event("session_start", session_id, user_id, device, browser)
    e.update({
        "page": "home",
        "referrer": fake.url(),
        "channel": random.choice(CHANNELS),
        "utm_source": random.choice(["google", "facebook", "newsletter", None]),
        "utm_campaign": random.choice(["summer_sale", "retargeting", "brand", None]),
    })
    return e


def page_view_event(session_id, user_id, device, browser):
    e = base_event("page_view", session_id, user_id, device, browser)
    e.update({
        "page": random.choice(PAGES),
        "page_url": fake.uri(),
        "time_on_page_seconds": random.randint(5, 300),
        "scroll_depth_pct": random.randint(10, 100),
    })
    return e


def product_view_event(session_id, user_id, device, browser):
    product = random.choice(PRODUCTS)
    e = base_event("product_view", session_id, user_id, device, browser)
    e.update({
        "page": "product",
        **product,
        "image_clicks": random.randint(0, 5),
        "review_clicks": random.randint(0, 3),
    })
    return e


def add_to_cart_event(session_id, user_id, device, browser):
    product = random.choice(PRODUCTS)
    e = base_event("add_to_cart", session_id, user_id, device, browser)
    e.update({
        "page": "product",
        **product,
        "quantity": random.randint(1, 4),
        "cart_total": round(product["price"] * random.randint(1, 4), 2),
    })
    return e


def remove_from_cart_event(session_id, user_id, device, browser):
    product = random.choice(PRODUCTS)
    e = base_event("remove_from_cart", session_id, user_id, device, browser)
    e.update({"page": "cart", **product, "quantity": random.randint(1, 2)})
    return e


def checkout_start_event(session_id, user_id, device, browser):
    e = base_event("checkout_start", session_id, user_id, device, browser)
    e.update({
        "page": "checkout",
        "cart_item_count": random.randint(1, 8),
        "cart_total": round(random.uniform(20, 800), 2),
    })
    return e


def purchase_event(session_id, user_id, device, browser):
    items = random.randint(1, 5)
    product = random.choice(PRODUCTS)
    subtotal = round(product["price"] * items, 2)
    tax = round(subtotal * 0.08, 2)
    e = base_event("purchase", session_id, user_id, device, browser)
    e.update({
        "page": "confirmation",
        "order_id": str(uuid.uuid4()),
        "product_id": product["product_id"],
        "product_name": product["name"],
        "category": product["category"],
        "quantity": items,
        "unit_price": product["price"],
        "subtotal": subtotal,
        "tax": tax,
        "shipping": round(random.uniform(0, 15), 2),
        "total_amount": round(subtotal + tax, 2),
        "payment_method": random.choice(["credit_card", "paypal", "apple_pay", "google_pay"]),
        "coupon_used": random.choice([None, "SAVE10", "SUMMER20", "WELCOME15"]),
    })
    return e


def session_end_event(session_id, user_id, device, browser, duration_seconds):
    e = base_event("session_end", session_id, user_id, device, browser)
    e.update({
        "session_duration_seconds": duration_seconds,
        "total_page_views": random.randint(1, 20),
        "bounced": duration_seconds < 15,
    })
    return e


# ── Session simulator ────────────────────────────────────────────
def simulate_session() -> list:
    """Simulate a realistic user session with weighted event flow."""
    session_id = str(uuid.uuid4())
    user_id = f"U{random.randint(1000, 9999)}"
    device = random.choice(DEVICES)
    browser = random.choice(BROWSERS)
    events = []
    duration = 0

    events.append(session_start_event(session_id, user_id, device, browser))

    # Browse phase
    for _ in range(random.randint(1, 5)):
        events.append(page_view_event(session_id, user_id, device, browser))
        duration += random.randint(10, 120)

    # Product interest
    if random.random() > 0.3:
        for _ in range(random.randint(1, 3)):
            events.append(product_view_event(session_id, user_id, device, browser))
            duration += random.randint(20, 180)

    # Add to cart
    if random.random() > 0.5:
        events.append(add_to_cart_event(session_id, user_id, device, browser))
        duration += random.randint(5, 30)

        # Maybe remove
        if random.random() > 0.8:
            events.append(remove_from_cart_event(session_id, user_id, device, browser))

        # Checkout
        if random.random() > 0.4:
            events.append(checkout_start_event(session_id, user_id, device, browser))
            duration += random.randint(30, 120)

            # Purchase
            if random.random() > 0.3:
                events.append(purchase_event(session_id, user_id, device, browser))
                duration += random.randint(10, 30)

    events.append(session_end_event(session_id, user_id, device, browser, duration))
    return events


# ── Kafka delivery callback ──────────────────────────────────────
def delivery_report(err, msg):
    if err:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[OK] {msg.topic()} partition={msg.partition()} offset={msg.offset()}")


# ── Main ─────────────────────────────────────────────────────────
def run(sessions: int = 100, delay_ms: int = 50):
    producer = Producer(KAFKA_CONFIG)
    total_events = 0

    print(f"Starting producer: {sessions} sessions, {delay_ms}ms delay between events")

    for i in range(sessions):
        session_events = simulate_session()
        for event in session_events:
            producer.produce(
                topic=TOPIC_NAME,
                key=event["session_id"],
                value=json.dumps(event),
                callback=delivery_report
            )
            producer.poll(0)
            total_events += 1
            time.sleep(delay_ms / 1000)

        if (i + 1) % 10 == 0:
            print(f"Sessions completed: {i + 1}/{sessions} | Events sent: {total_events}")

    producer.flush()
    print(f"\nDone. Total events published: {total_events}")


if __name__ == "__main__":
    run(sessions=100, delay_ms=50)
