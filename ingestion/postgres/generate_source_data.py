"""
generate_source_data.py

Populates the RetailPulse OLTP Postgres database with realistic-looking
data, and deliberately injects known bad-data cases so there's something
real for the downstream data quality framework (Part 12) to catch later.

Bad-data cases seeded here:
    - duplicate customers   (same person, two customer_id rows)
    - NULL emails
    - invalid prices         (negative price on some products)
    - future signup_date     (customers "signed up" in the future)
    - duplicate orders       (same order content, two order_id rows)
    - invalid product_id     (order_items pointing at a product_id that
                               doesn't exist — see 02_relax_constraints.sql,
                               which is what makes this insertable at all)

Run with:
    python ingestion/postgres/generate_source_data.py
"""
import os
import random
from datetime import date, datetime, timedelta

import psycopg2
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

# ── Config ───────────────────────────────────────────────────────────────
NUM_SUPPLIERS = 20
NUM_STORES = 15
NUM_PRODUCTS = 500
NUM_CUSTOMERS = 2000
NUM_ORDERS = 5000
MAX_ITEMS_PER_ORDER = 4

# Roughly what fraction of each table's rows are deliberately bad data.
BAD_DATA_RATE = 0.03

PRODUCT_CATEGORIES = [
    "Electronics", "Home & Kitchen", "Clothing", "Grocery", "Toys",
    "Sports & Outdoors", "Books", "Beauty", "Automotive", "Furniture",
]
ORDER_STATUSES = ["placed", "shipped", "delivered", "cancelled", "returned"]
PAYMENT_METHODS = ["credit_card", "debit_card", "upi", "net_banking", "cod"]
PAYMENT_STATUSES = ["completed", "failed", "pending", "refunded"]

fake = Faker()
seed = os.getenv("SEED")
if seed:
    Faker.seed(int(seed))
    random.seed(int(seed))


def get_connection():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "retailpulse"),
        user=os.getenv("PGUSER", "retailpulse"),
        password=os.getenv("PGPASSWORD", "retailpulse_dev_pw"),
    )


# ── Generators — each returns the list of ids it inserted ──────────────────
def generate_suppliers(cur) -> list[int]:
    ids = []
    for _ in range(NUM_SUPPLIERS):
        cur.execute(
            "INSERT INTO suppliers (supplier_name, country) VALUES (%s, %s) "
            "RETURNING supplier_id",
            (fake.company(), fake.country()),
        )
        ids.append(cur.fetchone()[0])
    return ids


def generate_stores(cur) -> list[int]:
    ids = []
    for _ in range(NUM_STORES):
        cur.execute(
            "INSERT INTO stores (store_name, city, state) VALUES (%s, %s, %s) "
            "RETURNING store_id",
            (f"{fake.city()} Store", fake.city(), fake.state()),
        )
        ids.append(cur.fetchone()[0])
    return ids


def generate_products(cur, supplier_ids: list[int]) -> list[int]:
    ids = []
    for _ in range(NUM_PRODUCTS):
        price = round(random.uniform(5, 2000), 2)
        # Bad data: some products get an invalid (negative) price
        if random.random() < BAD_DATA_RATE:
            price = -abs(price)

        cur.execute(
            "INSERT INTO products (product_name, category, price, supplier_id) "
            "VALUES (%s, %s, %s, %s) RETURNING product_id",
            (
                fake.unique.catch_phrase(),
                random.choice(PRODUCT_CATEGORIES),
                price,
                random.choice(supplier_ids),
            ),
        )
        ids.append(cur.fetchone()[0])
    return ids


def _random_signup_date() -> date:
    # Bad data: some customers "signed up" in the future
    if random.random() < BAD_DATA_RATE:
        return date.today() + timedelta(days=random.randint(1, 365))
    return fake.date_between(start_date="-3y", end_date="today")


def generate_customers(cur) -> list[int]:
    ids = []
    inserted_rows = []  # keep the raw data around so we can duplicate some later

    for _ in range(NUM_CUSTOMERS):
        name = fake.name()
        # Bad data: some customers have a NULL email
        email = None if random.random() < BAD_DATA_RATE else fake.unique.email()
        city = fake.city()
        state = fake.state()
        signup_date = _random_signup_date()

        cur.execute(
            "INSERT INTO customers (name, email, city, state, signup_date) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING customer_id",
            (name, email, city, state, signup_date),
        )
        cust_id = cur.fetchone()[0]
        ids.append(cust_id)
        inserted_rows.append((name, email, city, state, signup_date))

    # Bad data: duplicate customers — same person's details re-inserted
    # under a brand new customer_id, simulating an upstream dedup bug.
    num_duplicates = int(NUM_CUSTOMERS * BAD_DATA_RATE)
    for _ in range(num_duplicates):
        name, email, city, state, signup_date = random.choice(inserted_rows)
        cur.execute(
            "INSERT INTO customers (name, email, city, state, signup_date) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING customer_id",
            (name, email, city, state, signup_date),
        )
        ids.append(cur.fetchone()[0])

    return ids


def generate_orders(cur, customer_ids: list[int], store_ids: list[int]) -> list[int]:
    ids = []
    inserted_rows = []

    for _ in range(NUM_ORDERS):
        customer_id = random.choice(customer_ids)
        store_id = random.choice(store_ids)
        order_date = fake.date_time_between(start_date="-1y", end_date="now")
        status = random.choice(ORDER_STATUSES)
        total_amount = round(random.uniform(10, 5000), 2)

        cur.execute(
            "INSERT INTO orders (customer_id, store_id, order_date, status, total_amount) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING order_id",
            (customer_id, store_id, order_date, status, total_amount),
        )
        order_id = cur.fetchone()[0]
        ids.append(order_id)
        inserted_rows.append((customer_id, store_id, order_date, status, total_amount))

    # Bad data: duplicate orders — same order content re-inserted under a
    # new order_id, simulating a retry/at-least-once delivery bug upstream.
    num_duplicates = int(NUM_ORDERS * BAD_DATA_RATE)
    for _ in range(num_duplicates):
        customer_id, store_id, order_date, status, total_amount = random.choice(inserted_rows)
        cur.execute(
            "INSERT INTO orders (customer_id, store_id, order_date, status, total_amount) "
            "VALUES (%s, %s, %s, %s, %s) RETURNING order_id",
            (customer_id, store_id, order_date, status, total_amount),
        )
        ids.append(cur.fetchone()[0])

    return ids


def generate_order_items(cur, order_ids: list[int], product_ids: list[int]) -> None:
    # An id guaranteed to not exist in products, for the invalid-FK bad case.
    invalid_product_id = max(product_ids) + 9999

    for order_id in order_ids:
        num_items = random.randint(1, MAX_ITEMS_PER_ORDER)
        for _ in range(num_items):
            # Bad data: some order_items reference a product_id that
            # doesn't exist (see 02_relax_constraints.sql).
            if random.random() < BAD_DATA_RATE:
                product_id = invalid_product_id
            else:
                product_id = random.choice(product_ids)

            cur.execute(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) "
                "VALUES (%s, %s, %s, %s)",
                (order_id, product_id, random.randint(1, 5), round(random.uniform(5, 2000), 2)),
            )


def generate_payments(cur, order_ids: list[int]) -> None:
    for order_id in order_ids:
        # Most orders have exactly one payment; a few have none (e.g. cancelled
        # before payment), which is realistic and not treated as "bad data".
        if random.random() < 0.95:
            cur.execute(
                "INSERT INTO payments (order_id, payment_method, amount, payment_status, payment_date) "
                "VALUES (%s, %s, %s, %s, %s)",
                (
                    order_id,
                    random.choice(PAYMENT_METHODS),
                    round(random.uniform(10, 5000), 2),
                    random.choice(PAYMENT_STATUSES),
                    fake.date_time_between(start_date="-1y", end_date="now"),
                ),
            )


def generate_inventory(cur, product_ids: list[int], store_ids: list[int]) -> None:
    for product_id in product_ids:
        # Each product is stocked at a handful of stores, not necessarily all.
        stocked_at = random.sample(store_ids, k=min(len(store_ids), random.randint(1, 5)))
        for store_id in stocked_at:
            cur.execute(
                "INSERT INTO inventory (product_id, store_id, quantity_on_hand, last_restock_date) "
                "VALUES (%s, %s, %s, %s)",
                (
                    product_id,
                    store_id,
                    random.randint(0, 500),
                    fake.date_between(start_date="-90d", end_date="today"),
                ),
            )


def main():
    conn = get_connection()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        print("Generating suppliers...")
        supplier_ids = generate_suppliers(cur)

        print("Generating stores...")
        store_ids = generate_stores(cur)

        print("Generating products...")
        product_ids = generate_products(cur, supplier_ids)

        print("Generating customers...")
        customer_ids = generate_customers(cur)

        print("Generating orders...")
        order_ids = generate_orders(cur, customer_ids, store_ids)

        print("Generating order_items...")
        generate_order_items(cur, order_ids, product_ids)

        print("Generating payments...")
        generate_payments(cur, order_ids)

        print("Generating inventory...")
        generate_inventory(cur, product_ids, store_ids)

        conn.commit()
        print("\nDone. Row counts:")
        print(f"  suppliers:    {len(supplier_ids)}")
        print(f"  stores:       {len(store_ids)}")
        print(f"  products:     {len(product_ids)}")
        print(f"  customers:    {len(customer_ids)}  (includes ~{int(NUM_CUSTOMERS * BAD_DATA_RATE)} deliberate duplicates)")
        print(f"  orders:       {len(order_ids)}  (includes ~{int(NUM_ORDERS * BAD_DATA_RATE)} deliberate duplicates)")

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()