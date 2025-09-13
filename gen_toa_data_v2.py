#!/usr/bin/env python3

"""
gen_toa_data_v2.py

Script for generating restaurant-style order data (similar to McDonald’s) and either 
sending it to Azure Event Hub or saving historical data to a CSV file.

Main functionalities:
  - `stream` mode: continuously send order events every N seconds to Event Hub.
  - `historical` mode: generate orders within a given date range and save to a CSV file.
  - Unique, incremental `order_id` persisted across restarts.
  - Weather simulation with trends (temperature, humidity, condition) and its impact on order volume.
  - Probabilistic selection of items within an order (e.g., fries more likely with burgers).
  - Each order may include categories: hamburgers, fried chicken, fries, beverages, ice cream, desserts.
  - Support for multiple register types: in-store, drive-through, kiosks — each with its own ID.
  - Configurable parameters: interval, date range, output file.
"""

# python gen_toa_data_v2.py --mode stream --interval 15
# python gen_toa_data_v2.py --mode historical --start_date 2025-05-01T00:00:00 --end_date 2025-08-01T23:59:59 --interval 15 --output august01_orders.csv

import argparse
import csv
import json
import os
import random
import time
from datetime import datetime, timedelta

from azure.eventhub import EventHubProducerClient, EventData

# --- Event Hub ---
EVENTHUB_CONN_STR = os.getenv("EVENTHUB_CONN_STR", "xxx")
EVENTHUB_NAME     = os.getenv("EVENTHUB_NAME", "xxx")
ORDER_ID_FILE     = "order_id_counter.txt"

MENU_PRICES = {
    "hamburger":      12.0,
    "fried_chicken":  14.0,
    "fries":           6.0,
    "drink":           5.0,
    "ice_cream":       7.0,
    "dessert":         8.0
}

EMPLOYEES = [f"employee_{i+1}" for i in range(15)]
REGISTERS = {
    "stationary": [f"stationary_{i+1}" for i in range(3)],
    "drive":      [f"drive_{i+1}"      for i in range(2)],
    "kiosk":      [f"kiosk_{i+1}"      for i in range(2)]
}

WEATHER_CONDITIONS = ["sunny", "cloudy", "rainy", "snowy"]


def load_last_order_id():
    if not os.path.exists(ORDER_ID_FILE):
        with open(ORDER_ID_FILE, "w") as f:
            f.write("0")
        return 0
    return int(open(ORDER_ID_FILE).read().strip())


def save_last_order_id(last_id):
    with open(ORDER_ID_FILE, "w") as f:
        f.write(str(last_id))


def next_order_id():
    last = load_last_order_id()
    new  = last + 1
    save_last_order_id(new)
    return f"order_{new}"


class WeatherSimulator:
    def __init__(self, start=None):
        self.current = start or {"temperature": 20.0, "humidity": 50.0, "condition": "sunny"}

    def step(self):
        # Powolne zmiany
        self.current["temperature"] = round(min(max(self.current["temperature"] + random.uniform(-0.2, 0.2), -5), 35), 1)
        self.current["humidity"]    = round(min(max(self.current["humidity"] + random.uniform(-0.5, 0.5), 20), 90), 1)
        if random.random() < 0.05:
            self.current["condition"] = random.choice(WEATHER_CONDITIONS)
        return dict(self.current)


def generate_items(order_size):
    items = {}
    total = 0.0
    for item, price in MENU_PRICES.items():
        # probabilistic item combinations
        base_prob = 0.5
        if item == "fries":
            base_prob = 0.7
        if item in ("ice_cream", "dessert"):
            base_prob = 0.3
        if random.random() < base_prob:
            max_qty = 2 if order_size == "small" else 5
            qty = random.randint(1, max_qty)
            items[item] = qty
            total += qty * price
    return items, round(total, 2)


def create_order(ts: datetime, weather: dict):
    register_type = random.choice(list(REGISTERS.keys()))
    register_id   = random.choice(REGISTERS[register_type])
    employee_id   = random.choice(EMPLOYEES)
    size_weights  = [0.6, 0.4]  # small vs large
    order_size    = random.choices(["small", "large"], weights=size_weights)[0]

    items, total_amount = generate_items(order_size)

    return {
        "order_id":        next_order_id(),
        "timestamp":       ts.isoformat(),
        "restaurant_id":   "restaurant_001",
        "register_type":   register_type,
        "register_id":     register_id,
        "employee_id":     employee_id,
        "order_size":      order_size,
        "items":           items,
        "total_amount":    total_amount,
        "processing_time": ts.isoformat(),
        "IsWeather":       weather
    }


def send_to_eventhub(producer, data):
    batch = producer.create_batch()
    batch.add(EventData(json.dumps(data)))
    producer.send_batch(batch)


def stream_mode(interval_sec: int):
    assert EVENTHUB_CONN_STR and EVENTHUB_NAME, "Missing ENV variables: EVENTHUB_CONN_STR / EVENTHUB_NAME"
    producer    = EventHubProducerClient.from_connection_string(EVENTHUB_CONN_STR, eventhub_name=EVENTHUB_NAME)
    weather_sim = WeatherSimulator()
    try:
        while True:
            ts      = datetime.utcnow()
            weather = weather_sim.step()
            order   = create_order(ts, weather)
            send_to_eventhub(producer, order)
            print(f"[{ts.isoformat()}] Sent {order['order_id']}")
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("Streaming stopped by user.")
    finally:
        producer.close()


def historical_mode(start: datetime, end: datetime, interval_sec: int, output: str):
    weather_sim = WeatherSimulator()
    ts = start
    with open(output, "w", newline="") as csvfile:
        fieldnames = ["order_id","timestamp","restaurant_id","register_type","register_id",
                      "employee_id","order_size","items","total_amount","processing_time","IsWeather"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        while ts <= end:
            weather = weather_sim.step()
            order   = create_order(ts, weather)
            writer.writerow(order)
            ts += timedelta(seconds=interval_sec)
    print(f"Historical data written to {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Order Event Generator")
    parser.add_argument("--mode", choices=["stream", "historical"], default="stream")
    parser.add_argument("--interval", type=int, default=15, help="Seconds between orders")
    parser.add_argument("--start_date", type=lambda s: datetime.fromisoformat(s), help="ISO start datetime")
    parser.add_argument("--end_date",   type=lambda s: datetime.fromisoformat(s), help="ISO end datetime")
    parser.add_argument("--output",     type=str, default="orders.csv", help="Output CSV path for historical mode")

    args = parser.parse_args()

    if args.mode == "stream":
        stream_mode(args.interval)
    else:
        assert args.start_date and args.end_date, "Please provide --start_date and --end_date when using the 'historical' mode"
        historical_mode(args.start_date, args.end_date, args.interval, args.output)
