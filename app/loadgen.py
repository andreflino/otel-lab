"""Load generator: browses the shop, opens order details and places orders so there's always telemetry."""
import os
import random
import re
import time

import requests

WEB = os.getenv("WEB_URL", "http://web:8000")
ITEMS = ["keyboard", "mouse", "monitor", "headset", "webcam"]

while True:
    try:
        page = requests.get(WEB, timeout=30).text
        ids = re.findall(r'href="/order/(\w+)"', page)
        if ids and random.random() < 0.5:
            requests.get(f"{WEB}/order/{random.choice(ids)}", timeout=30)  # hits the slow history query
        requests.post(f"{WEB}/order", data={"item": random.choice(ITEMS), "qty": random.randint(1, 3)},
                      timeout=30, allow_redirects=False)
    except requests.RequestException as e:
        print(f"loadgen: {e}", flush=True)
    time.sleep(random.uniform(1, 4))
