"""Load generator: shops, opens order details, signs users up and clicks confirmation links."""
import os
import random
import re
import time

import requests

WEB = os.getenv("WEB_URL", "http://web:8000")
MAILPIT = os.getenv("MAILPIT_API", "http://mailpit:8025")
ITEMS = ["keyboard", "mouse", "monitor", "headset", "webcam"]

while True:
    try:
        page = requests.get(WEB, timeout=30).text
        ids = re.findall(r'href="/order/(\w+)"', page)
        if ids and random.random() < 0.5:
            requests.get(f"{WEB}/order/{random.choice(ids)}", timeout=30)  # hits the slow history query
        requests.post(f"{WEB}/order", data={"item": random.choice(ITEMS), "qty": random.randint(1, 3)},
                      timeout=30, allow_redirects=False)

        if random.random() < 0.3:  # a new user signs up...
            requests.post(f"{WEB}/signup", data={"email": f"user{random.randint(1, 10**6)}@example.com"},
                          timeout=30, allow_redirects=False)
        if random.random() < 0.3:  # ...and someone clicks the confirmation link in their latest email
            latest = requests.get(f"{MAILPIT}/api/v1/messages?limit=1", timeout=10).json()["messages"]
            if latest:
                text = requests.get(f"{MAILPIT}/api/v1/message/{latest[0]['ID']}", timeout=10).json()["Text"]
                if m := re.search(r"/confirm/([\w-]+)", text):
                    requests.get(f"{WEB}/confirm/{m.group(1)}", timeout=30)
    except requests.RequestException as e:
        print(f"loadgen: {e}", flush=True)
    time.sleep(random.uniform(1, 4))
