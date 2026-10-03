"""Web frontend: renders orders from the API, places new ones, and controls the slow-query chaos knob."""
import logging
import os

import requests
from flask import Flask, redirect, render_template_string, request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("web")

API = os.getenv("API_URL", "http://api:5000")
app = Flask(__name__)
ITEMS = ["keyboard", "mouse", "monitor", "headset", "webcam"]

STYLE = """<style>
 body{font-family:system-ui,sans-serif;max-width:760px;margin:2rem auto;padding:0 1rem;color:#222}
 table{width:100%;border-collapse:collapse}td,th{padding:.4rem;border-bottom:1px solid #ddd;text-align:left}
 .completed{color:#16794a}.failed{color:#c0392b}.pending{color:#b7791f}.err{color:#c0392b}
 form{margin:1rem 0;display:flex;gap:.5rem;align-items:center}input,select,button{padding:.4rem}
 .on{font-weight:bold;outline:2px solid #c0392b}
</style>"""

INDEX = STYLE + """<title>OTel Lab Shop</title><meta http-equiv="refresh" content="5">
<h1>OTel Lab Shop</h1>
{% if error %}<p class="err">{{ error }}</p>{% endif %}
<form method="post" action="/order">
  <select name="item">{% for i in items %}<option>{{ i }}</option>{% endfor %}</select>
  <input name="qty" type="number" value="1" min="1" max="10">
  <button>Place order</button>
</form>
<form method="post" action="/chaos">Slow queries:
  {% for ms in [0, 500, 2000] %}<button name="slow_ms" value="{{ ms }}" class="{{ 'on' if ms == slow_ms }}">
  {{ 'off' if ms == 0 else ms ~ ' ms' }}</button>{% endfor %}
</form>
<table><tr><th>ID</th><th>Item</th><th>Qty</th><th>Status</th></tr>
{% for o in orders %}<tr><td><a href="/order/{{ o.id }}">{{ o.id }}</a></td><td>{{ o.item }}</td>
<td>{{ o.qty }}</td><td class="{{ o.status }}">{{ o.status }}</td></tr>{% endfor %}
</table>"""

DETAIL = STYLE + """<title>Order {{ o.id }}</title>
<p><a href="/">&larr; back</a></p><h1>Order {{ o.id }}</h1>
<p>{{ o.qty }} &times; {{ o.item }} &mdash; <span class="{{ o.status }}">{{ o.status }}</span></p>
<table><tr><th>Event</th><th>At</th></tr>
{% for h in o.history %}<tr><td>{{ h.event }}</td><td>{{ h.created_at }}</td></tr>{% endfor %}</table>"""


@app.get("/")
def index():
    orders, slow_ms, error = [], 0, request.args.get("error")
    try:
        orders = requests.get(f"{API}/orders", timeout=15).json()
        slow_ms = requests.get(f"{API}/settings/slow", timeout=5).json()["slow_ms"]
    except requests.RequestException as e:
        log.error("could not load orders: %s", e)
        error = "API unavailable"
    return render_template_string(INDEX, orders=orders, items=ITEMS, slow_ms=slow_ms, error=error)


@app.get("/order/<order_id>")
def order_detail(order_id):
    resp = requests.get(f"{API}/orders/{order_id}", timeout=15)
    if resp.status_code != 200:
        return redirect("/?error=Order+not+found")
    return render_template_string(DETAIL, o=resp.json())


@app.post("/order")
def order():
    payload = {"item": request.form.get("item", "keyboard"), "qty": request.form.get("qty", 1)}
    resp = requests.post(f"{API}/orders", json=payload, timeout=15)
    if resp.status_code != 201:
        log.warning("order failed status=%s body=%s", resp.status_code, resp.text)
        return redirect(f"/?error=Order+failed+({resp.status_code})")
    return redirect("/")


@app.post("/chaos")
def chaos():
    requests.post(f"{API}/settings/slow", json={"slow_ms": int(request.form.get("slow_ms", 0))}, timeout=5)
    return redirect("/")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)
