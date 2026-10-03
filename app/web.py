"""Web frontend: renders orders from the API, places new ones, and controls the chaos knob and the cache."""
import logging
import os

import requests
from flask import Flask, redirect, render_template_string, request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger("web")

API = os.getenv("API_URL", "http://api:5000")
SIGNUP = os.getenv("SIGNUP_URL", "http://signup:5001")
MAILPIT = os.getenv("MAILPIT_URL", "http://localhost:8025")
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
<p><a href="/signup">Signup lab: dual write vs. outbox &rarr;</a></p>
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
<form method="post" action="/cache">Redis cache:
  <button name="enabled" value="1" class="{{ 'on' if cache.enabled }}">on</button>
  <button name="enabled" value="0" class="{{ 'on' if not cache.enabled }}">off</button>
  <small>hits {{ cache.hit }} &middot; misses {{ cache.miss }} &middot; hit ratio {{ cache.hit_ratio }}</small>
</form>
<p><b>Top items</b> <small>(Kafka &rarr; analytics consumer &rarr; Redis sorted set)</small>:
{% for t in analytics.top_items %}{{ t.item }} ({{ t.qty }}){{ ", " if not loop.last }}{% else %}none yet{% endfor %}
&nbsp;|&nbsp; {% for k, v in analytics.status.items() %}{{ k }}: {{ v }} {% endfor %}</p>
<table><tr><th>ID</th><th>Item</th><th>Qty</th><th>Status</th></tr>
{% for o in orders %}<tr><td><a href="/order/{{ o.id }}">{{ o.id }}</a></td><td>{{ o.item }}</td>
<td>{{ o.qty }}</td><td class="{{ o.status }}">{{ o.status }}</td></tr>{% endfor %}
</table>"""

DETAIL = STYLE + """<title>Order {{ o.id }}</title>
<p><a href="/">&larr; back</a></p><h1>Order {{ o.id }}</h1>
<p>{{ o.qty }} &times; {{ o.item }} &mdash; <span class="{{ o.status }}">{{ o.status }}</span></p>
<p><small>Served from <b>{{ o.source }}</b> in {{ ms }} ms</small></p>
<table><tr><th>Event</th><th>At</th></tr>
{% for h in o.history %}<tr><td>{{ h.event }}</td><td>{{ h.created_at }}</td></tr>{% endfor %}</table>"""


SIGNUP_PAGE = STYLE + """<title>Signup lab</title><meta http-equiv="refresh" content="5">
<p><a href="/">&larr; shop</a> &middot; <a href="{{ mailpit }}" target="_blank">open inbox (Mailpit) &rarr;</a></p>
<h1>Signup lab: dual write vs. transactional outbox</h1>
{% if msg %}<p class="{{ 'err' if 'rror' in msg or 'crash' in msg else 'completed' }}">{{ msg }}</p>{% endif %}
<form method="post" action="/signup"><input name="email" type="email" placeholder="you@example.com" required>
  <button>Sign up</button></form>
<form method="post" action="/signup/settings">Mode:
  {% for m in ["naive-db-first", "naive-kafka-first", "outbox"] %}
  <button name="mode" value="{{ m }}" class="{{ 'on' if s.mode == m }}">{{ m }}</button>{% endfor %}</form>
<form method="post" action="/signup/settings">Crash between the two writes:
  {% for f in [0, 0.2, 0.5] %}<button name="failure_rate" value="{{ f }}" class="{{ 'on' if s.failure_rate == f }}">
  {{ (f * 100) | int }}%</button>{% endfor %}</form>
<form method="post" action="/signup/settings">Outbox relay crash after publish:
  {% for f in [0, 0.3] %}<button name="relay_crash_rate" value="{{ f }}" class="{{ 'on' if s.relay_crash_rate == f }}">
  {{ (f * 100) | int }}%</button>{% endfor %}</form>
<form method="post" action="/signup/settings">Email idempotency:
  <button name="idempotent" value="1" class="{{ 'on' if s.idempotent }}">on</button>
  <button name="idempotent" value="0" class="{{ 'on' if not s.idempotent }}">off</button></form>
<table><tr><th>Users</th><th>Confirmed</th><th>Lost emails</th><th>Ghost emails</th><th>Emailed twice+</th>
<th>Outbox pending</th></tr>
<tr><td>{{ s.users }}</td><td>{{ s.confirmed }}</td><td class="{{ 'failed' if s.lost }}">{{ s.lost }}</td>
<td class="{{ 'failed' if s.ghost }}">{{ s.ghost }}</td><td class="{{ 'pending' if s.duplicated }}">{{ s.duplicated }}</td>
<td>{{ s.outbox_pending }}</td></tr></table>
<p><small><b>Lost</b>: user saved but no email after 30 s (can never confirm). <b>Ghost</b>: email sent for a user
that was never saved. <b>Emailed twice+</b>: duplicate deliveries (relay crash + idempotency off).</small></p>
<form method="post" action="/signup/reset"><button>Reset signup data</button></form>"""


@app.get("/signup")
def signup_page():
    s = requests.get(f"{SIGNUP}/signup/status", timeout=10).json()
    return render_template_string(SIGNUP_PAGE, s=s, mailpit=MAILPIT, msg=request.args.get("msg"))


@app.post("/signup")
def do_signup():
    resp = requests.post(f"{SIGNUP}/signup", json={"email": request.form["email"]}, timeout=15)
    body = resp.json()
    msg = (f"Signed up {body['email']} ({body['mode']}). Check the inbox." if resp.status_code == 201
           else f"Signup error ({body.get('mode')}): {body.get('error')}")
    return redirect(f"/signup?msg={requests.utils.quote(msg)}")


@app.post("/signup/settings")
def signup_settings():
    form = dict(request.form)
    if "idempotent" in form:
        form["idempotent"] = form["idempotent"] == "1"
    requests.post(f"{SIGNUP}/signup/settings", json=form, timeout=5)
    return redirect("/signup")


@app.post("/signup/reset")
def signup_reset():
    requests.post(f"{SIGNUP}/signup/reset", timeout=10)
    return redirect("/signup?msg=Signup+data+reset")


@app.get("/confirm/<token>")
def confirm(token):
    resp = requests.get(f"{SIGNUP}/confirm/{token}", timeout=10)
    if resp.status_code == 200:
        return render_template_string(STYLE + "<h1 class='completed'>Account confirmed</h1><p>{{ e }}</p>"
                                      "<p><a href='/signup'>back</a></p>", e=resp.json()["confirmed"])
    return render_template_string(STYLE + "<h1 class='failed'>Account does not exist</h1><p>This confirmation "
                                  "email is a <b>ghost</b>: it was sent, but the user was never saved.</p>"
                                  "<p><a href='/signup'>back</a></p>"), 404


@app.get("/")
def index():
    orders, slow_ms, error = [], 0, request.args.get("error")
    cache, analytics = {"enabled": True, "hit": 0, "miss": 0, "hit_ratio": None}, {"top_items": [], "status": {}}
    try:
        orders = requests.get(f"{API}/orders", timeout=15).json()
        slow_ms = requests.get(f"{API}/settings/slow", timeout=5).json()["slow_ms"]
        cache = requests.get(f"{API}/settings/cache", timeout=5).json()
        analytics = requests.get(f"{API}/analytics", timeout=5).json()
    except requests.RequestException as e:
        log.error("could not load orders: %s", e)
        error = "API unavailable"
    return render_template_string(INDEX, orders=orders, items=ITEMS, slow_ms=slow_ms, error=error,
                                  cache=cache, analytics=analytics)


@app.get("/order/<order_id>")
def order_detail(order_id):
    resp = requests.get(f"{API}/orders/{order_id}", timeout=15)
    if resp.status_code != 200:
        return redirect("/?error=Order+not+found")
    return render_template_string(DETAIL, o=resp.json(), ms=round(resp.elapsed.total_seconds() * 1000))


@app.post("/order")
def order():
    payload = {"item": request.form.get("item", "keyboard"), "qty": request.form.get("qty", 1)}
    resp = requests.post(f"{API}/orders", json=payload, timeout=15)
    if resp.status_code != 201:
        log.warning("order failed status=%s body=%s", resp.status_code, resp.text)
        return redirect(f"/?error=Order+failed+({resp.status_code})")
    return redirect("/")


@app.post("/cache")
def toggle_cache():
    requests.post(f"{API}/settings/cache", json={"enabled": request.form.get("enabled") == "1"}, timeout=5)
    return redirect("/")


@app.post("/chaos")
def chaos():
    requests.post(f"{API}/settings/slow", json={"slow_ms": int(request.form.get("slow_ms", 0))}, timeout=5)
    return redirect("/")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)
