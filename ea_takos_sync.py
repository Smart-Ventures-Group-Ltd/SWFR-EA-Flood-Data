#!/usr/bin/env python3
"""Sync EA river gauges and active flood warnings into a GoTAK (TAK OS) Data Sync
mission as native map items, so ATAK, iTAK and WinTAK subscribers receive them.

Environment:
  TAKOS_MCP_URL     default https://live.gotak.dev/api/mcp
  TAKOS_MCP_KEY     bearer key (tactical:read + tactical:write only). Required.
  TAKOS_MISSION_ID  mission id of "EA Flood Data"
  DRY_RUN=1         build items and print the plan without calling TAK OS

Every object has a fixed id derived from its EA reference, so each run updates
the same items. Each new or changed item is attached to the mission (so devices
that open or join Data Sync get the full picture) and also sent to the team as
live CoT (so connected devices, including iTAK, update immediately). Ended
warnings turn grey ("no longer in force"), are sent once, detached from the
mission and left to expire on devices after 24 h. Objects are never deleted, so
ids are never reused.
"""
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
import uuid

import ea_flood_kml as ea

URL = os.environ.get("TAKOS_MCP_URL", "https://live.gotak.dev/api/mcp")
KEY = os.environ.get("TAKOS_MCP_KEY", "")
MISSION = os.environ.get("TAKOS_MISSION_ID", "b8a434af-ad21-4e7e-bd32-724f1e9220d8")
TEAM = os.environ.get("TAKOS_TEAM_ID", "f10187df-0a48-49ec-a7b5-27ebb9b38a47")
DRY = os.environ.get("DRY_RUN") == "1"
NS = uuid.UUID("6f1d3c2a-5b8e-4f7a-9c1d-2e3f4a5b6c7d")
TAG = "[EA-FEED]"
REFRESH_HOURS = 12  # re-send unchanged items so they never go stale on devices (CoT stale = 24 h)
LEVEL_STEP_M = 0.05  # re-send a gauge when its level moves at least this much
TIME_BUDGET_S = int(os.environ.get("TIME_BUDGET_S", "720"))  # stop starting new items after 12 min
STATE_FILE = os.environ.get("STATE_FILE", "state/ea_state.json")
CONFIRM_GAP_S = 6.3  # GoTAK: 10 confirmed calls per minute
MUTATE_GAP_S = 3.2   # GoTAK: 20 mutating calls per minute
MUTATING = {"create_map_object", "update_map_object", "attach_map_object_to_mission",
            "share_map_object", "detach_map_object_from_mission", "delete_map_object"}
SIMPLIFY_DEG = 0.00015  # roughly 10-15 m; invisible at map scale

COL = {"high": "#FF0000", "normal": "#2E8BFF", "low": "#FFD700", "unknown": "#FFFFFF", "none": "#808080"}
WARN = {1: ("SEVERE FLOOD WARNING", "#8B0000", 0.5), 2: ("FLOOD WARNING", "#FF0000", 0.35),
        3: ("FLOOD ALERT", "#FF8C00", 0.25)}


# ---------------------------------------------------------------- MCP client
class MCP:
    def __init__(self, url, key):
        self.url, self.key, self.session, self.n = url, key, None, 0

    def _post(self, payload, expect_reply=True):
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream",
                   "Authorization": f"Bearer {self.key}", "User-Agent": "SWFR-EA-sync/1.0"}
        if self.session:
            headers["Mcp-Session-Id"] = self.session
        req = urllib.request.Request(self.url, data=json.dumps(payload).encode(), headers=headers, method="POST")
        with urllib.request.urlopen(req, timeout=90) as r:
            sid = r.headers.get("Mcp-Session-Id")
            if sid:
                self.session = sid
            body = r.read().decode("utf-8", "replace")
            ctype = r.headers.get("Content-Type", "")
        if not expect_reply or not body.strip():
            return None
        if "text/event-stream" in ctype:
            msgs = [json.loads(l[5:].strip()) for l in body.splitlines() if l.startswith("data:") and l[5:].strip()]
            msgs = [m for m in msgs if m.get("id") == payload.get("id")] or msgs
            return msgs[-1] if msgs else None
        return json.loads(body)

    def rpc(self, method, params=None):
        self.n += 1
        msg = {"jsonrpc": "2.0", "id": self.n, "method": method, "params": params or {}}
        for attempt in range(4):
            try:
                res = self._post(msg)
                break
            except urllib.error.HTTPError as e:
                if e.code in (401, 403):
                    raise SystemExit(f"TAK OS rejected the key (HTTP {e.code}). Check the TAKOS_MCP_KEY secret.")
                if attempt == 3 or e.code < 500 and e.code != 429:
                    raise
            except urllib.error.URLError:
                if attempt == 3:
                    raise
            time.sleep(2 * (attempt + 1))
        if res is None:
            raise RuntimeError(f"no reply to {method}")
        if "error" in res:
            raise RuntimeError(f"{method}: {res['error']}")
        return res.get("result", {})

    def start(self):
        self.rpc("initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
                                "clientInfo": {"name": "swfr-ea-sync", "version": "1.0"}})
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized"}, expect_reply=False)

    _last = {"confirm": 0.0, "mutate": 0.0}

    def _pace(self, tool, args):
        now = time.monotonic()
        if "confirm_token" in args:
            wait = self._last["confirm"] + CONFIRM_GAP_S - now
        elif tool in MUTATING:
            wait = self._last["mutate"] + MUTATE_GAP_S - now
        else:
            wait = 0
        if wait > 0:
            time.sleep(wait)
        if "confirm_token" in args:
            self._last["confirm"] = time.monotonic()
        if tool in MUTATING:
            self._last["mutate"] = time.monotonic()

    def call(self, tool, args):
        for attempt in range(5):
            self._pace(tool, args)
            text, data, err = self._call_once(tool, args)
            m = re.search(r"Rate limit exceeded.*?Retry in (\d+)s", text)
            if err and m:
                time.sleep(int(m.group(1)) + 1)
                continue
            return text, data, err
        return text, data, err

    def _call_once(self, tool, args):
        res = self.rpc("tools/call", {"name": tool, "arguments": args})
        text = "\n".join(c.get("text", "") for c in res.get("content", []) if c.get("type") == "text")
        data = res.get("structuredContent")
        if data is None:
            m = re.search(r"\{.*\}", text, re.S)
            if m:
                try:
                    data = json.loads(m.group(0))
                except ValueError:
                    data = None
        return text, data or {}, bool(res.get("isError"))

    def call_confirmed(self, tool, args):
        for attempt in range(3):
            data = self._confirmed_once(tool, args)
            if data is not None:
                return data
        raise RuntimeError(f"{tool} failed after retries")

    def _confirmed_once(self, tool, args):
        text, data, err = self.call(tool, args)
        token = data.get("confirm_token")
        if not token:
            m = re.search(r'confirm_token["\s:]+"?([A-Za-z0-9_\-]{8,})', text)
            token = m.group(1) if m else None
        if token and ("confirmation" in text.lower() or data.get("status") == "confirmation_required"):
            text, data, err = self.call(tool, {**args, "confirm_token": token})
            if err and re.search(r"token|expired|voided", text, re.I):
                return None  # token lost to a delay; start the confirmation again
        if err:
            raise RuntimeError(f"{tool} failed: {text[:300]}")
        return data


# ---------------------------------------------------------------- EA -> features
def oid(kind, ref):
    return str(uuid.uuid5(NS, f"{kind}:{ref}"))


def simplify(ring):
    try:
        from shapely.geometry import Polygon
        p = Polygon(ring)
        if not p.is_valid:
            p = p.buffer(0)
        s = p.simplify(SIMPLIFY_DEG, preserve_topology=True)
        if s.geom_type == "Polygon" and len(s.exterior.coords) >= 4:
            ring = list(s.exterior.coords)
    except Exception:
        step = max(1, len(ring) // 150)
        ring = ring[::step] + [ring[0]]
    out = [[round(x, 5), round(y, 5)] for x, y, *_ in ring]
    if out[0] != out[-1]:
        out.append(out[0])
    return out


def gauge_items():
    stations = ea.get(f"/id/stations?parameter=level&lat={ea.LAT}&long={ea.LON}&dist={ea.DIST_KM:g}&_view=full")["items"]
    latest = ea.latest_by_measure("level")
    items = []
    for s in stations:
        lat, lon, ref = s.get("lat"), s.get("long"), s.get("stationReference")
        if isinstance(lat, list) or lat is None or lon is None or not ref:
            continue
        reading, unit = None, ""
        for m in ea.as_list(s.get("measures")):
            if m.get("parameter") != "level":
                continue
            r = latest.get(str(m.get("@id", "")).replace("http://", "https://", 1))
            if r and (reading is None or m.get("qualifier") == "Stage"):
                reading = r
            unit = unit or m.get("unitName", "")
        if not reading:
            reading = ea.last_reading_fallback(ref)
        sc = s.get("stageScale") if isinstance(s.get("stageScale"), dict) else {}
        lo, hi = sc.get("typicalRangeLow"), sc.get("typicalRangeHigh")
        rec = (sc.get("highestRecent") or {}).get("value")
        label = str(s.get("label", ref))
        state, tag, v, age, when = "none", "", None, "", ""
        if reading and isinstance(reading.get("value"), (int, float)):
            v = reading["value"]
            t = ea.parse_time(reading.get("dateTime", ""))
            when = reading.get("dateTime", "")
            if hi is not None and v > hi:
                state, tag = "high", " HIGH"
            elif lo is not None and v < lo:
                state, tag = "low", " LOW"
            elif hi is not None or lo is not None:
                state = "normal"
            else:
                state = "unknown"
            if t and (ea.now_utc() - t).total_seconds() > ea.STALE_MINUTES * 60:
                age = uk_time(t)
        name = label if v is None else f"{label} {v:.2f}m{tag}"
        if age:
            name += f" @{age}"
        status = {"high": "ABOVE typical range", "low": "Below typical range", "normal": "Within typical range",
                  "unknown": "No typical range published", "none": "No reading in the last ~4 weeks"}[state]
        remarks = (f"{TAG}gauge\nEA river gauge: {label}\nRiver: {s.get('riverName', '')}\n"
                   f"Level: {'n/a' if v is None else f'{v:.2f} {unit}'}\nStatus: {status}\n"
                   f"Typical range: {lo} to {hi}\nHighest recent: {rec}\n"
                   f"Reading: {when or 'n/a'}{' (no newer reading available)' if age else ''}\n"
                   f"https://check-for-flooding.service.gov.uk/station/{s.get('RLOIid', '')}\n"
                   "Source: Environment Agency real-time API (beta), OGL v3. Not for safety-critical use.")
        items.append({"id": oid("gauge", ref), "feature": {
            "name": name, "kind": "marker", "cot_type": "b-m-p-s-m", "color": COL[state],
            "geometry": {"type": "Point", "coordinates": [round(float(lon), 5), round(float(lat), 5)]},
            "remarks": remarks},
            "meta": {"kind": "gauge", "level": v, "state": state, "stale": bool(age), "reading": when}})
    return items


def uk_time(t):
    try:
        from zoneinfo import ZoneInfo
        lt = t.astimezone(ZoneInfo("Europe/London"))
    except Exception:
        lt = t
    today = ea.now_utc().astimezone(lt.tzinfo).date()
    return lt.strftime("%H:%M") if lt.date() == today else lt.strftime("%d %b %H:%M")


def warning_items():
    floods = ea.get(f"/id/floods?lat={ea.LAT}&long={ea.LON}&dist={ea.DIST_KM:g}")["items"]
    items = []
    for f in floods:
        lvl = f.get("severityLevel", 4)
        if lvl not in WARN:
            continue
        label, colour, opacity = WARN[lvl]
        area = f.get("floodArea", {})
        code = f.get("floodAreaID", "")
        if not area.get("polygon"):
            continue
        gj = ea.get(area["polygon"])
        geoms = [x.get("geometry", x) for x in gj.get("features", [gj])]
        rings = []
        for g in geoms:
            if g.get("type") == "Polygon":
                rings.append(g["coordinates"][0])
            elif g.get("type") == "MultiPolygon":
                rings += [p[0] for p in g["coordinates"]]
        msg = (f.get("message") or "").strip()
        remarks = (f"{TAG}warning\n{label}: {f.get('description', '')}\nArea code: {code}\n"
                   f"River/sea: {area.get('riverOrSea', '')}\nSeverity changed: {f.get('timeSeverityChanged', '')}\n"
                   f"Message changed: {f.get('timeMessageChanged', '')}\n\n{msg[:1500]}\n\n"
                   "Source: Environment Agency real-time API (beta), OGL v3. Official EA warnings are authoritative.")
        for i, ring in enumerate(sorted(rings, key=len, reverse=True)[:20]):
            items.append({"id": oid("warning", f"{code}:{i}"), "feature": {
                "name": f"{label}: {f.get('description', '')}" + (f" ({i + 1})" if i else ""),
                "kind": "polygon", "color": colour, "fill_color": colour, "fill_opacity": opacity,
                "stroke_width": 3, "geometry": {"type": "Polygon", "coordinates": [simplify(ring)]},
                "remarks": remarks},
                "meta": {"kind": "warning", "level": lvl, "code": code,
                         "msg_changed": f.get("timeMessageChanged", "")}})
    return items


# ---------------------------------------------------------------- sync
def same(cur, new):
    c = cur.get("feature", {})
    return (c.get("name") == new["name"] and c.get("remarks") == new["remarks"]
            and (c.get("style") or {}).get("markerColor", (c.get("style") or {}).get("stroke", "")).lower()
            == new.get("color", "").lower() and c.get("visible", True) is not False
            and json.dumps(c.get("geometry", {}).get("coordinates")) == json.dumps(new["geometry"]["coordinates"]))


def fresh(cur):
    t = ea.parse_time(cur.get("updated_at", "") or "")
    return t is not None and (ea.now_utc() - t).total_seconds() < REFRESH_HOURS * 3600


def attach(mcp, ident, rev):
    mcp.call_confirmed("attach_map_object_to_mission", {
        "operation_id": str(uuid.uuid4()), "id": ident, "expected_revision": rev, "mission_id": MISSION})


def share(mcp, ident, rev):
    mcp.call_confirmed("share_map_object", {
        "operation_id": str(uuid.uuid4()), "id": ident, "expected_revision": rev, "team_id": TEAM})


def publish(mcp, ident, rev):
    attach(mcp, ident, rev)
    share(mcp, ident, rev)


def load_state():
    try:
        with open(STATE_FILE) as fh:
            return json.load(fh)
    except Exception:
        return {}


def save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(state, fh)
    os.replace(tmp, STATE_FILE)


def priority(item, prev):
    """None if no send is needed, else a sort key (lower = sooner)."""
    m = item["meta"]
    if not prev:
        return (1 if m["kind"] == "warning" else 3, 0)
    if m["kind"] == "warning":
        if prev.get("level") != m["level"] or prev.get("msg_changed") != m["msg_changed"] or not prev.get("active"):
            return (0, 0)
    else:
        if prev.get("state") != m["state"] or prev.get("stale") != m["stale"]:
            return (2, 0)
        a, b = prev.get("level"), m["level"]
        if a is not None and b is not None and abs(a - b) >= LEVEL_STEP_M:
            return (2, -abs(a - b))
        if (a is None) != (b is None):
            return (2, 0)
    sent = ea.parse_time(prev.get("sent", "") or "")
    if not sent or (ea.now_utc() - sent).total_seconds() > REFRESH_HOURS * 3600:
        return (4, 0)
    return None


def publish_item(mcp, item):
    ident, feat = item["id"], item["feature"]
    text, cur, err = mcp.call("get_map_object", {"id": ident})
    exists = not err and cur.get("id") == ident and not cur.get("deleted")
    if not exists:
        text, res, err = mcp.call("create_map_object", {"operation_id": ident, "feature": feat})
        if err or res.get("id") != ident:
            raise RuntimeError(f"create failed: {text[:200]}")
        rev, action = res["revision"], "created"
    elif same(cur, feat):
        rev, action = cur["revision"], "resent"
    else:
        text, res, err = mcp.call("update_map_object", {"operation_id": str(uuid.uuid4()), "id": ident,
                                                        "expected_revision": cur["revision"], "feature": feat})
        if err or "revision" not in res:
            raise RuntimeError(f"update failed: {text[:200]}")
        rev, action = res["revision"], "updated"
    publish(mcp, ident, rev)
    return action


def retire(mcp, ident, prev):
    cur = mcp.call("get_map_object", {"id": ident})[1]
    cf = cur["feature"]
    ended = {"name": "NO LONGER IN FORCE: " + cf.get("name", "").split(": ", 1)[-1], "kind": "polygon",
             "geometry": cf["geometry"], "color": "#808080", "fill_color": "#808080", "fill_opacity": 0.1,
             "stroke_width": 2, "stroke_style": "dashed",
             "remarks": f"{TAG}warning-ended\nEA warning no longer in force (seen ended "
                        f"{ea.now_utc().strftime('%d %b %H:%M UTC')}). This shape expires from devices within 24 h.\n\n"
                        + (cf.get("remarks", "") or "").split("\n", 1)[-1]}
    text, res, err = mcp.call("update_map_object", {"operation_id": str(uuid.uuid4()), "id": ident,
                                                    "expected_revision": cur["revision"], "feature": ended})
    if err or "revision" not in res:
        raise RuntimeError(f"retire update failed: {text[:200]}")
    share(mcp, ident, res["revision"])
    mcp.call_confirmed("detach_map_object_from_mission", {
        "operation_id": str(uuid.uuid4()), "id": ident, "expected_revision": res["revision"], "mission_id": MISSION})


def main():
    t0 = time.monotonic()
    gauges, warnings = gauge_items(), warning_items()
    items = warnings + gauges
    state = load_state()
    queue = []
    for it in items:
        p = priority(it, state.get(it["id"]))
        if p is not None:
            queue.append((p, it))
    queue.sort(key=lambda x: x[0])
    active = {w["id"] for w in warnings}
    ended = [i for i, v in state.items() if v.get("kind") == "warning" and v.get("active") and i not in active]
    print(f"EA: {len(gauges)} gauges, {len(warnings)} warning shapes; {len(queue)} to send, {len(ended)} to retire")
    if DRY:
        for p, it in queue[:8]:
            print(" ", p, it["feature"]["kind"], "|", it["feature"]["name"])
        return
    if not KEY:
        raise SystemExit("TAKOS_MCP_KEY is not set")
    mcp = MCP(URL, KEY)
    mcp.start()
    stats = {"created": 0, "updated": 0, "resent": 0, "retired": 0, "failed": 0, "deferred": 0}
    for ident in ended:
        if time.monotonic() - t0 > TIME_BUDGET_S:
            stats["deferred"] += 1
            continue
        try:
            retire(mcp, ident, state[ident])
            state[ident]["active"] = False
            stats["retired"] += 1
            save_state(state)
        except Exception as exc:
            stats["failed"] += 1
            print(f"FAILED retiring {ident}: {exc}", file=sys.stderr)
    for p, it in queue:
        if time.monotonic() - t0 > TIME_BUDGET_S:
            stats["deferred"] += 1
            continue
        try:
            action = publish_item(mcp, it)
            stats[action] += 1
            rec = dict(it["meta"])
            rec.update({"sent": ea.now_utc().isoformat(), "active": True})
            state[it["id"]] = rec
            save_state(state)
        except Exception as exc:
            stats["failed"] += 1
            print(f"FAILED {it['feature']['name']}: {exc}", file=sys.stderr)
    save_state(state)
    print(json.dumps(stats))
    attempted = len(queue) + len(ended) - stats["deferred"]
    if attempted and stats["failed"] > attempted // 2:
        sys.exit(1)


if __name__ == "__main__":
    main()
