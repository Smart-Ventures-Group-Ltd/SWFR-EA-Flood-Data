#!/usr/bin/env python3
"""Convert Environment Agency real-time flood-monitoring data into KML for TAK.

Outputs (in OUT_DIR):
  flood-warnings.kml  active Flood Alerts / Warnings / Severe Warnings as polygons
  river-levels.kml    river level gauges coloured against their typical range
  rainfall.kml        rainfall gauges with the latest 15-minute total

Data: Environment Agency real-time flood-monitoring API (Beta), Open Government
Licence v3.0. Not a safety-critical service and not a replacement for official
flood warning channels.
"""
import datetime as dt
import json
import math
import os
import sys
import time
import urllib.request
from xml.sax.saxutils import escape

API = "https://environment.data.gov.uk/flood-monitoring"
LAT = float(os.environ.get("CENTRE_LAT", "52.192"))
LON = float(os.environ.get("CENTRE_LON", "-2.22"))
DIST_KM = float(os.environ.get("RADIUS_KM", "35"))
OUT_DIR = os.environ.get("OUT_DIR", "public")
STALE_MINUTES = 120
ATTRIB = ("Contains Environment Agency real-time flood-monitoring data (Beta), "
          "Open Government Licence v3.0. Not for safety-critical use; check "
          "official EA warnings.")

SEVERITY = {
    1: ("Severe Flood Warning", "ff0000cc", "990000cc"),
    2: ("Flood Warning", "ff0000ff", "800000ff"),
    3: ("Flood Alert", "ff00a5ff", "6000a5ff"),
    4: ("Warning no longer in force", "ff808080", "30808080"),
}


def get(path, retries=3):
    url = path if path.startswith("http") else API + path
    url = url.replace("http://", "https://", 1)
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "SWFR-TAK-flood-feed/1.0"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except Exception as exc:
            if attempt == retries - 1:
                raise RuntimeError(f"{url}: {exc}") from exc
            time.sleep(3 * (attempt + 1))


def now_utc():
    return dt.datetime.now(dt.timezone.utc)


def stamp():
    return now_utc().strftime("%d %b %Y %H:%M UTC")


def as_list(v):
    if v is None:
        return []
    return v if isinstance(v, list) else [v]


def doc(name, styles, body):
    return ('<?xml version="1.0" encoding="UTF-8"?>\n'
            '<kml xmlns="http://www.opengis.net/kml/2.2"><Document>'
            f"<name>{escape(name)}</name><description>{escape(ATTRIB)}</description>"
            f"{styles}{body}</Document></kml>\n")


def style(sid, icon_colour, poly_colour=None, scale=1.0):
    s = (f'<Style id="{sid}"><IconStyle><color>{icon_colour}</color><scale>{scale}</scale>'
         '<Icon><href>http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png</href></Icon>'
         f"</IconStyle><LineStyle><color>{icon_colour}</color><width>2</width></LineStyle>")
    if poly_colour:
        s += f"<PolyStyle><color>{poly_colour}</color></PolyStyle>"
    return s + "</Style>"


def point(name, sid, lon, lat, desc):
    return (f"<Placemark><name>{escape(name)}</name><styleUrl>#{sid}</styleUrl>"
            f"<description><![CDATA[{desc}]]></description>"
            f"<Point><coordinates>{lon:.5f},{lat:.5f},0</coordinates></Point></Placemark>")


def ring(coords):
    return " ".join(f"{c[0]:.5f},{c[1]:.5f},0" for c in coords)


def geom_kml(geometry):
    t = geometry.get("type")
    polys = [geometry["coordinates"]] if t == "Polygon" else geometry.get("coordinates", []) if t == "MultiPolygon" else []
    out = []
    for p in polys:
        inner = "".join(f"<innerBoundaryIs><LinearRing><coordinates>{ring(r)}</coordinates></LinearRing></innerBoundaryIs>" for r in p[1:])
        out.append(f"<Polygon><outerBoundaryIs><LinearRing><coordinates>{ring(p[0])}</coordinates></LinearRing></outerBoundaryIs>{inner}</Polygon>")
    return "<MultiGeometry>" + "".join(out) + "</MultiGeometry>" if out else ""


def warnings_kml():
    items = get(f"/id/floods?lat={LAT}&long={LON}&dist={DIST_KM:g}").get("items", [])
    items.sort(key=lambda i: i.get("severityLevel", 9))
    styles = "".join(style(f"sev{k}", v[1], v[2]) for k, v in SEVERITY.items())
    styles += style("ok", "ff00b000")
    body = []
    for it in items:
        lvl = it.get("severityLevel", 4)
        label = SEVERITY.get(lvl, SEVERITY[4])[0]
        area = it.get("floodArea", {})
        desc = (f"<b>{escape(label)}</b><br/>{escape(it.get('description', ''))}<br/>"
                f"Area code: {escape(it.get('floodAreaID', ''))}<br/>"
                f"River/sea: {escape(str(area.get('riverOrSea', '')))}<br/>"
                f"Message changed: {escape(str(it.get('timeMessageChanged', '')))}<br/>"
                f"Severity changed: {escape(str(it.get('timeSeverityChanged', '')))}<br/><br/>"
                f"{escape(it.get('message', '') or '')}<br/><br/><i>{escape(ATTRIB)}</i>")
        geo = ""
        if area.get("polygon"):
            try:
                gj = get(area["polygon"])
                feats = gj.get("features", [gj]) if isinstance(gj, dict) else []
                geo = "".join(geom_kml(f.get("geometry", f)) for f in feats)
            except Exception as exc:
                print(f"polygon failed for {it.get('floodAreaID')}: {exc}", file=sys.stderr)
        name = f"{label}: {it.get('description', '')}"
        if geo:
            body.append(f"<Placemark><name>{escape(name)}</name><styleUrl>#sev{lvl}</styleUrl>"
                        f"<description><![CDATA[{desc}]]></description>{geo}</Placemark>")
    active = [i for i in items if i.get("severityLevel", 4) <= 3]
    status = (f"No active EA flood alerts or warnings within {DIST_KM:g} km at {stamp()}"
              if not active else f"{len(active)} active EA alert(s)/warning(s) within {DIST_KM:g} km at {stamp()}")
    body.insert(0, point("EA feed status", "ok", LON, LAT, escape(status) + "<br/><i>" + escape(ATTRIB) + "</i>"))
    return doc(f"EA Flood Warnings ({stamp()})", styles, "".join(body)), len(active)


def latest_by_measure(parameter):
    items = get(f"/data/readings?latest&parameter={parameter}").get("items", [])
    return {r["measure"].replace("http://", "https://", 1): r for r in items if "measure" in r}


def parse_time(s):
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def levels_kml():
    stations = get(f"/id/stations?parameter=level&lat={LAT}&long={LON}&dist={DIST_KM:g}&_view=full").get("items", [])
    latest = latest_by_measure("level")
    styles = (style("high", "ff0000ff", scale=1.1) + style("normal", "ffff8000") +
              style("low", "ff00c8ff") + style("stale", "ff808080", scale=0.8))
    body, count = [], 0
    for s in stations:
        lat, lon = s.get("lat"), s.get("long")
        if isinstance(lat, list) or lat is None or lon is None:
            continue
        reading, unit = None, ""
        for m in as_list(s.get("measures")):
            if m.get("parameter") != "level":
                continue
            r = latest.get(str(m.get("@id", "")).replace("http://", "https://", 1))
            if r and (reading is None or m.get("qualifier") == "Stage"):
                reading, unit = r, m.get("unitName", "")
        scale = s.get("stageScale") if isinstance(s.get("stageScale"), dict) else {}
        lo, hi = scale.get("typicalRangeLow"), scale.get("typicalRangeHigh")
        rec = (scale.get("highestRecent") or {}).get("value")
        sid, state = "stale", "No recent reading"
        if reading:
            v = reading.get("value")
            v = v[0] if isinstance(v, list) else v
            t = parse_time(reading.get("dateTime", ""))
            if t and (now_utc() - t).total_seconds() <= STALE_MINUTES * 60 and isinstance(v, (int, float)):
                if hi is not None and v > hi:
                    sid, state = "high", "Above typical range"
                elif lo is not None and v < lo:
                    sid, state = "low", "Below typical range"
                else:
                    sid, state = "normal", "Within typical range" if hi is not None else "Reading (no typical range published)"
            vtxt = f"{v} {unit}" if v is not None else "n/a"
            when = reading.get("dateTime", "")
        else:
            vtxt, when = "n/a", ""
        name = f"{s.get('label', s.get('stationReference', 'Gauge'))}"
        river = s.get("riverName", "")
        desc = (f"<b>{escape(state)}</b><br/>River: {escape(str(river))}<br/>Level: {escape(vtxt)}<br/>"
                f"Reading time: {escape(when)}<br/>Typical range: {lo} to {hi}<br/>Highest recent: {rec}<br/>"
                f"Station: {escape(s.get('stationReference', ''))}<br/>"
                f"<a href=\"https://check-for-flooding.service.gov.uk/station/{escape(str(s.get('RLOIid', '')))}\">Check for flooding</a>"
                f"<br/><i>{escape(ATTRIB)}</i>")
        body.append(point(name, sid, float(lon), float(lat), desc))
        count += 1
    return doc(f"EA River Levels ({stamp()})", styles, "".join(body)), count


def rainfall_kml():
    stations = get(f"/id/stations?parameter=rainfall&lat={LAT}&long={LON}&dist={DIST_KM:g}").get("items", [])
    latest = latest_by_measure("rainfall")
    styles = style("rain", "ffffff00") + style("rainheavy", "ffff00ff", scale=1.1) + style("stale", "ff808080", scale=0.8)
    body = []
    for s in stations:
        lat, lon = s.get("lat"), s.get("long")
        if isinstance(lat, list) or lat is None or lon is None:
            continue
        sref = s.get("stationReference", "")
        r = next((v for k, v in latest.items() if f"/{sref}-rainfall" in k), None)
        sid, vtxt, when = "stale", "n/a", ""
        if r:
            t = parse_time(r.get("dateTime", ""))
            val = r.get("value")
            if t and (now_utc() - t).total_seconds() <= STALE_MINUTES * 60 and isinstance(val, (int, float)):
                sid = "rainheavy" if val >= 2 else "rain"
            vtxt, when = f"{val} mm in 15 min", r.get("dateTime", "")
        desc = f"Rainfall: {escape(vtxt)}<br/>Reading time: {escape(when)}<br/>Station: {escape(sref)}<br/><i>{escape(ATTRIB)}</i>"
        body.append(point(f"Rain gauge {s.get('label', sref)}", sid, float(lon), float(lat), desc))
    return doc(f"EA Rainfall ({stamp()})", styles, "".join(body)), len(body)


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    results, failed = {}, []
    for fname, fn in (("flood-warnings.kml", warnings_kml), ("river-levels.kml", levels_kml), ("rainfall.kml", rainfall_kml)):
        try:
            kml, n = fn()
            tmp = os.path.join(OUT_DIR, fname + ".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(kml)
            os.replace(tmp, os.path.join(OUT_DIR, fname))
            results[fname] = n
            print(f"{fname}: {n} items, {len(kml.encode()) / 1024:.0f} KB")
        except Exception as exc:
            failed.append(fname)
            print(f"{fname} FAILED: {exc}", file=sys.stderr)
    with open(os.path.join(OUT_DIR, "status.json"), "w") as f:
        json.dump({"generated": now_utc().isoformat(), "centre": [LAT, LON], "radius_km": DIST_KM,
                   "counts": results, "failed": failed}, f, indent=1)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
