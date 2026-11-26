# SWFR EA flood feeds for TAK

Every 15 minutes this repo turns Environment Agency real-time flood-monitoring data into KML and publishes it on GitHub Pages so GoTAK Overwatch, ATAK, iTAK and WinTAK can load it as network links.

| File | Content |
|---|---|
| `flood-warnings.kml` | Active Flood Alerts, Flood Warnings and Severe Flood Warnings drawn as polygons, plus a status point showing the time of the last check |
| `river-levels.kml` | River level gauges: red when above the typical range, blue when within it, yellow when below it, grey when there is no reading in the last 2 hours |
| `rainfall.kml` | Rain gauges with the latest 15-minute total, shown in magenta at 2 mm or more |
| `status.json` | When the feed was generated, item counts and any failures |

The default area is a 35 km radius around Worcester (52.192, -2.22). You can change it in `.github/workflows/flood-feeds.yml`.

## Setup

Repository: https://github.com/Smart-Ventures-Group-Ltd/SWFR-EA-Flood-Data (Pages source: GitHub Actions)

Feed URLs:
- https://smart-ventures-group-ltd.github.io/SWFR-EA-Flood-Data/flood-warnings.kml
- https://smart-ventures-group-ltd.github.io/SWFR-EA-Flood-Data/river-levels.kml
- https://smart-ventures-group-ltd.github.io/SWFR-EA-Flood-Data/rainfall.kml
- https://smart-ventures-group-ltd.github.io/SWFR-EA-Flood-Data/status.json

## Limitations

- GitHub's scheduled runs are best effort and are often 5 to 20 minutes late. Check the time shown in each layer's name.
- The EA API is in beta, has no service-level guarantee and is **not** for safety-critical use. Official EA warnings remain the authoritative source.
- Attribution: Contains Environment Agency data, Open Government Licence v3.0.

## Run locally

```
OUT_DIR=public python3 ea_flood_kml.py
```
