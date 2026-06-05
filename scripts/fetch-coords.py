#!/usr/bin/env python3
"""
Fetches MTA GTFS static data and extracts station coordinates.
Outputs public/station-coords.js keyed by parent station ID (matching station-data.js).
Run: python3 scripts/fetch-coords.py
"""
import csv
import io
import json
import sys
import urllib.request
import zipfile

GTFS_URL = "http://web.mta.info/developers/data/nyct/subway/google_transit.zip"

print("Fetching MTA GTFS static data...", file=sys.stderr)
try:
    with urllib.request.urlopen(GTFS_URL) as resp:
        data = resp.read()
except Exception as e:
    print(f"Error fetching GTFS data: {e}", file=sys.stderr)
    sys.exit(1)

print(f"Downloaded {len(data) // 1024}KB, extracting stops.txt...", file=sys.stderr)

with zipfile.ZipFile(io.BytesIO(data)) as z:
    stops_csv = z.read("stops.txt").decode("utf-8")

coords = {}
reader = csv.DictReader(stops_csv.splitlines())
for row in reader:
    if row.get("location_type") == "1":
        coords[row["stop_id"]] = [round(float(row["stop_lat"]), 6), round(float(row["stop_lon"]), 6)]

print(f"Found {len(coords)} parent stations.", file=sys.stderr)

out = (
    "const stationCoords = "
    + json.dumps(coords, separators=(",", ":"))
    + ";\n"
    + "if (typeof module !== 'undefined') module.exports = stationCoords;\n"
    + "else window.stationCoords = stationCoords;\n"
)

out_path = "public/station-coords.js"
with open(out_path, "w") as f:
    f.write(out)

print(f"Wrote {out_path}", file=sys.stderr)
