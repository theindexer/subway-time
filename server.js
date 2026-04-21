const express = require("express");
const compression = require("compression");
const protobuf = require("protobufjs");
const path = require("path");

const stationData = require("./public/station-data");

const app = express();
app.use(compression());
const PORT = process.env.PORT || 3000;

// --- Protobuf setup ---

const PROTO_DEFINITION = `
  syntax = "proto2";
  package transit_realtime;
  message FeedMessage {
    required FeedHeader header = 1;
    repeated FeedEntity entity = 2;
  }
  message FeedHeader {
    required string gtfs_realtime_version = 1;
    optional uint64 timestamp = 2;
  }
  message FeedEntity {
    required string id = 1;
    optional TripUpdate trip_update = 3;
    optional Alert alert = 5;
  }
  message TripUpdate {
    optional TripDescriptor trip = 1;
    repeated StopTimeUpdate stop_time_update = 2;
  }
  message TripDescriptor {
    optional string trip_id = 1;
    optional string route_id = 5;
  }
  message StopTimeUpdate {
    optional string stop_id = 4;
    optional StopTimeEvent arrival = 2;
  }
  message StopTimeEvent {
    optional int64 time = 2;
  }
  message TimeRange {
    optional uint64 start = 1;
    optional uint64 end = 2;
  }
  message Alert {
    repeated TimeRange active_period = 1;
    repeated EntitySelector informed_entity = 5;
    optional int32 effect = 7;
    optional TranslatedString header_text = 10;
  }
  message EntitySelector {
    optional string route_id = 2;
    optional string stop_id = 4;
  }
  message TranslatedString {
    message Translation {
      required string text = 1;
      optional string language = 2;
    }
    repeated Translation translation = 1;
  }
`;

const root = protobuf.parse(PROTO_DEFINITION).root;
const FeedMessage = root.lookupType("transit_realtime.FeedMessage");

// --- MTA feed URLs ---

const ALERTS_URL =
  "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/camsys%2Fsubway-alerts";

const FEED_URLS = [
  "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
  "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
  "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
  "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
  "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
  "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
  "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
  "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
];

// --- In-memory cache ---
// arrivalsCache shape: { "D26N": [ { route: "B", time: 1713500000 }, ... ], ... }
// alertsCache shape:   { "B": ["No B trains between..."], ... }
let arrivalsCache = {};
let alertsCache = {};
let lastFetchTime = null;

async function fetchFeed(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`HTTP ${response.status} from ${url}`);
  const buffer = await response.arrayBuffer();
  return FeedMessage.decode(new Uint8Array(buffer));
}

function decodeFeedsToCache(feeds) {
  const cache = {};
  const now = Math.floor(Date.now() / 1000);

  for (const feed of feeds) {
    for (const entity of feed.entity) {
      if (!entity.tripUpdate) continue;
      const routeId = entity.tripUpdate.trip?.routeId;
      if (!routeId) continue;
      const route = routeId === "FS" ? "S" : routeId;

      for (const stu of entity.tripUpdate.stopTimeUpdate || []) {
        const stopId = stu.stopId;
        if (!stopId) continue;
        const arrivalTime = stu.arrival?.time;
        if (!arrivalTime) continue;

        const time = Number(arrivalTime);
        // Only include arrivals in the next 90 minutes
        if (time < now || time > now + 90 * 60) continue;

        if (!cache[stopId]) cache[stopId] = [];
        cache[stopId].push({ route, time });
      }
    }
  }

  // Sort each stop's arrivals by time
  for (const stopId of Object.keys(cache)) {
    cache[stopId].sort((a, b) => a.time - b.time);
  }

  return cache;
}

const IGNORED_EFFECTS = new Set([5, 10]); // ADDITIONAL_SERVICE, NO_EFFECT (informational)
const IGNORED_ALERT_IDS = new Set([
  "lmm:planned_work:20534", // B: "Take the [A][C][D][Q] instead" — standing no-service notice
  "lmm:planned_work:19872", // Z: "Take the [J] instead" — standing no-service notice
]);

function isActivePeriod(periods) {
  if (!periods?.length) return true; // no period = always active
  const now = Math.floor(Date.now() / 1000);
  return periods.some(p => {
    const start = Number(p.start) || 0;
    const end = Number(p.end) || Infinity;
    return now >= start && now <= end;
  });
}

function decodeAlerts(feed) {
  const alerts = {};
  for (const entity of feed.entity) {
    if (!entity.alert) continue;
    if (IGNORED_ALERT_IDS.has(entity.id)) continue;
    if (!isActivePeriod(entity.alert.activePeriod)) continue;
    if (IGNORED_EFFECTS.has(entity.alert.effect)) continue;
    const header = entity.alert.headerText?.translation?.[0]?.text;
    if (!header) continue;
    const now = Math.floor(Date.now() / 1000);
    const activePeriod = entity.alert.activePeriod.find(p => now <= (Number(p.end) || Infinity));
    const start = activePeriod?.start ? Number(activePeriod.start) : null;
    for (const sel of entity.alert.informedEntity || []) {
      if (!sel.routeId) continue;
      const route = sel.routeId === "FS" ? "S" : sel.routeId;
      if (!alerts[route]) alerts[route] = [];
      if (!alerts[route].find(a => a.text === header)) alerts[route].push({ text: header, start });
    }
  }
  return alerts;
}

async function refreshArrivals() {
  try {
    const feeds = await Promise.all(FEED_URLS.map((url) => fetchFeed(url)));
    arrivalsCache = decodeFeedsToCache(feeds);
    lastFetchTime = new Date();
    console.log(`[${lastFetchTime.toISOString()}] Arrivals refreshed — ${Object.keys(arrivalsCache).length} stops`);
  } catch (err) {
    console.error("Failed to refresh arrivals:", err.message);
  }
}

async function refreshAlerts() {
  try {
    const feed = await fetchFeed(ALERTS_URL);
    alertsCache = decodeAlerts(feed);
    console.log(`[${new Date().toISOString()}] Alerts refreshed — ${Object.keys(alertsCache).length} routes with alerts`);
  } catch (err) {
    console.error("Failed to refresh alerts:", err.message);
  }
}

// --- API ---

function getStopBase(stopId) {
  const lastChar = stopId.slice(-1);
  if (lastChar === "N" || lastChar === "S") return stopId.slice(0, -1);
  return stopId;
}

app.get("/api/arrivals", (req, res) => {
  const stationsParam = req.query.stations;
  if (!stationsParam) {
    return res.status(400).json({ error: "stations parameter required" });
  }

  const stops = stationsParam.split(",").map((s) => {
    const [id, lineStr] = s.trim().split(":");
    const lines = lineStr ? lineStr.split(";") : null;
    return { id, lines };
  });
  const arrivals = {};
  const stations = {};
  const relevantRoutes = new Set();
  for (const { id, lines } of stops) {
    const raw = arrivalsCache[id] || [];
    arrivals[id] = lines ? raw.filter(a => lines.includes(a.route)) : raw;
    const base = getStopBase(id);
    if (!stations[base]) stations[base] = { name: stationData[base]?.[0] || null, routes: stationData[base]?.[1] || [] };
    const stationRoutes = stationData[base]?.[1] || [];
    for (const route of (lines ? stationRoutes.filter(r => lines.includes(r)) : stationRoutes)) {
      relevantRoutes.add(route);
    }
  }

  const alerts = {};
  for (const route of relevantRoutes) {
    if (alertsCache[route]?.length) alerts[route] = alertsCache[route];
  }

  res.set('Cache-Control', 'no-store');
  res.json({ arrivals, stations, alerts, lastFetchTime: lastFetchTime?.toISOString() || null });
});

// --- Static files ---

app.use(express.static(path.join(__dirname, "public")));

// --- Start ---

refreshArrivals();
refreshAlerts();
setInterval(refreshArrivals, 60_000);
setInterval(refreshAlerts, 3 * 60_000);

app.listen(PORT, () => {
  console.log(`Subway Time server listening on port ${PORT}`);
});
