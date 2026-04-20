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
`;

const root = protobuf.parse(PROTO_DEFINITION).root;
const FeedMessage = root.lookupType("transit_realtime.FeedMessage");

// --- MTA feed URLs ---

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
// Shape: { "D26N": [ { route: "B", time: 1713500000 }, ... ], ... }
let arrivalsCache = {};
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

async function refreshCache() {
  try {
    const feeds = await Promise.all(FEED_URLS.map((url) => fetchFeed(url)));
    arrivalsCache = decodeFeedsToCache(feeds);
    lastFetchTime = new Date();
    console.log(
      `[${lastFetchTime.toISOString()}] Cache refreshed — ${Object.keys(arrivalsCache).length} stops`
    );
  } catch (err) {
    console.error("Failed to refresh cache:", err.message);
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

  const stopIds = stationsParam.split(",").map((s) => s.trim());
  const arrivals = {};
  const names = {};
  for (const id of stopIds) {
    arrivals[id] = arrivalsCache[id] || [];
    const base = getStopBase(id);
    if (!names[base]) names[base] = stationData[base]?.[0] || null;
  }

  res.json({ arrivals, names, lastFetchTime: lastFetchTime?.toISOString() || null });
});

// --- Static files ---

app.use(express.static(path.join(__dirname, "public")));

// --- Start ---

refreshCache();
setInterval(refreshCache, 60_000);

app.listen(PORT, () => {
  console.log(`Subway Time server listening on port ${PORT}`);
});
