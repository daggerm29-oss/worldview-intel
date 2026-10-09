/* =====================================================================
   WORLDVIEW ENGINE v2.2  (Render free tier + Supabase free tier)
   ---------------------------------------------------------------------
   v2.2 (on top of v2.1): GLOBAL AISStream by default (ONE connection, one key),
   permessage-deflate on + checked, gentler wait after HTTP 429, hard guarantee
   that this process never holds two AIS sockets, /api/radar + /api/live accept
   ?bbox=minLat,minLon,maxLat,maxLon and the radar cache is longer in global mode.
   ---------------------------------------------------------------------
   What is new compared to v2:
   1. AIS FEED HEALTH  - the dark-fleet detector now knows when OUR data
                  feed is broken. While AISStream is down, nobody can be
                  "dark". After it comes back, every ship gets a fresh
                  clock. Also: event-loop lag guard, "mass silence" guard,
                  neighbour test (was anybody else heard nearby?).
   2. AIS SOCKET - proper backoff (the counter is only reset after a
                  stable connection), longer wait after HTTP 429, one
                  reconnect timer only, stale-socket guard, smaller
                  subscription box (v2.2: global is now the default; set
                  AIS_REGIONAL=1 for the small box), clean close on SIGTERM so a new deploy
                  does not collide with the old instance.
   3. CRASH-PROOFING - every background job is wrapped, unhandled promise
                  rejections are logged instead of killing the process.
   4. SMALL FIXES - SOG 102.3 ("not available") is no longer "sailing",
                  stale MarineTraffic rows are pruned from RAM even when
                  scraping fails, adsb.lol uses Retry-After, longer cache,
                  plane frames with too many missing sources are skipped.
   Run supabase_setup.sql BEFORE deploying this file (same as v2).
   ===================================================================== */

require('dotenv').config();
const express = require('express');
const cors = require('cors');
const compression = require('compression');
const WebSocket = require('ws');
const { createClient } = require('@supabase/supabase-js');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');

puppeteer.use(StealthPlugin());

// A stray rejected promise must not kill the whole radar. Log it and keep running.
process.on('unhandledRejection', (r) => console.error('[UNHANDLED REJECTION]', r && r.message ? r.message : r));
process.on('uncaughtException', (e) => console.error('[UNCAUGHT EXCEPTION]', e && e.message ? e.message : e));

// =====================================================================
// 0. SETTINGS (change numbers here, nowhere else)
// =====================================================================
const PORT = process.env.PORT || 3000;
const AIS_API_KEY = process.env.AISSTREAM_API_KEY;
// Move your old hard-coded "vessel-image" token into this Render env variable
const MT_VESSEL_IMAGE_TOKEN = process.env.MT_VESSEL_IMAGE_TOKEN || '';
// Global AISStream coverage is the default. AIS_REGIONAL=1 -> only the Red Sea..Bay of Bengal box (CFG.AIS_BOX), much lighter.
const AIS_GLOBAL = process.env.AIS_REGIONAL !== '1';
// BLOCK_HEAVY_RESOURCES=1 -> scraper browser skips images/fonts/media (less CPU + RAM). Test before relying on it.
const BLOCK_HEAVY_RESOURCES = process.env.BLOCK_HEAVY_RESOURCES === '1';

if (!process.env.SUPABASE_URL || !process.env.SUPABASE_KEY) {
    console.error('Missing SUPABASE_URL or SUPABASE_KEY in environment variables.');
    process.exit(1);
}
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

const MIN = 60 * 1000;
const HOUR = 60 * MIN;
const DAY = 24 * HOUR;

const CFG = {
    // dark fleet
    DARK_THRESHOLD_MS: 5 * MIN,       // silent for 5 min = "pending" (RAM only, nothing saved) ...
    DARK_CONFIRM_MS: 30 * MIN,        // ... still silent after 30 min = confirmed, saved to the DB
    DARK_MIN_SOG: 3,                  // only ships that were really sailing (>= 3 knots), not anchored/jittering
    MIN_REAL_MOVE_NM: 2,              // came back < 2 nm away = just a coverage gap, not "dark"
    REAPPEAR_MIN_MINUTES: 5,          // log every reappearance (v1 ignored 5-10 min)
    DARK_MEMORY_MS: 72 * HOUR,        // remember dark ships in RAM for 72 h
    DARK_MIN_NEIGHBOURS: 2,           // at confirmation, at least this many OTHER ships must have been heard nearby (0 = off)
    DARK_NEIGHBOUR_RADIUS_NM: 30,     // "nearby" = within 30 nm of the last known position
    DARK_NEIGHBOUR_FRESH_MS: 2 * MIN, // a neighbour counts only if it reported in the last 2 min
    DARK_TANKERS_ONLY: false,         // true = only ships MarineTraffic already tagged as TANKER can become "dark"

    // AIS feed health (these protect the dark fleet detector from OUR OWN outages)
    AIS_BOX: [[-15, 30], [36, 100]],  // lat/lon corners we subscribe to (a bit bigger than isHighRiskZone on purpose)
    FEED_DOWN_AFTER_MS: 45 * 1000,    // no position report for 45 s = feed is considered DOWN
    AIS_SILENT_RESTART_MS: 90 * 1000, // socket open but no data for 90 s = restart it
    AIS_STABLE_MS: 60 * 1000,         // a connection that lived >= 60 s counts as stable (resets the backoff)
    LOOP_LAG_MS: 15 * 1000,           // node was blocked > 15 s (CPU starved) = restart the dark-fleet clock
    MASS_SILENCE_MIN: 20,             // 20+ ships going silent in one check ...
    MASS_SILENCE_FRACTION: 0.25,      // ... and more than 25% of all sailing ships = feed/coverage problem, not dark ships

    // scraper
    SWEEP_EVERY_MS: (Number(process.env.SWEEP_INTERVAL_MIN) || 5) * MIN,
    STALE_INTEL_MS: 1 * HOUR,         // MarineTraffic rows older than this are deleted
    // playback
    SHIP_FRAME_EVERY_MS: 15 * MIN,
    SHIP_FRAME_KEEP_H: 48,
    PLANE_FRAME_EVERY_MS: 5 * MIN,
    PLANE_FRAME_KEEP_H: 24,
    FRAME_MAX_AGE_MS: 20 * MIN,       // ignore ship positions older than this when taking a frame
    // adsb.lol
    ADSB_GAP_MS: 8000,                // pause between area requests (adsb.lol rate-limits fast bursts)
    ADSB_CACHE_MS: 15 * MIN,          // reuse the last good answer for this long when adsb.lol says 429
    PLANE_MAX_MISSING: 1,             // skip a plane frame if MORE than this many sources failed completely
    // retention of the other tables (days)
    DARK_KEEP_DAYS: 30,
    OIL_HISTORY_KEEP_DAYS: 90,
    ARRIVALS_KEEP_DAYS: 365
};

// ---- Oil math settings ------------------------------------------------
const OIL = {
    BARRELS_PER_TONNE: 7.33,          // average crude (OK as a constant, real range ~6.9-7.9)
    MIN_CRUDE_DWT: 60000,             // smaller tankers are mostly product/chemical tankers
    LOAD_FACTOR: 0.90,                // assume 90% of DWT is cargo when the ship arrives full
    PORT_RADIUS_NM: 30,               // "arrived" = within 30 nautical miles of an Indian port
    REARRIVAL_BLOCK_MS: 5 * DAY       // the same ship is not counted again for 5 days
};

// ---- Playback areas ---------------------------------------------------
const PLAYBACK_REGIONS = {
    INDIA:  { latMin: 5,  latMax: 26, lonMin: 66, lonMax: 90 },
    HORMUZ: { latMin: 22, latMax: 31, lonMin: 47, lonMax: 62 }
};
// Planes: ALL military worldwide + any aircraft inside these circles (radius max 250 nm)
const PLANE_AREAS = [
    { name: 'HORMUZ',      lat: 26.0, lon: 56.0, radius: 250 },
    { name: 'INDIA_WEST',  lat: 19.0, lon: 73.0, radius: 250 },
    { name: 'INDIA_NORTH', lat: 28.5, lon: 77.0, radius: 250 },
    { name: 'INDIA_SOUTH', lat: 12.5, lon: 78.5, radius: 250 }
];
const SHIP_FIELDS = ['mmsi', 'lat', 'lon', 'sog', 'cog', 'cat'];                    // cat: 0 other, 1 tanker, 2 military
const PLANE_FIELDS = ['hex', 'lat', 'lon', 'alt_ft', 'gs_kt', 'track', 'callsign', 'mil'];

// Approximate port / terminal positions used to detect "tanker arrived in India"
const INDIA_PORTS = [
    { name: 'Mundra',          lat: 22.74, lon: 69.70 },
    { name: 'Sikka/Jamnagar',  lat: 22.43, lon: 69.84 },
    { name: 'Vadinar',         lat: 22.45, lon: 69.70 },
    { name: 'Kandla',          lat: 23.00, lon: 70.22 },
    { name: 'Mumbai/JNPT',     lat: 18.95, lon: 72.90 },
    { name: 'Hazira',          lat: 21.10, lon: 72.65 },
    { name: 'Dahej',           lat: 21.70, lon: 72.55 },
    { name: 'Pipavav',         lat: 20.92, lon: 71.52 },
    { name: 'Mormugao',        lat: 15.41, lon: 73.80 },
    { name: 'New Mangalore',   lat: 12.93, lon: 74.80 },
    { name: 'Cochin',          lat: 9.97,  lon: 76.27 },
    { name: 'Tuticorin',       lat: 8.75,  lon: 78.20 },
    { name: 'Chennai/Ennore',  lat: 13.15, lon: 80.30 },
    { name: 'Visakhapatnam',   lat: 17.69, lon: 83.30 },
    { name: 'Paradip',         lat: 20.27, lon: 86.68 },
    { name: 'Dhamra',          lat: 20.78, lon: 86.97 },
    { name: 'Haldia',          lat: 22.03, lon: 88.08 }
];

// Words that mean "this ship's destination is India"
const INDIA_WORDS = [
    'INDIA', 'MUNDRA', 'SIKKA', 'JAMNAGAR', 'VADINAR', 'KANDLA', 'DEENDAYAL', 'PARADIP', 'HALDIA',
    'KOLKATA', 'VISAKHAPATNAM', 'VIZAG', 'KAKINADA', 'CHENNAI', 'ENNORE', 'KAMARAJAR', 'TUTICORIN',
    'COCHIN', 'KOCHI', 'MANGALORE', 'MORMUGAO', 'GOA', 'JNPT', 'NHAVA', 'MUMBAI', 'HAZIRA', 'DAHEJ',
    'PIPAVAV', 'DHAMRA', 'NAVLAKHI', 'SALAYA', 'OKHA', 'NAGAPATTINAM', 'PORBANDAR', 'KRISHNAPATNAM',
    'GANGAVARAM'
];
const INDIA_REGEX = new RegExp('\\b(' + INDIA_WORDS.join('|') + ')\\b');

// =====================================================================
// 1. SMALL HELPERS
// =====================================================================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const round = (n, d) => { const f = Math.pow(10, d); return Math.round(n * f) / f; };
const hoursAgoISO = (h) => new Date(Date.now() - h * HOUR).toISOString();
const clampInt = (v, min, max, def) => {
    const n = parseInt(v, 10);
    if (!Number.isFinite(n)) return def;
    return Math.min(max, Math.max(min, n));
};
const inBox = (lat, lon, b) => lat >= b.latMin && lat <= b.latMax && lon >= b.lonMin && lon <= b.lonMax;

// Wrap a background job so one failure (e.g. a network error) can never become an unhandled rejection
const safeJob = (name, fn) => async () => {
    try { await fn(); } catch (e) { console.error(`[JOB ${name}]`, e && e.message ? e.message : e); }
};

function haversineNm(lat1, lon1, lat2, lon2) {
    const R = 3440.065; // earth radius in nautical miles
    const toRad = (d) => (d * Math.PI) / 180;
    const dLat = toRad(lat2 - lat1);
    const dLon = toRad(lon2 - lon1);
    const a = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
    return 2 * R * Math.asin(Math.sqrt(a));
}

// Calendar day in Indian Standard Time (UTC+5:30), as YYYY-MM-DD
function istDay(ms) {
    return new Date(ms + 5.5 * HOUR).toISOString().slice(0, 10);
}

function isHighRiskZone(lat, lon) {
    // Red Sea -> Persian Gulf -> Indian Subcontinent
    return lat >= -10 && lat <= 30 && lon >= 35 && lon <= 95;
}

function nearestIndianPort(lat, lon) {
    let best = null;
    for (const p of INDIA_PORTS) {
        const d = haversineNm(lat, lon, p.lat, p.lon);
        if (!best || d < best.distNm) best = { name: p.name, distNm: d };
    }
    return best;
}

function isIndiaDestination(dest) {
    if (!dest) return false;
    const d = String(dest).toUpperCase();
    // "FUJAIRAH > SIKKA" -> take the LAST part (final destination)
    const parts = d.split(/>|->|\s-\s|,|\||\//).map((p) => p.trim()).filter(Boolean);
    const last = parts.length ? parts[parts.length - 1] : d;
    if (/^IN ?[A-Z]{3}$/.test(last)) return true; // UN/LOCODE style: INMUN, IN SIK
    return INDIA_REGEX.test(last);
}

// =====================================================================
// 2. IN-MEMORY STATE  (RAM only, rebuilt after every restart)
// =====================================================================
const liveVessels = new Map();   // mmsi(number) -> live AIS position
const darkNow = new Map();       // CONFIRMED dark ships: mmsi(number) -> { t, lat, lon, sog, cog, name }
const darkPending = new Map();   // silent 5-30 min, not saved yet
const deepIntel = {              // MarineTraffic rows, kept in RAM so /api/radar never reads the DB
    india: new Map(),            // mmsi(string) -> row
    hormuz: new Map()
};
const recentArrivals = new Map(); // mmsi(string) -> last time seen inside an Indian port zone (ms)
let firstIndiaSweepDone = false;
let shuttingDown = false;
let server = null;
let currentBrowser = null;

function intelFor(mmsiStr) {
    return deepIntel.india.get(mmsiStr) || deepIntel.hormuz.get(mmsiStr) || null;
}
function catFromCategory(c) {
    return c === 'TANKER' ? 1 : c === 'MILITARY' ? 2 : 0;
}

// =====================================================================
// 3. AIS FEED HEALTH
// ---------------------------------------------------------------------
// The dark-fleet detector judges ships by "how long since we last heard
// them". That is only fair while OUR feed is healthy. If AISStream drops
// us, or node is starved of CPU, every ship looks silent. So:
//   - feed.lastMsgAt   = last position report received (any ship)
//   - feed.resumedAt   = start of the current "fresh clock": ship silence
//                        is only counted from this moment on
// =====================================================================
const feed = {
    lastMsgAt: 0,
    resumedAt: 0,
    outages: 0,
    pausedLogged: false
};

function feedIsUp(now = Date.now()) {
    return feed.lastMsgAt > 0 && now - feed.lastMsgAt <= CFG.FEED_DOWN_AFTER_MS;
}

function noteFeedMessage(now) {
    if (feed.lastMsgAt === 0) {
        feed.resumedAt = now; // first data since boot
    } else if (now - feed.lastMsgAt > CFG.FEED_DOWN_AFTER_MS) {
        feed.resumedAt = now;
        feed.outages++;
        console.log(`[AIS] Data flowing again after ${Math.round((now - feed.lastMsgAt) / 1000)}s of silence - dark-fleet clock restarted.`);
    }
    feed.lastMsgAt = now;
}

// How long has this ship really been silent, ignoring time when OUR feed was broken?
const silentFor = (v, now) => now - Math.max(v.last_updated, feed.resumedAt);

// Event-loop lag guard: on a 0.1 CPU instance (Chrome + AIS + Express) node can freeze for a while.
// After a freeze, timers can run BEFORE the buffered AIS messages are read, so ships look older than they are.
let lastTick = Date.now();
setInterval(() => {
    const now = Date.now();
    const lag = now - lastTick - 1000;
    lastTick = now;
    if (lag > CFG.LOOP_LAG_MS) {
        feed.resumedAt = now;
        console.log(`[HEALTH] Event loop was blocked ~${Math.round(lag / 1000)}s (CPU starved?) - dark-fleet clock restarted.`);
    }
}, 1000);

// =====================================================================
// 4. AISSTREAM WORKER (live ships -> RAM)
// =====================================================================
let aisSocket = null;
let reconnectTimer = null;
let lastAisActivityAt = Date.now(); // any message at all (for the watchdog)
let aisFailures = 0;                // consecutive failed/unstable connections (for the growing wait)
let aisRateLimited = false;         // the last attempt ended with HTTP 429
const aisStats = { reconnects: 0, lastConnectedAt: 0, compression: null };

async function logReappearance(mmsi, name, dark, lat, lon) {
    const now = Date.now();
    const minutesOffline = Math.floor((now - dark.t) / MIN);
    if (minutesOffline < CFG.REAPPEAR_MIN_MINUTES) return;

    const distanceNm = round(haversineNm(dark.lat, dark.lon, lat, lon), 1);

    // Came back almost where it vanished = it was only a receiver/coverage gap. Remove the false alert.
    if (distanceNm < CFG.MIN_REAL_MOVE_NM) {
        console.log(`[DARK FLEET] ${name}: coverage gap (moved ${distanceNm} nm) - false alert removed`);
        const { error: delErr } = await supabase.from('dark_fleet').delete()
            .eq('mmsi', String(mmsi)).eq('time_went_dark', new Date(dark.t).toISOString());
        if (delErr) console.error('Failed to remove false alert:', delErr.message);
        return;
    }
    console.log(`[DARK FLEET] REAPPEARED: ${name} after ${minutesOffline} min, moved ${distanceNm} nm while dark`);

    const { error } = await supabase.from('dark_fleet_reappearances').upsert({
        mmsi: String(mmsi),
        ship_name: name,
        time_went_dark: new Date(dark.t).toISOString(),
        time_reappeared: new Date(now).toISOString(),
        minutes_offline: minutesOffline,
        last_known_lat: dark.lat,
        last_known_lon: dark.lon,
        reappear_lat: lat,
        reappear_lon: lon,
        distance_nm: distanceNm
    }, { onConflict: 'mmsi,time_went_dark', ignoreDuplicates: true });
    if (error) console.error('Failed to log reappearance:', error.message);
}

function scheduleReconnect(livedMs) {
    if (reconnectTimer || shuttingDown) return; // never more than ONE pending reconnect
    const step = Math.min(aisFailures, 4);
    // normal drops: 5, 10, 20, 40, 60 s.  HTTP 429: 60, 120, 240, 480, 600 s.
    // Hammering a 429 does not help, and one AISStream user reports that endless fast retries may
    // have got their whole account throttled, so after a 429 we stay quiet for a long time.
    let wait = aisRateLimited
        ? Math.min(600000, 60000 * Math.pow(2, step))
        : Math.min(60000, 5000 * Math.pow(2, step));
    wait += Math.floor(Math.random() * 2000); // small random jitter
    aisFailures++;
    aisStats.reconnects++;
    const lived = livedMs > 0 ? ` after ${Math.round(livedMs / 1000)}s` : '';
    console.log(`[AIS] Disconnected${lived}${aisRateLimited ? ' (rate limited, HTTP 429)' : ''}. Reconnecting in ${Math.round(wait / 1000)}s...`);
    reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connectAIS();
    }, wait);
}

function connectAIS() {
    if (shuttingDown) return;
    // AISStream allows only 3 subscribed connections per account and 3 open connections per IP
    // (checked before login). This process must NEVER hold more than one.
    if (aisSocket && (aisSocket.readyState === WebSocket.CONNECTING || aisSocket.readyState === WebSocket.OPEN)) return;
    console.log(`[AIS] Connecting to ${AIS_GLOBAL ? 'global' : 'regional'} feed...`);
    // permessage-deflate: uncompressed connections are bandwidth-limited by AISStream (since Sept 2026)
    const ws = new WebSocket('wss://stream.aisstream.io/v0/stream', { perMessageDeflate: true, handshakeTimeout: 15000 });
    aisSocket = ws;
    aisRateLimited = false;
    lastAisActivityAt = Date.now();
    let openedAt = 0;

    ws.on('open', () => {
        openedAt = Date.now();
        aisStats.lastConnectedAt = openedAt;
        lastAisActivityAt = openedAt;
        console.log('[AIS] Connected. Streaming live to RAM...');
        ws.send(JSON.stringify({
            APIKey: AIS_API_KEY,
            BoundingBoxes: AIS_GLOBAL ? [[[-90, -180], [90, 180]]] : [CFG.AIS_BOX],
            FilterMessageTypes: ['PositionReport']
        }));
    });

    ws.on('message', (data) => {
        const now = Date.now();
        lastAisActivityAt = now;
        // NOTE: v2 reset the backoff counter here, which made a connection that lived 3 seconds
        // look "healthy" and caused a tight reconnect loop. The reset now happens in 'close'.
        try {
            const msg = JSON.parse(data);
            if (msg.error) { console.error('[AIS] Server message:', msg.error); return; }
            if (msg.MessageType === 'SubscriptionConfirmation') {
                const on = !!(msg.Message && msg.Message.CompressionEnabled);
                aisStats.compression = on;
                console.log(`[AIS] Subscription confirmed. Compression: ${on ? 'ON' : 'OFF - uncompressed connections get bandwidth-limited and messages are dropped'}`);
                return;
            }
            if (msg.MessageType !== 'PositionReport') return;

            noteFeedMessage(now);

            const report = msg.Message.PositionReport;
            const meta = msg.MetaData;
            const mmsi = Number(meta.MMSI);
            const lat = report.Latitude;
            const lon = report.Longitude;
            if (!Number.isFinite(mmsi) || Math.abs(lat) > 90 || Math.abs(lon) > 180) return;
            if (lat === 0 && lon === 0) return; // classic "no GPS fix" garbage

            const name = meta.ShipName ? meta.ShipName.trim() : 'UNKNOWN';

            liveVessels.set(mmsi, {
                mmsi: mmsi,
                vessel_name: name,
                lat: lat,
                lon: lon,
                sog: report.Sog,
                cog: report.Cog,
                last_updated: now
            });

            // --- REAPPEARANCE DETECTOR ---
            darkPending.delete(mmsi); // came back before 30 min: normal gap, forget it
            const dark = darkNow.get(mmsi);
            if (dark) {
                darkNow.delete(mmsi);
                // not awaited on purpose, but a failure must not become an unhandled rejection
                logReappearance(mmsi, name, dark, lat, lon).catch((e) => console.error('[DARK FLEET] Reappearance error:', e.message));
            }
        } catch (e) { /* ignore bad message */ }
    });

    ws.on('error', (error) => {
        console.error(`[AIS CONNECTION ERROR] ${error.message}`);
        if (/429/.test(String(error.message))) aisRateLimited = true;
    });

    ws.on('close', () => {
        if (ws !== aisSocket) return; // an old socket closing late must not trigger a second reconnect
        aisSocket = null;
        if (shuttingDown) return;
        const lived = openedAt ? Date.now() - openedAt : 0;
        if (lived >= CFG.AIS_STABLE_MS) aisFailures = 0; // only a STABLE connection resets the backoff
        scheduleReconnect(lived);
    });
}

// Watchdog: if AISStream stops sending data without closing, restart the socket
setInterval(() => {
    if (aisSocket && aisSocket.readyState === WebSocket.OPEN && Date.now() - lastAisActivityAt > CFG.AIS_SILENT_RESTART_MS) {
        console.log(`[AIS] No data for ${CFG.AIS_SILENT_RESTART_MS / 1000}s - restarting connection`);
        lastAisActivityAt = Date.now();
        try { aisSocket.terminate(); } catch (e) { /* ignore */ }
    }
}, 30 * 1000);

// =====================================================================
// 5. DARK FLEET DETECTOR  (feed-aware)
// ---------------------------------------------------------------------
// Honest naming: this finds "AIS GAP CANDIDATES" - sailing ships that we
// stopped hearing for 30+ min while our feed was healthy and other ships
// nearby were still heard. It cannot prove that a transponder was switched
// off (receiver range, ships leaving the area and spoofing are invisible).
// =====================================================================
const isSailing = (v) => typeof v.sog === 'number' && v.sog >= CFG.DARK_MIN_SOG && v.sog < 102.3; // 102.3 = "not available"

// How many OTHER ships were heard recently around this position?
function countNeighbours(p, now) {
    const dLat = CFG.DARK_NEIGHBOUR_RADIUS_NM / 60; // 1 degree of latitude = 60 nm
    const dLon = dLat / Math.max(0.2, Math.cos((p.lat * Math.PI) / 180));
    let n = 0;
    for (const o of liveVessels.values()) {
        if (now - o.last_updated > CFG.DARK_NEIGHBOUR_FRESH_MS) continue;
        if (Math.abs(o.lat - p.lat) > dLat || Math.abs(o.lon - p.lon) > dLon) continue; // cheap pre-filter
        if (haversineNm(p.lat, p.lon, o.lat, o.lon) <= CFG.DARK_NEIGHBOUR_RADIUS_NM) n++;
    }
    return n;
}

let darkBusy = false;
async function detectDarkFleet() {
    if (darkBusy) return;
    darkBusy = true;
    try {
        const now = Date.now();

        // GUARD 1: our own feed is down -> nobody can be called dark right now
        if (!feedIsUp(now)) {
            if (!feed.pausedLogged) {
                console.log('[DARK FLEET] AIS feed is down - detection paused (no ship can be called dark while we are deaf).');
                feed.pausedLogged = true;
            }
            return;
        }
        feed.pausedLogged = false;

        // Step 1: sailing ships that went silent for 5+ min become "pending" (RAM only)
        let sailing = 0;
        const silent = [];
        for (const v of liveVessels.values()) {
            if (!isSailing(v) || !isHighRiskZone(v.lat, v.lon)) continue;
            sailing++;
            if (silentFor(v, now) > CFG.DARK_THRESHOLD_MS) silent.push(v);
        }

        // GUARD 2: "mass silence" - a big share of all ships going quiet at the same moment is a
        // feed / receiver problem, not a fleet of ships switching off AIS together.
        if (silent.length >= CFG.MASS_SILENCE_MIN && silent.length > sailing * CFG.MASS_SILENCE_FRACTION) {
            console.log(`[DARK FLEET] ${silent.length} of ${sailing} sailing ships went silent together - feed/coverage problem, not flagging them. Clock restarted.`);
            for (const v of silent) liveVessels.delete(v.mmsi); // forget them quietly; they come back with their next message
            feed.resumedAt = now;
            return;
        }

        for (const v of silent) {
            liveVessels.delete(v.mmsi);
            if (CFG.DARK_TANKERS_ONLY) {
                const intel = intelFor(String(v.mmsi));
                if (!intel || intel.category !== 'TANKER') continue;
            }
            if (!darkNow.has(v.mmsi) && !darkPending.has(v.mmsi)) {
                darkPending.set(v.mmsi, { t: v.last_updated, lat: v.lat, lon: v.lon, sog: v.sog, cog: v.cog, name: v.vessel_name });
            }
        }

        // Step 2: still silent after 30 min -> confirmed dark, save to Supabase
        for (const [mmsi, p] of darkPending) {
            // the 30 min are counted from the last moment OUR feed was healthy again
            if (now - Math.max(p.t, feed.resumedAt) < CFG.DARK_CONFIRM_MS) continue;
            darkPending.delete(mmsi);

            // GUARD 3: neighbour test - if nobody else near that spot is heard either, it is a coverage hole
            if (CFG.DARK_MIN_NEIGHBOURS > 0) {
                const n = countNeighbours(p, now);
                if (n < CFG.DARK_MIN_NEIGHBOURS) {
                    console.log(`[DARK FLEET] ${p.name}: only ${n} other ship(s) heard nearby - likely a coverage hole, not saved.`);
                    continue;
                }
            }

            const { error } = await supabase.from('dark_fleet').upsert({
                mmsi: String(mmsi),
                ship_name: p.name,
                last_known_lat: p.lat,
                last_known_lon: p.lon,
                last_sog: p.sog,
                last_cog: p.cog,
                time_went_dark: new Date(p.t).toISOString()
            });
            if (error) {
                console.error('[DARK FLEET] Save failed:', error.message);
                continue;
            }
            console.log(`[DARK FLEET] ${p.name} silent 30+ min in monitored zone. Saved.`);
            darkNow.set(mmsi, p);
        }
    } catch (e) {
        console.error('[DARK FLEET ERROR]', e.message);
    } finally {
        darkBusy = false;
    }
}

// =====================================================================
// 6. MARINETRAFFIC SCRAPER (regional deep intel)  - batched
// =====================================================================
function mtHeaders() {
    const h = {
        'accept': 'application/json',
        'accept-encoding': 'gzip',
        'is-mobile': 'true',
        'is_mobile_v2': 'true',
        'referer': 'https://www.marinetraffic.com',
        'user-agent': 'MarineTraffic/5.3.0 (Android)',
        'x-requested-with': 'XMLHttpRequest'
    };
    if (MT_VESSEL_IMAGE_TOKEN) h['vessel-image'] = MT_VESSEL_IMAGE_TOKEN;
    return h;
}

const ZONES = [
    {
        key: 'india', name: 'INDIA_SUBCONTINENT', table: 'india_intel',
        urls: [
            'https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:44/Y:27/station:0?cb=1',
            'https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:44/Y:28/station:0?cb=2',
            'https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:45/Y:29/station:0?cb=3',
            'https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:45/Y:30/station:0?cb=4',
            'https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:46/Y:31/station:0?cb=5',
            'https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:46/Y:29/station:0?cb=6',
            'https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:47/Y:28/station:0?cb=7'
        ]
    },
    {
        key: 'hormuz', name: 'GREATER_HORMUZ', table: 'hormuz_intel',
        urls: [
            'https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:40/Y:25/station:0?cb=8',
            'https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:40/Y:26/station:0?cb=9',
            'https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:41/Y:26/station:0?cb=10',
            'https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:42/Y:26/station:0?cb=11',
            'https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:42/Y:27/station:0?cb=12'
        ]
    }
];

async function sweepZone(page, zone) {
    const rows = new Map();
    let tilesOk = 0;

    for (const url of zone.urls) {
        try {
            const data = await page.evaluate(async (fetchUrl, headers) => {
                const response = await fetch(fetchUrl, { headers });
                return response.json();
            }, url, mtHeaders());
            tilesOk++;

            for (const ship of (data?.data?.rows || [])) {
                const typeId = String(ship.SHIPTYPE);
                const mmsi = String(ship.MMSI);
                if (!(typeId === '8' || typeId === '35' || typeId === '55') || rows.has(mmsi)) continue;

                const lat = parseFloat(ship.LAT);
                const lon = parseFloat(ship.LON);
                if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;

                const category = typeId === '8' ? 'TANKER' : 'MILITARY';
                const dwt = ship.DWT ? parseFloat(ship.DWT) || 0 : 0;
                // Estimated cargo if the ship is loaded (see OIL settings at the top)
                const estBarrels = (category === 'TANKER' && dwt > 0)
                    ? Math.round(dwt * OIL.LOAD_FACTOR * OIL.BARRELS_PER_TONNE) : 0;

                rows.set(mmsi, {
                    mmsi: mmsi,
                    ship_name: ship.SHIPNAME || 'UNKNOWN',
                    lat: lat,
                    lon: lon,
                    ship_type: ship.SHIPTYPE,
                    category: category,
                    status: ship.STATUS_NAME || 'N/A',
                    destination: ship.DESTINATION || 'UNKNOWN',
                    dwt: dwt,
                    est_barrels: estBarrels,
                    last_updated: new Date().toISOString()
                });
            }
        } catch (e) {
            console.error(`[STEALTH] ${zone.name} tile failed: ${e.message}`);
        }
        await sleep(4500); // be polite, avoid cloud bans
    }

    if (tilesOk === 0 || rows.size === 0) {
        console.log(`[STEALTH] ${zone.name}: no usable data this round, keeping old data.`);
        return;
    }

    // 1) save to Supabase in batches (v1 made one request PER ship)
    const list = Array.from(rows.values());
    for (let i = 0; i < list.length; i += 500) {
        const { error } = await supabase.from(zone.table).upsert(list.slice(i, i + 500));
        if (error) console.error(`[STEALTH] ${zone.name} save error:`, error.message);
    }

    // 2) update RAM: keep rows that are still fresh, add the new ones
    const old = deepIntel[zone.key];
    const fresh = new Map();
    const cutoff = Date.now() - CFG.STALE_INTEL_MS;
    for (const [k, r] of old) {
        if (Date.parse(r.last_updated) > cutoff) fresh.set(k, r);
    }
    for (const [k, r] of rows) fresh.set(k, r);
    deepIntel[zone.key] = fresh;

    // 3) delete ships that left the area (stale rows made oil numbers too big in v1)
    const { error: delErr } = await supabase.from(zone.table).delete().lt('last_updated', hoursAgoISO(CFG.STALE_INTEL_MS / HOUR));
    if (delErr) console.error(`[STEALTH] ${zone.name} cleanup error:`, delErr.message);

    console.log(`[STEALTH] ${zone.name} updated: ${rows.size} targets (${fresh.size} in RAM).`);

    if (zone.key === 'india') await detectArrivals();
}

let sweepRunning = false;
async function runRegionalSweep() {
    if (sweepRunning || shuttingDown) {
        if (sweepRunning) console.log('[STEALTH] Previous sweep still running, skipping this round.');
        return;
    }
    sweepRunning = true;
    let browser;
    try {
        console.log('[STEALTH] Launching scraper...');
        browser = await puppeteer.launch({
            headless: true,
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
        });
        currentBrowser = browser;
        const page = await browser.newPage();

        if (BLOCK_HEAVY_RESOURCES) {
            // we only need the session/cookies from the page, not its pictures and fonts
            await page.setRequestInterception(true);
            page.on('request', (req) => {
                const t = req.resourceType();
                if (t === 'image' || t === 'media' || t === 'font') req.abort().catch(() => {});
                else req.continue().catch(() => {});
            });
        }

        await page.goto('https://www.marinetraffic.com', { waitUntil: 'domcontentloaded', timeout: 60000 });
        for (const zone of ZONES) {
            if (shuttingDown) break;
            await sweepZone(page, zone);
        }
    } catch (error) {
        console.error('[STEALTH ERROR]:', error.message);
    } finally {
        if (browser) await browser.close().catch(() => {});
        currentBrowser = null;
        sweepRunning = false;
    }
}

// =====================================================================
// 7. OIL ANALYTICS  (fixed math)
// ---------------------------------------------------------------------
// OLD (wrong for "per day"): sum(DWT x 7.33) of every tanker inside the box.
//   - that is tanker CAPACITY on the water at one moment, not a daily flow
//   - adding hourly snapshots counts the same ship again and again
//   - included ships going to China, ballast ships, product/chemical/gas tankers
// NEW: count every crude-size tanker ONCE when it reaches an Indian port:
//   barrels = DWT x 0.90 x 7.33      (only ships with DWT >= 60,000)
//   daily imports = sum of barrels of tankers that arrived that IST day
// Note: DWT alone cannot tell a crude tanker from a big product tanker, so
// the totals are an upper-bound style estimate.
// =====================================================================
function isCrudeSize(s) {
    return s.category === 'TANKER' && Number(s.dwt) >= OIL.MIN_CRUDE_DWT;
}

async function detectArrivals() {
    const now = Date.now();
    const warmUp = !firstIndiaSweepDone; // first sweep after a restart: do not count ships that were already in port
    firstIndiaSweepDone = true;

    for (const [mmsi, s] of deepIntel.india) {
        if (!isCrudeSize(s) || !isIndiaDestination(s.destination)) continue;

        const port = nearestIndianPort(s.lat, s.lon);
        if (!port || port.distNm > OIL.PORT_RADIUS_NM) continue;

        const last = recentArrivals.get(mmsi);
        if (last && now - last < OIL.REARRIVAL_BLOCK_MS) {
            recentArrivals.set(mmsi, now); // still waiting in port: extend the block
            continue;
        }
        recentArrivals.set(mmsi, now);
        if (warmUp) continue;

        const barrels = Math.round(s.dwt * OIL.LOAD_FACTOR * OIL.BARRELS_PER_TONNE);
        const { error } = await supabase.from('oil_arrivals').upsert({
            mmsi: mmsi,
            ship_name: s.ship_name,
            dwt: s.dwt,
            est_barrels: barrels,
            destination: s.destination,
            port: port.name,
            arrived_at: new Date(now).toISOString(),
            arrival_day: istDay(now)
        }, { onConflict: 'mmsi,arrival_day', ignoreDuplicates: true });

        if (error) console.error('[OIL] Arrival save failed:', error.message);
        else console.log(`[OIL] ARRIVAL: ${s.ship_name} at ${port.name}, ~${barrels.toLocaleString()} bbl`);
    }
}

function computeOilStats() {
    let activeTankers = 0;
    let capacity = 0;
    for (const s of deepIntel.india.values()) {
        if (s.category !== 'TANKER') continue;
        activeTankers++;
        capacity += (Number(s.dwt) || 0) * OIL.BARRELS_PER_TONNE;
    }

    // Crude-size tankers heading to India that have NOT reached port yet
    const seen = new Set();
    let inboundTankers = 0;
    let inboundBarrels = 0;
    for (const map of [deepIntel.india, deepIntel.hormuz]) {
        for (const [mmsi, s] of map) {
            if (seen.has(mmsi)) continue;
            seen.add(mmsi);
            if (!isCrudeSize(s) || !isIndiaDestination(s.destination)) continue;
            const port = nearestIndianPort(s.lat, s.lon);
            if (port && port.distNm <= OIL.PORT_RADIUS_NM) continue; // already arrived
            inboundTankers++;
            inboundBarrels += s.dwt * OIL.LOAD_FACTOR * OIL.BARRELS_PER_TONNE;
        }
    }
    return {
        active_tankers: activeTankers,
        capacity_barrels_in_region: Math.round(capacity),
        inbound_tankers: inboundTankers,
        inbound_barrels: Math.round(inboundBarrels)
    };
}

async function takeOilSnapshot() {
    try {
        if (deepIntel.india.size === 0) {
            console.log('[ANALYTICS] India data not loaded yet - snapshot skipped.');
            return;
        }
        const st = computeOilStats();
        const { error } = await supabase.from('oil_transit_history').insert({
            region: 'INDIAN_SUBCONTINENT',
            active_tankers: st.active_tankers,
            total_barrels: st.capacity_barrels_in_region,
            inbound_tankers: st.inbound_tankers,
            inbound_barrels: st.inbound_barrels
        });
        if (error) throw error;
        console.log(`[ANALYTICS] Snapshot: ${st.active_tankers} tankers in region, ${st.inbound_tankers} inbound to India.`);
    } catch (err) {
        console.error('[ANALYTICS ERROR]:', err.message);
    }
}

// =====================================================================
// 8. PLAYBACK RECORDERS
// ---------------------------------------------------------------------
// One DB row = one full frame (jsonb array of small arrays). This keeps the
// row count tiny and the free Supabase database small.
// =====================================================================
const cleanSog = (v) => (typeof v === 'number' && v < 102.3 ? round(v, 1) : null);
const cleanCog = (v) => (typeof v === 'number' && v >= 0 && v < 360 ? Math.round(v) : null);

let shipFrameBusy = false;
async function takeShipFrames() {
    if (shipFrameBusy) return;
    shipFrameBusy = true;
    try {
        const now = Date.now();
        for (const [region, box] of Object.entries(PLAYBACK_REGIONS)) {
            const seen = new Set();
            const ships = [];

            for (const v of liveVessels.values()) {
                if (now - v.last_updated > CFG.FRAME_MAX_AGE_MS) continue;
                if (!inBox(v.lat, v.lon, box)) continue;
                const key = String(v.mmsi);
                seen.add(key);
                const intel = intelFor(key);
                ships.push([v.mmsi, round(v.lat, 3), round(v.lon, 3), cleanSog(v.sog), cleanCog(v.cog), intel ? catFromCategory(intel.category) : 0]);
            }
            // ships that only MarineTraffic knows about
            for (const map of [deepIntel.india, deepIntel.hormuz]) {
                for (const [key, s] of map) {
                    if (seen.has(key)) continue;
                    if (now - Date.parse(s.last_updated) > CFG.FRAME_MAX_AGE_MS) continue;
                    if (!inBox(s.lat, s.lon, box)) continue;
                    seen.add(key);
                    ships.push([Number(key), round(s.lat, 3), round(s.lon, 3), null, null, catFromCategory(s.category)]);
                }
            }

            if (ships.length === 0) continue;
            const { error } = await supabase.from('ship_frames').insert({ region, ship_count: ships.length, ships });
            if (error) console.error(`[PLAYBACK] Ship frame ${region} failed:`, error.message);
            else console.log(`[PLAYBACK] Ship frame ${region}: ${ships.length} ships`);
        }
    } catch (e) {
        console.error('[PLAYBACK] Ship frame error:', e.message);
    } finally {
        shipFrameBusy = false;
    }
}

async function fetchAdsb(path, attempt = 1) {
    const res = await fetch('https://api.adsb.lol/v2' + path, {
        signal: AbortSignal.timeout(15000),
        headers: { 'user-agent': 'worldview-radar/2.1' }
    });
    // Rate limited (429): wait (respect Retry-After if present) and try once more
    if (res.status === 429 && attempt < 2) {
        const ra = Number(res.headers.get('retry-after'));
        await sleep(Number.isFinite(ra) && ra > 0 ? Math.min(ra, 20) * 1000 : 8000);
        return fetchAdsb(path, attempt + 1);
    }
    if (!res.ok) throw new Error('ADSB HTTP ' + res.status);
    return res.json();
}

function addPlane(map, ac, isMil) {
    const hex = String(ac.hex || '').toLowerCase();
    if (!hex || typeof ac.lat !== 'number' || typeof ac.lon !== 'number') return;
    const alt = ac.alt_baro === 'ground' ? 0 : (typeof ac.alt_baro === 'number' ? Math.round(ac.alt_baro / 100) * 100 : null);
    const mil = isMil || ((Number(ac.dbFlags) || 0) & 1) === 1;
    const prev = map.get(hex);
    if (prev && prev[7] === 1) return; // keep the military flag if already set
    map.set(hex, [
        hex,
        round(ac.lat, 3),
        round(ac.lon, 3),
        alt,
        typeof ac.gs === 'number' ? Math.round(ac.gs) : null,
        typeof ac.track === 'number' ? Math.round(ac.track) : null,
        String(ac.flight || '').trim(),
        mil ? 1 : 0
    ]);
}

// If adsb.lol says "429 too many requests", reuse the last good answer (max 15 min old)
// so frames do not suddenly lose a whole area. Returns null when there is nothing at all.
const adsbCache = new Map(); // key -> { t, list }
async function getAircraft(key, path) {
    try {
        const d = await fetchAdsb(path);
        const list = d.ac || d.aircraft || [];
        adsbCache.set(key, { t: Date.now(), list });
        return list;
    } catch (e) {
        const c = adsbCache.get(key);
        if (c && Date.now() - c.t < CFG.ADSB_CACHE_MS) {
            console.error(`[PLAYBACK] ${key} failed (${e.message}) - reusing data from ${Math.round((Date.now() - c.t) / 1000)}s ago`);
            return c.list;
        }
        console.error(`[PLAYBACK] ${key} failed:`, e.message);
        return null;
    }
}

let planeFrameBusy = false;
async function takePlaneFrame() {
    if (planeFrameBusy) return;
    planeFrameBusy = true;
    try {
        const planes = new Map();
        let missing = 0;

        const mil = await getAircraft('MIL', '/mil');
        if (mil) { for (const ac of mil) addPlane(planes, ac, true); } else missing++;

        for (const a of PLANE_AREAS) {
            await sleep(CFG.ADSB_GAP_MS); // adsb.lol rate-limits fast bursts
            const list = await getAircraft(a.name, `/point/${a.lat}/${a.lon}/${a.radius}`);
            if (list) { for (const ac of list) addPlane(planes, ac, false); } else missing++;
        }

        // A frame with a whole area missing makes planes "vanish" in playback. A gap is more honest.
        if (missing > CFG.PLANE_MAX_MISSING) {
            console.log(`[PLAYBACK] Plane frame skipped: ${missing} of ${PLANE_AREAS.length + 1} sources failed completely.`);
            return;
        }
        if (planes.size === 0) return;
        const list = Array.from(planes.values());
        const { error } = await supabase.from('plane_frames').insert({ plane_count: list.length, planes: list });
        if (error) console.error('[PLAYBACK] Plane frame failed:', error.message);
        else console.log(`[PLAYBACK] Plane frame: ${list.length} aircraft`);
    } catch (e) {
        console.error('[PLAYBACK] Plane frame error:', e.message);
    } finally {
        planeFrameBusy = false;
    }
}

// =====================================================================
// 9. HOUSEKEEPING  (keeps the free Supabase database and RAM small)
// =====================================================================
async function cleanupOldData() {
    // RAM first (cannot fail)
    const now = Date.now();
    for (const [mmsi, d] of darkNow) {
        if (now - d.t > CFG.DARK_MEMORY_MS) darkNow.delete(mmsi);
    }
    for (const [mmsi, t] of recentArrivals) {
        if (now - t > OIL.REARRIVAL_BLOCK_MS * 2) recentArrivals.delete(mmsi);
    }

    // then the database; one failing table must not stop the others
    const jobs = [
        ['ship_frames', 'ts', CFG.SHIP_FRAME_KEEP_H],
        ['plane_frames', 'ts', CFG.PLANE_FRAME_KEEP_H],
        ['dark_fleet', 'time_went_dark', CFG.DARK_KEEP_DAYS * 24],
        ['dark_fleet_reappearances', 'time_reappeared', CFG.DARK_KEEP_DAYS * 24],
        ['oil_transit_history', 'recorded_at', CFG.OIL_HISTORY_KEEP_DAYS * 24],
        ['oil_arrivals', 'arrived_at', CFG.ARRIVALS_KEEP_DAYS * 24]
    ];
    for (const [table, col, hours] of jobs) {
        try {
            const { error } = await supabase.from(table).delete().lt(col, hoursAgoISO(hours));
            if (error) console.error(`[CLEANUP] ${table}:`, error.message);
        } catch (e) {
            console.error(`[CLEANUP] ${table}:`, e.message);
        }
    }
    console.log('[CLEANUP] Done.');
}

// Every 5 min: forget ships that stopped reporting and MarineTraffic rows that are too old
function cleanMemory() {
    const now = Date.now();

    // Only judge silence while our feed is healthy (and count it from the last "fresh clock" moment)
    if (feedIsUp(now)) {
        for (const [mmsi, v] of liveVessels) {
            if (silentFor(v, now) > 15 * MIN) liveVessels.delete(mmsi);
        }
    }

    // v2 only pruned MarineTraffic rows after a SUCCESSFUL sweep. If scraping keeps failing,
    // old rows stayed in RAM forever and polluted the radar and the oil numbers.
    const cutoff = now - CFG.STALE_INTEL_MS;
    for (const map of [deepIntel.india, deepIntel.hormuz]) {
        for (const [k, r] of map) {
            if (!(Date.parse(r.last_updated) > cutoff)) map.delete(k);
        }
    }
}

// =====================================================================
// 10. LOAD STATE FROM THE DATABASE AFTER A RESTART
// =====================================================================
async function loadFromDatabase() {
    try {
        for (const z of ZONES) {
            const { data, error } = await supabase.from(z.table).select('*').gte('last_updated', hoursAgoISO(CFG.STALE_INTEL_MS / HOUR));
            if (error) { console.error(`[BOOT] ${z.table}:`, error.message); continue; }
            for (const r of (data || [])) {
                deepIntel[z.key].set(String(r.mmsi), {
                    ...r,
                    lat: Number(r.lat), lon: Number(r.lon), dwt: Number(r.dwt) || 0, est_barrels: Number(r.est_barrels) || 0
                });
            }
        }

        const arr = await supabase.from('oil_arrivals').select('mmsi,arrived_at').gte('arrived_at', new Date(Date.now() - OIL.REARRIVAL_BLOCK_MS).toISOString());
        if (arr.error) console.error('[BOOT] oil_arrivals:', arr.error.message, '(did you run supabase_setup.sql?)');
        for (const r of (arr.data || [])) recentArrivals.set(String(r.mmsi), Date.parse(r.arrived_at));

        const dk = await supabase.from('dark_fleet')
            .select('mmsi,ship_name,last_known_lat,last_known_lon,last_sog,last_cog,time_went_dark')
            .gte('time_went_dark', new Date(Date.now() - CFG.DARK_MEMORY_MS).toISOString());
        if (dk.error) console.error('[BOOT] dark_fleet:', dk.error.message, '(did you run supabase_setup.sql?)');
        for (const r of (dk.data || [])) {
            darkNow.set(Number(r.mmsi), {
                t: Date.parse(r.time_went_dark), lat: Number(r.last_known_lat), lon: Number(r.last_known_lon),
                sog: r.last_sog, cog: r.last_cog, name: r.ship_name
            });
        }
        console.log(`[BOOT] Restored ${deepIntel.india.size} India + ${deepIntel.hormuz.size} Hormuz ships, ${recentArrivals.size} arrivals, ${darkNow.size} dark ships.`);
    } catch (e) {
        console.error('[BOOT] Restore failed:', e.message);
    }
}

// =====================================================================
// 11. EXPRESS API
// =====================================================================
const app = express();
app.use(cors());
app.use(compression());

// small cache for playback frames (a frame never changes, so this is safe)
const frameCache = new Map();
const cacheGet = (k) => frameCache.get(k);
function cacheSet(k, v) {
    frameCache.set(k, v);
    if (frameCache.size > 40) frameCache.delete(frameCache.keys().next().value);
}
const normRegion = (r) => {
    const u = String(r || '').toUpperCase();
    return PLAYBACK_REGIONS[u] ? u : null;
};

// Route 0: ping for UptimeRobot
app.get('/', (_req, res) => res.status(200).send('WORLDVIEW ENGINE SECURE & ACTIVE'));

app.get('/api/status', (_req, res) => {
    const now = Date.now();
    const up = feedIsUp(now);
    res.json({
        live_vessels: liveVessels.size,
        india_intel: deepIntel.india.size,
        hormuz_intel: deepIntel.hormuz.size,
        dark_ships_tracked: darkNow.size,
        dark_pending: darkPending.size,
        ais: {
            scope: AIS_GLOBAL ? 'global' : 'regional',
            socket_open: !!(aisSocket && aisSocket.readyState === WebSocket.OPEN),
            feed_up: up,
            dark_detection_paused: !up,
            last_position_age_s: feed.lastMsgAt ? Math.round((now - feed.lastMsgAt) / 1000) : null,
            dark_clock_started_s_ago: feed.resumedAt ? Math.round((now - feed.resumedAt) / 1000) : null,
            outages_since_boot: feed.outages,
            reconnects_since_boot: aisStats.reconnects,
            compression: aisStats.compression
        },
        uptime_min: Math.round(process.uptime() / 60)
    });
});

// ?bbox=minLat,minLon,maxLat,maxLon  (optional, for /api/live and /api/radar)
// With the global AIS feed these routes can return 100k+ ships. A frontend should ask only for what is on screen.
function parseBbox(q) {
    if (!q) return null;
    const p = String(q).split(',').map(Number);
    if (p.length !== 4 || p.some((n) => !Number.isFinite(n))) return null;
    return {
        latMin: Math.min(p[0], p[2]), lonMin: Math.min(p[1], p[3]),
        latMax: Math.max(p[0], p[2]), lonMax: Math.max(p[1], p[3])
    };
}

// Route 1: raw AIS ships from RAM
app.get('/api/live', (req, res) => {
    const box = parseBbox(req.query.bbox);
    const all = Array.from(liveVessels.values());
    res.json(box ? all.filter((v) => inBox(v.lat, v.lon, box)) : all);
});

// Route 2: fused radar (RAM only, cached 4 s - no Supabase call per request)
function buildRadar() {
    const master = new Map();
    for (const [mmsi, v] of liveVessels) {
        const key = String(mmsi);
        const intel = intelFor(key);
        if (intel) {
            master.set(key, { ...v, dwt: intel.dwt, est_barrels: intel.est_barrels, destination: intel.destination, category: intel.category, status: intel.status, source: 'FUSED_INTEL' });
        } else {
            master.set(key, { ...v, dwt: 0, est_barrels: 0, category: 'UNKNOWN', source: 'AISSTREAM' });
        }
    }
    for (const map of [deepIntel.india, deepIntel.hormuz]) {
        for (const [key, ship] of map) {
            if (!master.has(key)) master.set(key, { ...ship, source: 'MARINETRAFFIC' });
        }
    }
    return Array.from(master.values());
}
// Building the radar for a whole-world feed is heavy on a 0.1 CPU instance (and a blocked event loop
// starves the AIS socket), so in global mode it is rebuilt at most every 15 s.
const RADAR_CACHE_MS = AIS_GLOBAL ? 15000 : 4000;
let radarCache = { ts: 0, list: [], body: '[]' };
app.get('/api/radar', (req, res) => {
    try {
        if (Date.now() - radarCache.ts > RADAR_CACHE_MS) radarCache = { ts: Date.now(), list: buildRadar(), body: null };
        const box = parseBbox(req.query.bbox);
        if (box) return res.json(radarCache.list.filter((s) => inBox(s.lat, s.lon, box)));
        if (radarCache.body === null) radarCache.body = JSON.stringify(radarCache.list);
        res.type('application/json').send(radarCache.body);
    } catch (e) {
        console.error('[RADAR ERROR]', e.message);
        res.type('application/json').send(radarCache.body || '[]'); // serve last good copy
    }
});

// Route 3: dark fleet ("AIS gap candidates")
app.get('/api/dark-fleet', async (_req, res) => {
    try {
        const { data, error } = await supabase.from('dark_fleet').select('*').order('time_went_dark', { ascending: false }).limit(50);
        if (error) throw error;
        res.json(data || []);
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch dark fleet data.' });
    }
});
app.get('/api/dark-fleet/reappearances', async (_req, res) => {
    try {
        const { data, error } = await supabase.from('dark_fleet_reappearances').select('*').order('time_reappeared', { ascending: false }).limit(50);
        if (error) throw error;
        res.json(data || []);
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch reappearances.' });
    }
});

// Route 4: old hourly history (kept so existing charts do not break)
app.get('/api/analytics/india', async (_req, res) => {
    try {
        const { data, error } = await supabase.from('oil_transit_history').select('*')
            .eq('region', 'INDIAN_SUBCONTINENT').order('recorded_at', { ascending: false }).limit(168);
        if (error) throw error;
        res.json((data || []).reverse());
    } catch (err) {
        res.status(500).json({ error: 'Failed to fetch analytics.' });
    }
});

// Route 5: live oil numbers (RAM) + arrivals of the last 24 h
app.get('/api/analytics/live-oil', async (_req, res) => {
    const st = computeOilStats();
    let arrived24h = { tankers: 0, barrels: 0 };
    try {
        const { data, error } = await supabase.from('oil_arrivals').select('est_barrels').gte('arrived_at', hoursAgoISO(24));
        if (!error && data) {
            arrived24h = { tankers: data.length, barrels: data.reduce((s, r) => s + (Number(r.est_barrels) || 0), 0) };
        }
    } catch (e) { /* ignore */ }
    res.json({
        active_tankers: st.active_tankers,
        // old key name kept for the frontend; it is CAPACITY of tankers in the region, not imports
        total_barrels_in_transit: st.capacity_barrels_in_region,
        capacity_barrels_in_region: st.capacity_barrels_in_region,
        inbound_tankers: st.inbound_tankers,
        inbound_barrels_to_india: st.inbound_barrels,
        arrived_last_24h: arrived24h,
        note: 'Estimates. Barrels = DWT x 0.90 x 7.33 for crude-size tankers (DWT >= 60,000).'
    });
});

// Route 6: REAL daily imports estimate (tankers counted once, at arrival)
app.get('/api/analytics/india/daily', async (req, res) => {
    try {
        const days = clampInt(req.query.days, 1, 60, 14);
        const since = new Date(Date.now() - (days + 1) * DAY).toISOString();
        const { data, error } = await supabase.from('oil_arrivals')
            .select('arrival_day,est_barrels,ship_name,port,arrived_at')
            .gte('arrived_at', since).order('arrived_at', { ascending: true }).limit(1000);
        if (error) throw error;

        const byDay = {};
        for (const r of (data || [])) {
            if (!byDay[r.arrival_day]) byDay[r.arrival_day] = { day: r.arrival_day, tankers: 0, barrels: 0 };
            byDay[r.arrival_day].tankers += 1;
            byDay[r.arrival_day].barrels += Number(r.est_barrels) || 0;
        }
        const series = [];
        for (let i = days - 1; i >= 0; i--) {
            const d = istDay(Date.now() - i * DAY);
            series.push(byDay[d] || { day: d, tankers: 0, barrels: 0 });
        }
        const last7 = series.slice(-7);
        const avg7 = Math.round(last7.reduce((s, d) => s + d.barrels, 0) / last7.length);

        res.json({
            days: series,
            avg_barrels_per_day_last_7d: avg7,
            note: 'Estimate of crude arriving at Indian ports (IST days). Counting starts when this server first ran, so early days are low. Compare with PPAC monthly data.'
        });
    } catch (err) {
        res.status(500).json({ error: 'Failed to calculate daily imports.' });
    }
});

// ---------------- PLAYBACK API ----------------
app.get('/api/playback/config', (_req, res) => {
    res.json({
        ship_regions: PLAYBACK_REGIONS,
        ship_frame_every_min: CFG.SHIP_FRAME_EVERY_MS / MIN,
        ship_keep_hours: CFG.SHIP_FRAME_KEEP_H,
        plane_frame_every_min: CFG.PLANE_FRAME_EVERY_MS / MIN,
        plane_keep_hours: CFG.PLANE_FRAME_KEEP_H,
        plane_areas: PLANE_AREAS,
        ship_fields: SHIP_FIELDS,
        plane_fields: PLANE_FIELDS,
        ship_category_codes: { 0: 'OTHER', 1: 'TANKER', 2: 'MILITARY' }
    });
});

// list of frames (light - no ship data) for the time slider
app.get('/api/playback/ships/:region/index', async (req, res) => {
    try {
        const region = normRegion(req.params.region);
        if (!region) return res.status(400).json({ error: 'region must be INDIA or HORMUZ' });
        const hours = clampInt(req.query.hours, 1, CFG.SHIP_FRAME_KEEP_H, 24);
        const { data, error } = await supabase.from('ship_frames').select('id,ts,ship_count')
            .eq('region', region).gte('ts', hoursAgoISO(hours)).order('ts', { ascending: true }).limit(1000);
        if (error) throw error;
        res.json({ region, hours, frames: data || [] });
    } catch (e) {
        res.status(500).json({ error: 'Failed to load playback index.' });
    }
});

// one frame
app.get('/api/playback/ships/:region/frame/:id', async (req, res) => {
    try {
        const region = normRegion(req.params.region);
        const id = parseInt(req.params.id, 10);
        if (!region || !Number.isInteger(id)) return res.status(400).json({ error: 'bad region or id' });
        const key = `s:${region}:${id}`;
        let frame = cacheGet(key);
        if (!frame) {
            const { data, error } = await supabase.from('ship_frames').select('id,ts,region,ship_count,ships')
                .eq('id', id).eq('region', region).maybeSingle();
            if (error) throw error;
            if (!data) return res.status(404).json({ error: 'frame not found (maybe deleted)' });
            frame = { ...data, fields: SHIP_FIELDS };
            cacheSet(key, frame);
        }
        res.set('Cache-Control', 'public, max-age=3600');
        res.json(frame);
    } catch (e) {
        res.status(500).json({ error: 'Failed to load frame.' });
    }
});

// path of one ship over the stored period
app.get('/api/playback/ships/:region/trail/:mmsi', async (req, res) => {
    try {
        const region = normRegion(req.params.region);
        if (!region || !/^\d{7,9}$/.test(req.params.mmsi)) return res.status(400).json({ error: 'bad region or mmsi' });
        const hours = clampInt(req.query.hours, 1, CFG.SHIP_FRAME_KEEP_H, 24);
        const { data, error } = await supabase.rpc('ship_trail', { p_region: region, p_mmsi: Number(req.params.mmsi), p_hours: hours });
        if (error) throw error;
        res.json(data || []);
    } catch (e) {
        res.status(500).json({ error: 'Failed to load trail.' });
    }
});

app.get('/api/playback/planes/index', async (req, res) => {
    try {
        const hours = clampInt(req.query.hours, 1, CFG.PLANE_FRAME_KEEP_H, 24);
        const { data, error } = await supabase.from('plane_frames').select('id,ts,plane_count')
            .gte('ts', hoursAgoISO(hours)).order('ts', { ascending: true }).limit(1000);
        if (error) throw error;
        res.json({ hours, frames: data || [] });
    } catch (e) {
        res.status(500).json({ error: 'Failed to load playback index.' });
    }
});

app.get('/api/playback/planes/frame/:id', async (req, res) => {
    try {
        const id = parseInt(req.params.id, 10);
        if (!Number.isInteger(id)) return res.status(400).json({ error: 'bad id' });
        const key = `p:${id}`;
        let frame = cacheGet(key);
        if (!frame) {
            const { data, error } = await supabase.from('plane_frames').select('id,ts,plane_count,planes').eq('id', id).maybeSingle();
            if (error) throw error;
            if (!data) return res.status(404).json({ error: 'frame not found (maybe deleted)' });
            frame = { ...data, fields: PLANE_FIELDS };
            cacheSet(key, frame);
        }
        res.set('Cache-Control', 'public, max-age=3600');
        res.json(frame);
    } catch (e) {
        res.status(500).json({ error: 'Failed to load frame.' });
    }
});

app.get('/api/playback/planes/trail/:hex', async (req, res) => {
    try {
        const hex = String(req.params.hex).toLowerCase();
        if (!/^~?[0-9a-f]{6}$/.test(hex)) return res.status(400).json({ error: 'bad hex' });
        const hours = clampInt(req.query.hours, 1, CFG.PLANE_FRAME_KEEP_H, 24);
        const { data, error } = await supabase.rpc('plane_trail', { p_hex: hex, p_hours: hours });
        if (error) throw error;
        res.json(data || []);
    } catch (e) {
        res.status(500).json({ error: 'Failed to load trail.' });
    }
});

// =====================================================================
// 12. GRACEFUL SHUTDOWN
// ---------------------------------------------------------------------
// On a Render deploy the old instance gets SIGTERM. Closing the AIS socket
// and Chrome here means the NEW instance does not collide with a still-open
// connection from the old one (a likely source of the 429s at boot).
// =====================================================================
async function shutdown(signal) {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[SHUTDOWN] ${signal} received - closing AIS socket and browser...`);
    if (reconnectTimer) clearTimeout(reconnectTimer);
    try { if (aisSocket) aisSocket.terminate(); } catch (e) { /* ignore */ }
    try { if (currentBrowser) await currentBrowser.close(); } catch (e) { /* ignore */ }
    if (server) server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 5000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// =====================================================================
// 13. BOOT
// =====================================================================
server = app.listen(PORT, async () => {
    console.log(`=== WORLDVIEW ENGINE v2.1 RUNNING ON PORT ${PORT} ===`);
    if (!MT_VESSEL_IMAGE_TOKEN) console.warn('[WARN] MT_VESSEL_IMAGE_TOKEN is not set - MarineTraffic scraping may fail.');

    await loadFromDatabase();

    if (AIS_API_KEY) connectAIS();
    else console.error('[AIS] AISSTREAM_API_KEY is not set - live AIS and dark-fleet detection are disabled.');

    safeJob('sweep', runRegionalSweep)();
    setInterval(safeJob('sweep', runRegionalSweep), CFG.SWEEP_EVERY_MS);
    setInterval(safeJob('dark-fleet', detectDarkFleet), 60 * 1000);
    setInterval(safeJob('memory', cleanMemory), 5 * MIN);
    setInterval(safeJob('oil-snapshot', takeOilSnapshot), HOUR);
    setTimeout(safeJob('oil-snapshot', takeOilSnapshot), 3 * MIN); // after the first scrape has finished

    // wait a little so the AIS feed can fill up before the first frame
    setTimeout(safeJob('ship-frames', takeShipFrames), 2 * MIN);
    setInterval(safeJob('ship-frames', takeShipFrames), CFG.SHIP_FRAME_EVERY_MS);
    setTimeout(safeJob('plane-frame', takePlaneFrame), 20 * 1000);
    setInterval(safeJob('plane-frame', takePlaneFrame), CFG.PLANE_FRAME_EVERY_MS);

    setTimeout(safeJob('cleanup', cleanupOldData), 60 * 1000);
    setInterval(safeJob('cleanup', cleanupOldData), HOUR);
});
