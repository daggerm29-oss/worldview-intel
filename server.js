require('dotenv').config();
const express = require('express');
const cors = require('cors');
const WebSocket = require('ws');
const { createClient } = require('@supabase/supabase-js');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');

puppeteer.use(StealthPlugin());

// --- 1. INITIALIZATION ---
const PORT = process.env.PORT || 3000;
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
const AIS_API_KEY = process.env.AISSTREAM_API_KEY;

const app = express();
app.use(cors());

// --- 2. IN-MEMORY STATE (Saves Database Storage) ---
const liveVessels = new Map();
const recentlyLoggedDark = new Map(); // UPGRADED: Now tracks EXACTLY when they went dark

// --- 3. AISSTREAM WORKER (Live RAM Feed) ---
function connectAIS() {
    console.log("[AIS] Connecting to global feed...");
    const ws = new WebSocket("wss://stream.aisstream.io/v0/stream");

    ws.on('open', () => {
        console.log("[AIS] Connected. Streaming live to RAM...");
        ws.send(JSON.stringify({
            APIKey: AIS_API_KEY,
            BoundingBoxes: [[[-90, -180], [90, 180]]],
            FilterMessageTypes: ["PositionReport"]
        }));
    });

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            if (msg.MessageType === "PositionReport") {
                const report = msg.Message.PositionReport;
                const meta = msg.MetaData;
                const mmsi = meta.MMSI;
                
                // Keep the live map fresh in RAM, never touch the database here
                liveVessels.set(mmsi, {
                    mmsi: mmsi,
                    vessel_name: meta.ShipName ? meta.ShipName.trim() : "UNKNOWN",
                    lat: report.Latitude,
                    lon: report.Longitude,
                    sog: report.Sog,
                    cog: report.Cog,
                    last_updated: Date.now()
                });

                // --- REAPPEARANCE DETECTOR ---
                if (recentlyLoggedDark.has(mmsi)) {
                    const timeWentDark = recentlyLoggedDark.get(mmsi);
                    const timeOfflineMs = Date.now() - timeWentDark;
                    const minutesOffline = Math.floor(timeOfflineMs / 60000);

                    if (minutesOffline >= 10) {
                        console.log(`[DARK FLEET] CAUGHT REAPPEARANCE: ${meta.ShipName || "UNKNOWN"} resurfaced after ${minutesOffline} mins!`);
                        
                        // Save the event permanently to Supabase
                        supabase.from('dark_fleet_reappearances').insert({
                            mmsi: String(mmsi),
                            ship_name: meta.ShipName || "UNKNOWN",
                            time_went_dark: new Date(timeWentDark).toISOString(),
                            time_reappeared: new Date().toISOString(),
                            minutes_offline: minutesOffline
                        }).then(({ error }) => {
                            if (error) console.error("Failed to log reappearance:", error.message);
                        });
                    }
                    // Remove from the dark list because it is back online
                    recentlyLoggedDark.delete(mmsi);
                }
            }
        } catch (e) {}
    });

    // --- SHOCK ABSORBER FOR AISSTREAM RATE LIMITS (429 ERROR) ---
    ws.on('error', (error) => {
        console.error(`[AIS CONNECTION ERROR] AISStream refused connection: ${error.message}`);
    });

    ws.on('close', () => {
        console.log("[AIS] Disconnected. Reconnecting in 5s...");
        setTimeout(connectAIS, 5000);
    });
}

// --- 4. DARK FLEET DETECTOR (5-Min Threshold + High-Risk Zone Filter) ---
const DARK_THRESHOLD_MS = 5 * 60 * 1000; // 5 minutes
const DARK_MIN_SOG = 0.5; // 0.5 knots

// GEOFENCE: Bounding box for High-Risk Region (Red Sea -> Persian Gulf -> Indian Subcontinent)
function isHighRiskZone(lat, lon) {
    return lat >= -10 && lat <= 30 && lon >= 35 && lon <= 95;
}

async function detectDarkFleet() {
    const now = Date.now();
    const flagged = [];

    // Scan RAM for lost signals
    for (const [mmsi, v] of liveVessels.entries()) {
        const age = now - v.last_updated;
        
        // FILTERS: Missing for 5 mins AND moving > 0.5 knots AND inside high-risk zone
        if (age > DARK_THRESHOLD_MS && (v.sog || 0) > DARK_MIN_SOG && isHighRiskZone(v.lat, v.lon)) {
            flagged.push(v);
        }
    }

    for (const v of flagged) {
        if (recentlyLoggedDark.has(v.mmsi)) {
            liveVessels.delete(v.mmsi);
            continue;
        }

        const { error } = await supabase.from("dark_fleet").upsert({
            mmsi: String(v.mmsi),
            ship_name: v.vessel_name,
            last_known_lat: v.lat,
            last_known_lon: v.lon,
            time_went_dark: new Date(v.last_updated).toISOString(),
        });

        if (!error) {
            console.log(`[DARK FLEET] Tactical Anomaly: ${v.vessel_name} went dark in monitored zone! Saved to DB.`);
            
            // Start the stopwatch for the reappearance detector
            recentlyLoggedDark.set(v.mmsi, v.last_updated); 
            
            // Forget after 24h so a re-appearance can be re-flagged tomorrow if they go dark again
            setTimeout(() => recentlyLoggedDark.delete(v.mmsi), 24 * 60 * 60 * 1000);
        }
        liveVessels.delete(v.mmsi);
    }
}
setInterval(detectDarkFleet, 60000); // Run detector every 60s

// Housekeeping: Remove old RAM entries that aren't dark fleet but stopped pinging
setInterval(() => {
    const now = Date.now();
    for (const [mmsi, v] of liveVessels.entries()) {
        // Delete from RAM if missing for 15 minutes (safely past the 5-min alarm)
        if (now - v.last_updated > 15 * 60 * 1000) { 
            liveVessels.delete(mmsi);
        }
    }
}, 5 * 60 * 1000); // Run cleanup every 5 mins


// --- 5. MARINETRAFFIC STEALTH WORKER (Regional Deep Intel) ---
const mtHeaders = {
    "accept": "application/json",
    "accept-encoding": "gzip",
    "is-mobile": "true",
    "is_mobile_v2": "true",
    "referer": "https://www.marinetraffic.com",
    "user-agent": "MarineTraffic/5.3.0 (Android)",
    "vessel-image": "339f89c84a559f573636a47ff8daed0d3308",
    "x-requested-with": "XMLHttpRequest"
};
const delay = ms => new Promise(res => setTimeout(res, ms));

async function runRegionalSweep() {
    console.log("[STEALTH] Launching Scraper for Deep Intel...");
    let browser;

    try {
        browser = await puppeteer.launch({
            headless: true,
            args: [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage', // Cloud survival flag
                '--disable-gpu'            // Cloud survival flag
            ]
        });
        const page = await browser.newPage();

        await page.goto('https://www.marinetraffic.com', { waitUntil: 'domcontentloaded', timeout: 60000 });
        const targetZones = [
            {
                name: "INDIA_SUBCONTINENT",
                tableName: "india_intel",
                urls: [
                    "https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:44/Y:27/station:0?cb=1",
                    "https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:44/Y:28/station:0?cb=2",
                    "https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:45/Y:29/station:0?cb=3",
                    "https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:45/Y:30/station:0?cb=4",
                    "https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:46/Y:31/station:0?cb=5",
                    "https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:46/Y:29/station:0?cb=6",
                    "https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:47/Y:28/station:0?cb=7"
                ]
            },
            {
                name: "GREATER_HORMUZ",
                tableName: "hormuz_intel",
                urls: [
                    "https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:40/Y:25/station:0?cb=8",
                    "https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:40/Y:26/station:0?cb=9",
                    "https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:41/Y:26/station:0?cb=10",
                    "https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:42/Y:26/station:0?cb=11",
                    "https://www.marinetraffic.com/getData/get_data_json_4_mob/z:7/X:42/Y:27/station:0?cb=12"
                ]
            }
        ];

        for (const zone of targetZones) {
            let targetCount = 0;
            const seenMMSI = new Set(); 

            for (const url of zone.urls) {
                const data = await page.evaluate(async (fetchUrl, headers) => {
                    const response = await fetch(fetchUrl, { headers });
                    return response.json();
                }, url, mtHeaders);

                const ships = data.data?.rows || [];

                for (const ship of ships) {
                    const typeId = ship.SHIPTYPE;
                    if ((typeId === "8" || typeId === "35" || typeId === "55") && !seenMMSI.has(ship.MMSI)) {
                        seenMMSI.add(ship.MMSI);
                        targetCount++;
                        
                        const category = (typeId === "8") ? "TANKER" : "MILITARY";
                        let dwt = ship.DWT ? parseFloat(ship.DWT) : 0;
                        let estBarrels = (category === "TANKER" && dwt > 0) ? dwt * 7.33 : 0;

                        await supabase.from(zone.tableName).upsert({
                            mmsi: ship.MMSI,
                            ship_name: ship.SHIPNAME || "UNKNOWN",
                            lat: parseFloat(ship.LAT),
                            lon: parseFloat(ship.LON),
                            ship_type: ship.SHIPTYPE,
                            category: category,
                            status: ship.STATUS_NAME || "N/A",
                            destination: ship.DESTINATION || "UNKNOWN",
                            dwt: dwt,
                            est_barrels: estBarrels,
                            last_updated: new Date().toISOString()
                        });
                    }
                }
                await delay(4500); // 4.5s delay to mimic human behavior and avoid cloud bans
            }
            console.log(`[STEALTH] ${zone.name} updated: ${targetCount} deep intel targets saved.`);
        }
    } catch (error) {
        console.error("[STEALTH ERROR]:", error.message);
    } finally {
        if (browser) await browser.close();
    }
}
setInterval(runRegionalSweep, 300000); // Every 5 minutes


// --- 5.5 HISTORICAL OIL AGGREGATOR (Runs every 1 Hour) ---
async function takeOilSnapshot() {
    try {
        const { data, error } = await supabase
            .from('india_intel')
            .select('est_barrels')
            .eq('category', 'TANKER');

        if (!error && data) {
            const activeTankers = data.length;
            const totalBarrels = data.reduce((sum, ship) => sum + Number(ship.est_barrels), 0);

            await supabase.from('oil_transit_history').insert({
                region: "INDIAN_SUBCONTINENT",
                active_tankers: activeTankers,
                total_barrels: totalBarrels
            });
            console.log(`[ANALYTICS] Oil snapshot saved: ${activeTankers} tankers, ${totalBarrels.toFixed(2)} barrels.`);
        }
    } catch (err) {
        console.error("[ANALYTICS ERROR]:", err.message);
    }
}
// Run once on boot, then every 60 minutes
setTimeout(takeOilSnapshot, 10000); 
setInterval(takeOilSnapshot, 60 * 60 * 1000);


// --- 6. EXPRESS API GATEWAYS ---

// Route 0: Lightweight Ping for UptimeRobot (Keeps Render awake 24/7)
app.get("/", (_req, res) => {
    res.status(200).send("WORLDVIEW ENGINE SECURE & ACTIVE");
});

// Route 1: The Raw RAM Feed (Just AISStream)
app.get("/api/live", (_req, res) => {
    res.json(Array.from(liveVessels.values()));
});

// Route 2: SENSOR FUSION GATEWAY (Eliminates Clones & Merges Intel)
app.get("/api/radar", async (_req, res) => {
    try {
        const masterRadar = new Map();

        // Load the baseline: Global live ships from RAM
        for (const [mmsi, vessel] of liveVessels.entries()) {
            masterRadar.set(String(mmsi), { 
                ...vessel, 
                source: "AISSTREAM",
                dwt: 0, 
                est_barrels: 0, 
                category: vessel.vessel_category || "UNKNOWN"
            });
        }

        // Fetch the Deep Intel from Supabase concurrently
        const [indiaRes, hormuzRes] = await Promise.all([
            supabase.from('india_intel').select('*'),
            supabase.from('hormuz_intel').select('*')
        ]);

        const deepIntelShips = [...(indiaRes.data || []), ...(hormuzRes.data || [])];

        // The Merge Logic (Clone Killer)
        for (const ship of deepIntelShips) {
            const mmsi = String(ship.mmsi);
            
            if (masterRadar.has(mmsi)) {
                // CLONE DETECTED: Merge Live RAM + Heavy Database Intel
                const liveData = masterRadar.get(mmsi);
                masterRadar.set(mmsi, {
                    ...liveData,             
                    dwt: ship.dwt,           
                    est_barrels: ship.est_barrels, 
                    destination: ship.destination,
                    category: ship.category, 
                    source: "FUSED_INTEL"    
                });
            } else {
                // Ship ONLY in MarineTraffic
                masterRadar.set(mmsi, { 
                    ...ship, 
                    source: "MARINETRAFFIC" 
                });
            }
        }

        // Send one perfect, clone-free array to the frontend
        res.json(Array.from(masterRadar.values()));

    } catch (error) {
        console.error("[FUSION API ERROR]:", error.message);
        res.status(500).json({ error: "Failed to fuse radar feeds." });
    }
});

// Route 3: Dark Fleet Intelligence
app.get("/api/dark-fleet", async (_req, res) => {
    try {
        const { data, error } = await supabase
            .from('dark_fleet')
            .select('*')
            .order('time_went_dark', { ascending: false })
            .limit(50); // Get the 50 most recent anomalies
        if (error) throw error;
        res.json(data || []);
    } catch (err) {
        res.status(500).json({ error: "Failed to fetch dark fleet data." });
    }
});

// Route 4: Regional Analytics (Historical Oil Imports for Graphs)
app.get("/api/analytics/india", async (_req, res) => {
    try {
        const { data, error } = await supabase
            .from('oil_transit_history')
            .select('*')
            .eq('region', 'INDIAN_SUBCONTINENT')
            .order('recorded_at', { ascending: true })
            .limit(168); // Last ~7 days of hourly snapshots
        if (error) throw error;
        res.json(data || []);
    } catch (err) {
        res.status(500).json({ error: "Failed to fetch analytics." });
    }
});

// Route 5: Live Active Oil Calculation
app.get("/api/analytics/live-oil", async (_req, res) => {
    try {
        const { data, error } = await supabase
            .from('india_intel')
            .select('est_barrels')
            .eq('category', 'TANKER');

        if (error) throw error;

        const activeTankers = data.length;
        // Sum up all the barrels currently on the map
        const totalBarrels = data.reduce((sum, ship) => sum + Number(ship.est_barrels), 0);

        res.json({ 
            active_tankers: activeTankers, 
            total_barrels_in_transit: totalBarrels 
        });
    } catch (err) {
        res.status(500).json({ error: "Failed to calculate live oil." });
    }
});

// --- 7. BOOT SEQUENCE ---
app.listen(PORT, () => {
    console.log(`=== WORLDVIEW ENGINE RUNNING ON PORT ${PORT} ===`);
    connectAIS();
    runRegionalSweep();
});
