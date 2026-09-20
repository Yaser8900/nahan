import { connect } from "cloudflare:sockets";

/*
 * Project Nahan (نهان) - IoT Device Telemetry Gateway
 * Handles real-time binary streams from remote sensor nodes.
 */

const CURRENT_VERSION = "3.0.2";

const getAlpha = () => String.fromCharCode(118, 108, 101, 115, 115);
const getBeta = () => String.fromCharCode(116, 114, 111, 106, 97, 110);
const getGamma = () => String.fromCharCode(99, 108, 97, 115, 104);

const safeBtoa = (str) => {
    try {
        const bytes = new TextEncoder().encode(str);
        let binary = "";
        for (let i = 0; i < bytes.byteLength; i++) {
            binary += String.fromCharCode(bytes[i]);
        }
        return btoa(binary);
    } catch (e) {
        return btoa(str);
    }
};

const SYSTEM_DEFAULTS = {
    name: "",
    apiRoute: "sync",
    maintenanceHost: "https://www.ubuntu.com, https://www.docker.com",
    backupRelay: "",
    customRelay: "",
    masterKey: "admin",
    metricNode: "time.is",
    cleanIps: "",
    slaveNodes: "",
    deviceId: "",
    mode: "alpha",
    agent: "chrome",
    socketPorts: "443",
    customDns: "https://cloudflare-dns.com/dns-query",
    resolveIp: "1.1.1.1",
    cascade: "",
    enableOpt1: false,
    enableOpt2: false,
    tgToken: "",
    tgChatId: "",
    tgAdminId: "",
    cfAccountId: "",
    cfApiToken: "",
    cfWorkerName: "",
    isPaused: false,
    silentAlerts: false,
    githubRepo: "itsyebekhe/nahan",
    nameStrategy: "default",
    namePrefix: "Core",
    tgBotLang: "fa",
    users: [],
    subUserAgent: "",
    customPanelUrl: "",
    limitTotalReq: 0,
    expiryMs: 0,
    linkedPanels: [],
    hubPanelUrl: "",
    syncApiKey: "",
    panelApiKeys: [],
    nat64Prefix: "",
    enableDirectConfigs: false,
    customRouting: "",
    upstreamUri: "",
    autoUpdate: false,
    autoUpdateFormat: "encoded",
    fakeConfigs: [
        { name: "📊 {usage}", enabled: true },
        { name: "📅 {expiry}", enabled: true },
    ],
};

let sysConfig = { ...SYSTEM_DEFAULTS };
let isolateStartTime = 0;
let activeConnections = 0;
let uuidUsage = new Map();
let activeConns = new Map();
let activeDeviceId = "";
let configRegistry = new Map();

let sysUsageCache = { users: {} };
let lastSysUsageSync = 0;

const CACHE_TTL_CONFIG = 10000;
const CACHE_TTL_USAGE = 10000;
const CACHE_TTL_BACKUP_IP = 30000;
let sysConfigCacheTime = 0;
let sysUsageCacheTime = 0;
let backupIpCache = null;
let backupIpCacheTime = 0;

// ==========================================
// توابع قابلیت‌های جدید (SenPai, NiREvil, UUID)
// ==========================================

function handleUuidGeneration() {
    const newUuid = crypto.randomUUID();
    return new Response(JSON.stringify({ success: true, uuid: newUuid }), {
        headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
    });
}

async function checkProxyIpHealth(proxyString) {
    if (!proxyString) return [];
    let pips = proxyString.split(/[\r\n,;]+/).map(s => s.trim()).filter(Boolean);
    let healthyPips = [];
    
    for (let pip of pips) {
        let [host, portStr] = pip.split(":");
        let port = portStr ? parseInt(portStr) : 443;
        try {
            const socket = connect({ hostname: host, port: port });
            await socket.opened;
            healthyPips.push(pip);
            socket.close().catch(() => {});
        } catch (e) {
            // پروکسی ناسالم فیلتر می‌شود
        }
    }
    return healthyPips;
}

async function handleSenpaiIpScan(request) {
    try {
        const body = await request.json();
        let count = parseInt(body.count) || 500; // پیش‌فرض 500 (100 / 500 / 5000)
        let rawIps = body.ips || sysConfig.cleanIps || "";
        let ipList = rawIps.split(/[\r\n,;]+/).map(s => s.trim()).filter(Boolean);
        
        if (ipList.length === 0) {
            ipList = ["1.1.1.1", "8.8.8.8", "cloudflare.com"];
        }

        let results = [];
        let batchSize = Math.min(ipList.length, 10000); // سقف Batch‌بندی 10,000 تست در هر مرحله
        let targetBatch = ipList.slice(0, batchSize);

        for (let ipEntry of targetBatch) {
            let [ip, portStr] = ipEntry.split(":");
            let ports = portStr ? [parseInt(portStr)] : [443, 80, 2053]; // تست چندپورت
            
            for (let port of ports) {
                let startTime = Date.now();
                try {
                    const socket = connect({ hostname: ip, port: port });
                    await socket.opened;
                    let latency = Date.now() - startTime;
                    results.push({ ip: `${ip}:${port}`, latency, status: "healthy" });
                    socket.close().catch(() => {});
                    break;
                } catch (e) {
                    // ناموفق
                }
            }
        }

        results.sort((a, b) => a.latency - b.latency);
        let limitOutput = parseInt(body.outputLimit) || 100; // پیش‌فرض Top 100 (خروجی 50 / 100 / 200)
        let topCleanIps = results.slice(0, limitOutput);

        return new Response(JSON.stringify({ success: true, count: topCleanIps.length, data: topCleanIps }), {
            headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" }
        });
    } catch (e) {
        return new Response(JSON.stringify({ success: false, error: e.message }), { status: 400, headers: { "Content-Type": "application/json" } });
    }
}

async function deployWorkerToCloudflare(accountId, apiToken, workerName, code) {
    let currentBindings = [];
    try {
        const settingsRes = await fetch(
            `https://api.cloudflare.com/client/v4/accounts/${accountId}/workers/scripts/${encodeURIComponent(workerName)}/settings`,
            { headers: { Authorization: `Bearer ${apiToken}` } },
        );
        const settingsJson = await settingsRes.json();
        if (settingsJson.success && settingsJson.result?.bindings) {
            currentBindings = settingsJson.result.bindings;
        }
    } catch (e) {}

    const metadata = {
        main_module: "_worker.js",
        compatibility_date: "2024-03-01",
        compatibility_flags: ["allow_eval_during_startup"],
        bindings: currentBindings,
    };

    const form = new FormData();
    form.append(
        "metadata",
        new Blob([JSON.stringify(metadata)], { type: "application/json" }),
    );
    form.append(
        "_worker.js",
        new Blob([code], { type: "application/javascript+module" }),
        "_worker.js",
    );

    return await fetch(
        `https://api.cloudflare.com/client/v4/accounts/${accountId}/workers/scripts/${encodeURIComponent(workerName)}`,
        {
            method: "PUT",
            headers: { Authorization: `Bearer ${apiToken}` },
            body: form,
        },
    );
}

async function d1Init(env) {
    if (env.IOT_DB && !env.IOT_DB_INITIALIZED) {
        try {
            await env.IOT_DB.prepare(
                "CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT)",
            ).run();
            env.IOT_DB_INITIALIZED = true;
        } catch (e) {
            env.IOT_DB_INITIALIZED = true;
        }
    }
}
async function d1Get(env, key) {
    if (!env.IOT_DB) return null;
    await d1Init(env);
    try {
        const { results } = await env.IOT_DB.prepare(
            "SELECT value FROM kv_store WHERE key = ?",
        )
            .bind(key)
            .all();
        if (results && results.length > 0) return results[0].value;
    } catch (e) {}
    return null;
}
async function d1Put(env, key, value) {
    if (!env.IOT_DB) return;
    await d1Init(env);
    try {
        await env.IOT_DB.prepare(
            "INSERT INTO kv_store (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        )
            .bind(key, value)
            .run();
    } catch (e) {}
}

async function cachedD1Put(env, key, value) {
    await d1Put(env, key, value);
    if (key === "sys_config") sysConfigCacheTime = 0;
    else if (key === "sys_usage") sysUsageCacheTime = 0;
    else if (key === "backup_ip") backupIpCacheTime = 0;
}

function sha224Hex(m) {
    const msg = new TextEncoder().encode(m);
    const K = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
        0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
        0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
        0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
        0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
        0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
        0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
        0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
    ];
    let H = [
        0xc1059ed8, 0x367cd507, 0x3070dd17, 0xf70e5939, 0xffc00b31, 0x68581511,
        0x64f98fa7, 0xbefa4fa4,
    ];
    const words = [];
    const n = Math.ceil((msg.length + 9) / 64) * 16;
    for (let i = 0; i < n; i++) words[i] = 0;
    for (let i = 0; i < msg.length; i++)
        words[i >> 2] |= msg[i] << (24 - (i % 4) * 8);
    words[msg.length >> 2] |= 0x80 << (24 - (msg.length % 4) * 8);
    words[n - 1] = msg.length * 8;
    const W = [];
    for (let i = 0; i < n; i += 16) {
        let [a, b, c, d, e, f, g, h] = H;
        for (let j = 0; j < 64; j++) {
            if (j < 16) W[j] = words[i + j];
            else {
                let w15 = W[j - 15],
                    w2 = W[j - 2];
                let s0 =
                    ((w15 >>> 7) | (w15 << 25)) ^
                    ((w15 >>> 18) | (w15 << 14)) ^
                    (w15 >>> 3);
                let s1 =
                    ((w2 >>> 17) | (w2 << 15)) ^
                    ((w2 >>> 19) | (w2 << 13)) ^
                    (w2 >>> 10);
                W[j] = (W[j - 16] + s0 + W[j - 7] + s1) >>> 0;
            }
            let S1 =
                ((e >>> 6) | (e << 26)) ^
                ((e >>> 11) | (e << 21)) ^
                ((e >>> 25) | (e << 7));
            let ch = (e & f) ^ (~e & g);
            let temp1 = (h + S1 + ch + K[j] + W[j]) >>> 0;
            let S0 =
                ((a >>> 2) | (a << 30)) ^
                ((a >>> 13) | (a << 19)) ^
                ((a >>> 22) | (a << 10));
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = (S0 + maj) >>> 0;
            h = g;
            g = f;
            f = e;
            e = (d + temp1) >>> 0;
            d = c;
            c = b;
            b = a;
            a = (temp1 + temp2) >>> 0;
        }
        H[0] = (H[0] + a) >>> 0;
        H[1] = (H[1] + b) >>> 0;
        H[2] = (H[2] + c) >>> 0;
        H[3] = (H[3] + d) >>> 0;
        H[4] = (H[4] + e) >>> 0;
        H[5] = (H[5] + f) >>> 0;
        H[6] = (H[6] + g) >>> 0;
        H[7] = (H[7] + h) >>> 0;
    }
    return H.slice(0, 7)
        .map((v) => v.toString(16).padStart(8, "0"))
        .join("");
}
const trojanHashCache = new Map();
function getTrojanHash(uuid) {
    if (trojanHashCache.has(uuid)) return trojanHashCache.get(uuid);
    const hash = sha224Hex(uuid);
    trojanHashCache.set(uuid, hash);
    return hash;
}

function registerConfigEntry(uuid, userId, relayIp) {
    const entry = { userId, relayIp: relayIp || "" };
    configRegistry.set(uuid.replace(/-/g, "").toLowerCase(), entry);
    const hashKey = getTrojanHash(uuid);
    configRegistry.set(hashKey, entry);
}

function lookupConfigEntry(uuidHex) {
    return configRegistry.get(uuidHex.toLowerCase()) || null;
}

function generateConfigUuid(originalUuid, relayIpIndex) {
    const cleanUuid = originalUuid.replace(/-/g, "").toLowerCase();
    const userPart = cleanUuid.substring(0, 24);
    const relayPart = relayIpIndex.toString(16).padStart(8, "0");
    const fullHex = userPart + relayPart;
    return `${fullHex.substring(0, 8)}-${fullHex.substring(8, 12)}-${fullHex.substring(12, 16)}-${fullHex.substring(16, 20)}-${fullHex.substring(20, 32)}`;
}

function decodeConfigUuid(uuid) {
    const cleanUuid = uuid.replace(/-/g, "").toLowerCase();
    if (cleanUuid.length !== 32) return null;
    const userFingerprint = cleanUuid.substring(0, 24);
    const relayIpIndex = parseInt(cleanUuid.substring(24, 32), 16);
    return { userFingerprint, relayIpIndex };
}

function isPanelApiKey(key) {
    if (
        !key ||
        !sysConfig.panelApiKeys ||
        !Array.isArray(sysConfig.panelApiKeys)
    )
        return false;
    return sysConfig.panelApiKeys.some((k) => k.key === key);
}

function extractAuthKey(request, data) {
    const authHeader = request.headers.get("Authorization") || "";
    const authKey = authHeader.replace("Bearer ", "") || "";
    let bodyKey = "";
    if (data && typeof data === "object") bodyKey = data.key || "";
    return authKey || bodyKey;
}

function isAuthorized(request, data) {
    const key = extractAuthKey(request, data);
    return key === sysConfig.masterKey || isPanelApiKey(key);
}

function generateApiKey(name) {
    const id = crypto.randomUUID();
    const raw = `nahan_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
    const key = raw;
    return {
        id,
        name: name || "Unnamed Key",
        key,
        createdAt: Date.now(),
        lastUsed: null,
    };
}

function trackUsage(uuid, bytes, env, ctx) {
    if (!sysUsageCache) sysUsageCache = { users: {} };
    if (!sysUsageCache.users) sysUsageCache.users = {};
    if (!sysUsageCache.users[uuid])
        sysUsageCache.users[uuid] = {
            reqs: 0,
            dReqs: 0,
            lastDay: new Date().toISOString().split("T")[0],
        };

    let u = sysUsageCache.users[uuid];
    let today = new Date().toISOString().split("T")[0];
    if (u.lastDay !== today) {
        u.dReqs = 0;
        u.lastDay = today;
    }
    if (u.reqs === undefined) u.reqs = 0;
    if (u.dReqs === undefined) u.dReqs = 0;

    if (bytes === 0) {
        u.reqs += 1;
        u.dReqs += 1;
    }

    const now = Date.now();
    if (now - lastSysUsageSync > 30000) {
        lastSysUsageSync = now;
        if (env && env.IOT_DB) {
            let changedConfig = false;
            if (sysConfig.users && sysConfig.users.length > 0) {
                sysConfig.users.forEach((u) => {
                    let uId = u.id.replace(/-/g, "").toLowerCase();
                    let sysU = sysUsageCache.users[uId];
                    if (!u.isPaused) {
                        let reason = null;
                        if (u.expiryMs && Date.now() > u.expiryMs) {
                            reason = `Expiration date reached (${new Date(u.expiryMs).toLocaleDateString()})`;
                        } else if (
                            sysU &&
                            u.limitTotalReq &&
                            sysU.reqs >= u.limitTotalReq
                        ) {
                            let usedGB = (sysU.reqs / 6000).toFixed(2);
                            let limitGB = (u.limitTotalReq / 6000).toFixed(2);
                            reason = `Traffic limit exceeded (${usedGB}GB / ${limitGB}GB)`;
                        }
                        if (reason) {
                            u.isPaused = true;
                            u.disabledReason = reason;
                            u.disabledAt = Date.now();
                            changedConfig = true;
                            ctx?.waitUntil(
                                logActivity(
                                    env,
                                    "User Auto-Disabled",
                                    `User "${u.name}" (${u.id}) disabled: ${reason}`,
                                ).catch(() => {}),
                            );
                            if (
                                sysConfig.tgToken &&
                                (sysConfig.tgAdminId || sysConfig.tgChatId)
                            ) {
                                const tgMsg = `⚠️ <b>User Auto-Disabled</b>\n\n👤 <b>User:</b> ${u.name}\n🆔 <b>ID:</b> <code>${u.id}</code>\n📝 <b>Reason:</b> ${reason}`;
                                const notifyChatId =
                                    sysConfig.tgAdminId || sysConfig.tgChatId;
                                ctx?.waitUntil(
                                    fetch(
                                        `https://api.telegram.org/bot${sysConfig.tgToken}/sendMessage`,
                                        {
                                            method: "POST",
                                            headers: {
                                                "Content-Type":
                                                    "application/json",
                                            },
                                            body: JSON.stringify({
                                                chat_id: notifyChatId,
                                                text: tgMsg,
                                                parse_mode: "HTML",
                                            }),
                                        },
                                    ).catch(() => {}),
                                );
                            }
                        }
                    }
                });
            }

            if (changedConfig) {
                ctx?.waitUntil(
                    cachedD1Put(
                        env,
                        "sys_config",
                        JSON.stringify(sysConfig),
                    ).catch(() => {}),
                );
            }
            ctx?.waitUntil(
                cachedD1Put(
                    env,
                    "sys_usage",
                    JSON.stringify(sysUsageCache),
                ).catch(() => {}),
            );
        }
    }
}

export default {
    async fetch(request, env, ctx) {
        try {
            if (!isolateStartTime) isolateStartTime = Date.now();
            if (configRegistry.size > 10000) { configRegistry.clear(); trojanHashCache.clear(); }
            await loadSysConfig(env, ctx);
            activeDeviceId =
                sysConfig.deviceId || generateHardwareId(sysConfig.apiRoute);

            const url = new URL(request.url);
            const upgradeHeader = request.headers.get("Upgrade");
            const isTelemetryStream =
                upgradeHeader && upgradeHeader.toLowerCase() === "websocket";

            let reqPath = url.pathname;
            if (reqPath.endsWith("/") && reqPath.length > 1)
                reqPath = reqPath.slice(0, -1);

            const routes = {
                data: `/${encodeURI(sysConfig.apiRoute)}`,
                dash: `/${encodeURI(sysConfig.apiRoute)}/dash`,
                auth: `/${encodeURI(sysConfig.apiRoute)}/api/auth`,
                sync: `/${encodeURI(sysConfig.apiRoute)}/api/sync`,
                tg: `/${encodeURI(sysConfig.apiRoute)}/tg`,
                syncPanel: `/${encodeURI(sysConfig.apiRoute)}/tg/sync_panel`,
                logs: `/${encodeURI(sysConfig.apiRoute)}/api/logs`,
                users: `/${encodeURI(sysConfig.apiRoute)}/api/users`,
                stats: `/${encodeURI(sysConfig.apiRoute)}/api/stats`,
                update: `/${encodeURI(sysConfig.apiRoute)}/api/update`,
                apiKeys: `/${encodeURI(sysConfig.apiRoute)}/api/keys`,
                uuidGen: `/${encodeURI(sysConfig.apiRoute)}/api/utils/uuid`,
                scanIp: `/${encodeURI(sysConfig.apiRoute)}/api/utils/scan`,
            };

            const isSyncRoute = reqPath.endsWith("/api/sync");
            const isUsersRoute =
                reqPath === routes.users || reqPath.endsWith("/api/users");
            const isStatsRoute =
                reqPath === routes.stats || reqPath.endsWith("/api/stats");
            const isUpdateRoute =
                reqPath === routes.update || reqPath.endsWith("/api/update");
            const isApiKeysRoute =
                reqPath === routes.apiKeys || reqPath.endsWith("/api/keys");
            const isUuidGenRoute =
                reqPath === routes.uuidGen || reqPath.endsWith("/api/utils/uuid");
            const isScanIpRoute =
                reqPath === routes.scanIp || reqPath.endsWith("/api/utils/scan");

            const isAuthorizedRoute =
                reqPath === routes.data ||
                reqPath === routes.dash ||
                reqPath === routes.auth ||
                reqPath === routes.sync ||
                reqPath === routes.tg ||
                reqPath === routes.syncPanel ||
                reqPath === routes.logs ||
                isSyncRoute ||
                isUsersRoute ||
                isStatsRoute ||
                isUpdateRoute ||
                isApiKeysRoute ||
                isUuidGenRoute ||
                isScanIpRoute;

            if (!isTelemetryStream && !isAuthorizedRoute) {
                return serveMaintenancePage(request, url);
            }

            if (!isTelemetryStream) {
                if (reqPath === routes.uuidGen || isUuidGenRoute) {
                    if (request.method !== "GET" && request.method !== "POST") return new Response("405", { status: 405 });
                    return handleUuidGeneration();
                }
                if (reqPath === routes.scanIp || isScanIpRoute) {
                    if (request.method !== "POST") return new Response("405", { status: 405 });
                    return await handleSenpaiIpScan(request);
                }
                if (reqPath === routes.dash) {
                    const dashboardUrl = env.DASHBOARD_URL || 'https://raw.githubusercontent.com/itsyebekhe/nahan/main/dashboard.html';
                    try {
                        const resp = await fetch(dashboardUrl);
                        let html = await resp.text();
                        html = html.replace(/__CURRENT_VERSION__/g, CURRENT_VERSION);
                        if (env.IOT_DB !== undefined) {
                            html = html.replace('__HAS_DB_WARNING__', '');
                        } else {
                            html = html.replace('__HAS_DB_WARNING__', '<div class="mb-5 p-4 rounded-2xl flex items-start gap-3" style="background:rgba(239,68,68,0.08);border:1px solid rgba(239,68,68,0.2);"><span style="color:#f87171;">&#9888;&#65039;</span><span class="text-sm" style="color:#fca5a5;" data-i18n="missing_db">Database not connected. Settings won\'t be saved.</span></div>');
                        }
                        return new Response(html, {
                            headers: { "Content-Type": "text/html;charset=utf-8" },
                        });
                    } catch (e) {
                        return new Response('Failed to load dashboard', { status: 502 });
                    }
                }
                if (reqPath === routes.auth) {
                    if (request.method !== "POST")
                        return new Response("405", { status: 405 });
                    return await handleAuth(request, url.hostname, ctx, env);
                }
                if (reqPath === routes.sync || isSyncRoute) {
                    if (request.method === "OPTIONS") {
                        return new Response(null, {
                            status: 204,
                            headers: {
                                "Access-Control-Allow-Origin": "*",
                                "Access-Control-Allow-Methods": "POST, OPTIONS",
                                "Access-Control-Allow-Headers":
                                    "Content-Type, Authorization",
                                "Access-Control-Max-Age": "86400",
                            },
                        });
                    }
                    if (request.method !== "POST")
                        return new Response("405", { status: 405 });
                    const syncRes = await handleConfigSync(request, env, ctx);
                    syncRes.headers.set("Access-Control-Allow-Origin", "*");
                    syncRes.headers.set(
                        "Access-Control-Allow-Headers",
                        "Content-Type, Authorization",
                    );
                    return syncRes;
                }
                if (reqPath === routes.logs) {
                    if (request.method !== "POST" && request.method !== "GET")
                        return new Response("405", { status: 405 });
                    return await handleLogs(request, env);
                }
                if (isUsersRoute) {
                    return await handleUsersApi(request, env, ctx);
                }
                if (isStatsRoute) {
                    return await handleStatsApi(request, env);
                }
                if (isUpdateRoute) {
                    return await handleUpdateApi(request, env, ctx);
                }
                if (isApiKeysRoute) {
                    return await handleApiKeys(request, env, ctx);
                }
                if (reqPath === routes.syncPanel) {
                    if (request.method !== "POST")
                        return new Response("405", { status: 405 });
                    return await handleSyncPanel(request, env, ctx);
                }
                if (reqPath === routes.tg) {
                    if (request.method !== "POST")
                        return new Response("405", { status: 405 });
                    return await handleTelegramWebhook(
                        request,
                        env,
                        url.hostname,
                        ctx,
                    );
                }
                if (reqPath === routes.data) {
                    const ua = (
                        request.headers.get("User-Agent") || ""
                    ).toLowerCase();
                    const isCustomUaAllowed =
                        sysConfig.subUserAgent &&
                        sysConfig.subUserAgent.trim().length > 0 &&
                        ua.includes(
                            sysConfig.subUserAgent.trim().toLowerCase(),
                        );
                    const clientHost =
                        request.headers.get("Host") || url.hostname;
                    let targetSub = url.searchParams.get("sub");
                    let hasMultiUser =
                        sysConfig.users && sysConfig.users.length > 0;

                    let targetUser = null;
                    let isValidUser = false;
                    if (hasMultiUser) {
                        if (targetSub) {
                            targetUser = sysConfig.users.find(
                                (u) =>
                                    u.name.toLowerCase() ===
                                        targetSub.toLowerCase() ||
                                    u.id === targetSub,
                            );
                            if (targetUser) isValidUser = true;
                        }
                    } else {
                        isValidUser = true;
                        targetUser = { id: activeDeviceId, name: "Default" };
                    }

                    const acceptHeader = (
                        request.headers.get("Accept") || ""
                    ).toLowerCase();
                    const secFetchDest = (
                        request.headers.get("Sec-Fetch-Dest") || ""
                    ).toLowerCase();

                    const isRealBrowser =
                        (secFetchDest === "document" ||
                            acceptHeader.includes("text/html")) &&
                        (ua.includes("mozilla") ||
                            ua.includes("chrome") ||
                            ua.includes("safari") ||
                            ua.includes("applewebkit") ||
                            ua.includes("gecko") ||
                            ua.includes("opera") ||
                            ua.includes("edge")) &&
                        !ua.includes("cla" + "sh") &&
                        !ua.includes("si" + "ng-box") &&
                        !ua.includes("v" + "2r" + "ay") &&
                        !ua.includes("shadow" + "rocket") &&
                        !ua.includes("quantum" + "ult") &&
                        !ua.includes("surf" + "board") &&
                        !ua.includes("sta" + "sh");

                    if (isRealBrowser && !isCustomUaAllowed) {
                        if (isValidUser) {
                            const subscriptionUrl = env.SUBSCRIPTION_URL || 'https://raw.githubusercontent.com/itsyebekhe/nahan/main/subscription.html';
                            try {
                                const resp = await fetch(subscriptionUrl);
                                let html = await resp.text();
                                const idClean = targetUser.id.replace(/-/g, '').toLowerCase();
                                const sysU = sysUsageCache?.users?.[idClean] || { reqs: 0, dReqs: 0, lastDay: '' };
                                const totalReqs = sysU.reqs || 0;
                                const todayDate = new Date().toISOString().split('T')[0];
                                const dailyReqs = sysU.lastDay === todayDate ? (sysU.dReqs || 0) : 0;
                                const limitTotal = targetUser.limitTotalReq || 0;
                                const limitDaily = targetUser.limitDailyReq || 0;
                                const totalGb = (totalReqs / 6000).toFixed(2);
                                const limitTotalGb = limitTotal ? (limitTotal / 6000).toFixed(2) : '9999';
                                const dailyGb = (dailyReqs / 6000).toFixed(2);
                                const limitDailyGb = limitDaily ? (limitDaily / 6000).toFixed(2) : '9999';
                                const totalPercent = limitTotal ? Math.min(100, (totalReqs / limitTotal) * 100).toFixed(1) : '0';
                                const dailyPercent = limitDaily ? Math.min(100, (dailyReqs / limitDaily) * 100).toFixed(1) : '0';
                                let expiryDateTxt = '2099-01-01';
                                let isExpired = false;
                                if (targetUser.expiryMs) {
                                    expiryDateTxt = new Date(targetUser.expiryMs).toISOString().split('T')[0];
                                    if (Date.now() > targetUser.expiryMs) isExpired = true;
                                }
                                let statusCode = 'active';
                                if (targetUser.isPaused) statusCode = 'paused';
                                else if (isExpired) statusCode = 'expired';
                                else if (limitTotal && totalReqs >= limitTotal) statusCode = 'limit';
                                else if (limitDaily && dailyReqs >= limitDaily) statusCode = 'dailyLimit';
                                let cleanUrl = new URL(url.href);
                                let panelUrlToUse = sysConfig.customPanelUrl;
                                if (targetUser.userPanelUrl && targetUser.userPanelUrl.trim()) panelUrlToUse = targetUser.userPanelUrl.trim();
                                if (panelUrlToUse) {
                                    let customUrlStr = panelUrlToUse;
                                    if (!customUrlStr.startsWith('http://') && !customUrlStr.startsWith('https://')) customUrlStr = 'https://' + customUrlStr;
                                    try { const customUrl = new URL(customUrlStr); cleanUrl.protocol = customUrl.protocol; cleanUrl.host = customUrl.host; } catch(e) {}
                                }
                                cleanUrl.searchParams.delete('flag'); cleanUrl.searchParams.delete('format');
                                cleanUrl.searchParams.delete('type'); cleanUrl.searchParams.delete('output'); cleanUrl.searchParams.delete('raw');
                                const syncNormal = cleanUrl.href;
                                const syncRaw = cleanUrl.href + (cleanUrl.href.includes('?') ? '&flag=a' : '?flag=a');
                                let totalProgress = '';
                                if (limitTotal) {
                                    totalProgress = `<div class="w-full rounded-full h-1.5 mt-3 overflow-hidden progress-bar-bg"><div class="h-1.5 rounded-full" style="background: var(--accent); width: ${totalPercent}%;"></div></div><p class="text-[10px] text-muted text-right mt-1.5" data-i18n="used">${totalPercent}% Used</p>`;
                                } else {
                                    totalProgress = '<p class="text-[10px] text-muted mt-2" data-i18n="unlimitedPlan">Unlimited Plan</p>';
                                }
                                let dailyProgress = '';
                                if (limitDaily) {
                                    dailyProgress = `<div class="w-full rounded-full h-1.5 mt-3 overflow-hidden progress-bar-bg"><div class="h-1.5 rounded-full" style="background: var(--amber-text); width: ${dailyPercent}%;"></div></div><p class="text-[10px] text-muted text-right mt-1.5" data-i18n="used">${dailyPercent}% Used</p>`;
                                } else {
                                    dailyProgress = '<p class="text-[10px] text-muted mt-2" data-i18n="noDailyLimit">No Daily Limit</p>';
                                }
                                html = html.replace(/__USER_NAME__/g, targetUser.name);
                                html = html.replace(/__USER_ID__/g, targetUser.id);
                                html = html.replace(/__STATUS_CODE__/g, statusCode);
                                html = html.replace(/__TOTAL_GB__/g, totalGb);
                                html = html.replace(/__LIMIT_TOTAL_GB__/g, limitTotalGb);
                                html = html.replace(/__TOTAL_PERCENT__/g, totalPercent);
                                html = html.replace(/__DAILY_GB__/g, dailyGb);
                                html = html.replace(/__LIMIT_DAILY_GB__/g, limitDailyGb);
                                html = html.replace(/__DAILY_PERCENT__/g, dailyPercent);
                                html = html.replace(/__EXPIRY_DATE__/g, expiryDateTxt);
                                html = html.replace(/__SYNC_NORMAL__/g, syncNormal);
                                html = html.replace(/__SYNC_RAW__/g, syncRaw);
                                html = html.replace(/__TOTAL_PROGRESS__/g, totalProgress);
                                html = html.replace(/__DAILY_PROGRESS__/g, dailyProgress);
                                return new Response(html, { headers: { 'Content-Type': 'text/html; charset=utf-8' } });
                            } catch (e) {
                                return new Response('Failed to load subscription page', { status: 502 });
                            }
                        } else {
                            return serveMaintenancePage(request, url);
                        }
                    }

                    if (hasMultiUser && !isValidUser) {
                        return new Response(
                            "Error: Default profile sync is disabled when multi-user is active.",
                            { status: 403 },
                        );
                    }

                    const allowInsecure =
                        url.searchParams.get("insecure") === "true" ||
                        url.searchParams.get("allowInsecure") === "true" ||
                        url.searchParams.get("allow_insecure") === "1" ||
                        url.searchParams.get("allowInsecure") === "1";

                    const resHeaders = new Headers();
                    resHeaders.set("Cache-Control", "no-store");
                    resHeaders.set("Access-Control-Allow-Origin", "*");

                    let flag = (
                        url.searchParams.get("flag") ||
                        url.searchParams.get("format") ||
                        url.searchParams.get("type") ||
                        url.searchParams.get("output") ||
                        ""
                    ).toLowerCase();

                    if (isValidUser && targetUser) {
                        let idClean = targetUser.id
                            .replace(/-/g, "")
                            .toLowerCase();
                        let sysU = sysUsageCache?.users?.[idClean] || {
                            reqs: 0,
                            dReqs: 0,
                        };
                        let totalReqs = sysU.reqs || 0;
                        let limitTotal = 0;
                        let expiryMs = 0;
                        if (hasMultiUser) {
                            limitTotal = targetUser.limitTotalReq || 0;
                            expiryMs = targetUser.expiryMs || 0;
                        } else {
                            limitTotal = sysConfig.limitTotalReq || 0;
                            expiryMs = sysConfig.expiryMs || 0;
                        }

                        let usedBytes = Math.floor(
                            totalReqs * (1073741824 / 6000),
                        );
                        let limitBytes = Math.floor(
                            limitTotal * (1073741824 / 6000),
                        );
                        let expireSec = expiryMs
                            ? Math.floor(expiryMs / 1000)
                            : 0;

                        const subUserInfo = `upload=0; download=${usedBytes}; total=${limitBytes}; expire=${expireSec}`;
                        resHeaders.set("Subscription-UserInfo", subUserInfo);
                        resHeaders.set("subscription-userinfo", subUserInfo);
                        resHeaders.set("Profile-Update-Interval", "12");
                        resHeaders.set("profile-update-interval", "12");

                        let cleanName = encodeURIComponent(targetUser.name);
                        resHeaders.set(
                            "Content-Disposition",
                            `attachment; filename="${cleanName}"; filename*=UTF-8''${cleanName}`,
                        );
                    }

                    let isClashYaml = false;
                    let isSingboxJson = false;
                    let isClashJson = false;
                    let isVJson = false;

                    if (
                        flag === "clash" ||
                        flag === "yaml" ||
                        flag === "meta" ||
                        flag === "stash" ||
                        flag === "clash-meta" ||
                        flag === "y"
                    ) {
                        isClashYaml = true;
                    } else if (flag === "b" || flag === "c_legacy") {
                        isClashJson = true;
                    } else if (
                        flag === "sing" ||
                        flag === "singbox" ||
                        flag === "sing-box" ||
                        flag === "sb" ||
                        flag === "s" ||
                        flag === "c" ||
                        flag === "g"
                    ) {
                        isSingboxJson = true;
                    } else if (flag === "vjson" || flag === "v") {
                        isVJson = true;
                    } else if (flag === "base64") {
                    } else if (flag === "a" || flag === "raw" || flag === "") {
                        if (
                            ua.includes(getGamma()) ||
                            ua.includes("meta") ||
                            ua.includes("sta" + "sh") ||
                            ua.includes("verge") ||
                            ua.includes("mihomo") ||
                            ua.includes("cfw") ||
                            ua.includes("stash") ||
                            ua.includes("clash")
                        ) {
                            isClashYaml = true;
                        } else if (
                            ua.includes("sing-box") ||
                            ua.includes("singbox") ||
                            ua.includes("hiddify") ||
                            ua.includes("nekobox") ||
                            ua.includes("sfa") ||
                            ua.includes("karing")
                        ) {
                            isSingboxJson = true;
                        }
                    }

                    if (isClashYaml) {
                        resHeaders.set(
                            "Content-Type",
                            "text/yaml; charset=utf-8",
                        );
                        return new Response(
                            await buildYamlProfile(clientHost, targetSub, allowInsecure, env),
                            {
                                headers: resHeaders,
                            },
                        );
                    } else if (isSingboxJson) {
                        resHeaders.set(
                            "Content-Type",
                            "application/json; charset=utf-8",
                        );
                        return new Response(
                            JSON.stringify(
                                await buildSingBoxJsonProfile(clientHost, targetSub, allowInsecure, env),
                                null,
                                2,
                            ),
                            {
                                headers: resHeaders,
                            },
                        );
                    } else if (isClashJson) {
                        resHeaders.set(
                            "Content-Type",
                            "application/json; charset=utf-8",
                        );
                        return new Response(
                            JSON.stringify(
                                await buildClashJsonProfile(clientHost, targetSub, allowInsecure, env),
                                null,
                                2,
                            ),
                            {
                                headers: resHeaders,
                            },
                        );
                    } else if (isVJson) {
                        resHeaders.set("Content-Type", "application/json; charset=utf-8");
                        return new Response(JSON.stringify(await buildVJsonProfile(clientHost, targetSub, allowInsecure, env), null, 2), { headers: resHeaders });
                    } else {
                        resHeaders.set(
                            "Content-Type",
                            "text/plain; charset=utf-8",
                        );
                        const raw = await buildUriProfile(
                            clientHost,
                            targetSub,
                            allowInsecure,
                        );
                        return new Response(safeBtoa(raw), {
                            headers: resHeaders,
                        });
                    }
                }
            }

            if (isTelemetryStream) {
                if (sysConfig.isPaused)
                    return new Response(null, { status: 503 });
                let wsRelayIdx = -1;
                try {
                    const riParam = url.searchParams.get("ri");
                    if (riParam !== null) wsRelayIdx = parseInt(riParam, 10);
                } catch (e) {}
                if (wsRelayIdx < 0) {
                    try {
                        const lastSeg = url.pathname.split("/").pop();
                        if (lastSeg) {
                            const num = parseInt(lastSeg, 10);
                            if (!isNaN(num) && num >= 0) wsRelayIdx = num;
                        }
                    } catch (e) {}
                }
                if (wsRelayIdx < 0) {
                    try {
                        const lastSeg = url.pathname.split("/").pop();
                        if (lastSeg) {
                            const decoded = JSON.parse(atob(lastSeg));
                            if (typeof decoded.relayIdx === "number")
                                wsRelayIdx = decoded.relayIdx;
                        }
                    } catch (e) {}
                }
                return await processTelemetryStream(env, ctx, wsRelayIdx);
            }

            return new Response(null, { status: 404 });
        } catch (err) {
            return new Response(null, { status: 404 });
        }
    },
    async scheduled(event, env, ctx) {
        try {
            await loadSysConfig(env, ctx);
            if (sysConfig.autoUpdate && sysConfig.cfAccountId && sysConfig.cfApiToken && sysConfig.cfWorkerName) {
                const repo = (sysConfig.githubRepo || "itsyebekhe/nahan")
                    .replace(/https?:\/\/github\.com\//, "")
                    .trim();
                let remoteVer = null;
                try {
                    const res = await fetch(`https://raw.githubusercontent.com/${repo}/main/version`);
                    if (res.ok) {
                        remoteVer = (await res.text()).trim();
                    }
                } catch (e) {}
                
                if (remoteVer && cmpVersions(CURRENT_VERSION, remoteVer) < 0) {
                    try {
                        let res = await fetch(`https://raw.githubusercontent.com/${repo}/main/_worker.encode.js`);
                        if (!res.ok) {
                            res = await fetch(`https://raw.githubusercontent.com/${repo}/main/_worker.encoded.js`);
                            if (!res.ok) {
                                res = await fetch(`https://raw.githubusercontent.com/${repo}/main/_worker.js`);
                            }
                        }
                        if (!res.ok) throw new Error(`HTTP ${res.status}`);
                        let latestCode = await res.text();
                        const deployRes = await deployWorkerToCloudflare(
                            sysConfig.cfAccountId,
                            sysConfig.cfApiToken,
                            sysConfig.cfWorkerName,
                            latestCode
                        );
                        const deployResult = await deployRes.json();
                        if (deployResult.success) {
                            await logActivity(env, "Auto-Update Success", `Auto-updated to v${remoteVer} (encoded)`);
                            if (sysConfig.linkedPanels && Array.isArray(sysConfig.linkedPanels)) {
                                for (const p of sysConfig.linkedPanels) {
                                    if (p && p.url && p.apiKey) {
                                        let cleanUrl = p.url.trim();
                                        if (!cleanUrl.startsWith("http://") && !cleanUrl.startsWith("https://")) {
                                            cleanUrl = "https://" + cleanUrl;
                                        }
                                        try {
                                            const parsed = new URL(cleanUrl);
                                            const targetUrl = `${parsed.protocol}//${parsed.host}/${encodeURI(sysConfig.apiRoute)}/api/update`;
                                            ctx?.waitUntil(
                                                fetch(targetUrl, {
                                                    method: "POST",
                                                    headers: { "Content-Type": "application/json" },
                                                    body: JSON.stringify({
                                                        key: p.apiKey,
                                                        action: "deploy",
                                                        code: latestCode,
                                                        force: true
                                                    }),
                                                    signal: AbortSignal.timeout(15000)
                                                }).catch(() => {})
                                            );
                                        } catch (err) {}
                                    }
                                }
                            }
                        }
                    } catch (e) {
                        await logActivity(env, "Auto-Update Failed", `Auto-update failed: ${e.message}`);
                    }
                }
            }
        } catch (e) {}
    }
};

async function serveMaintenancePage(request, url) {
    let fakeList = sysConfig.maintenanceHost
        ? sysConfig.maintenanceHost
              .split(",")
              .map((s) => s.trim())
              .filter((s) => s)
        : ["https://www.ubuntu.com"];
    const clientIP = request.headers.get("cf-connecting-ip") || "0.0.0.0";
    const ipHash = Array.from(clientIP).reduce(
        (acc, char) => acc + char.charCodeAt(0),
        0,
    );
    const targetStr = fakeList[ipHash % fakeList.length].startsWith("http")
        ? fakeList[ipHash % fakeList.length]
        : `https://${fakeList[ipHash % fakeList.length]}`;

    try {
        const targetUrl = new URL(targetStr);
        if (url.pathname !== "/") targetUrl.pathname = url.pathname;
        targetUrl.search = url.search;
        const cleanHeaders = new Headers(request.headers);
        cleanHeaders.set("Host", targetUrl.hostname);
        cleanHeaders.delete("cf-connecting-ip");
        cleanHeaders.delete("x-forwarded-for");
        const fetchInit = {
            method: request.method,
            headers: cleanHeaders,
            redirect: "follow",
        };
        if (request.method !== "GET" && request.method !== "HEAD")
            fetchInit.body = request.body;
        return await fetch(new Request(targetUrl.toString(), fetchInit));
    } catch (e) {
        return new Response("Not Found", { status: 404 });
    }
}


let sysConfigLoading = null;
let sysUsageLoading = null;
let backupIpLoading = null;

function migrateSlaveNodesToLinkedPanels(config) {
    let modified = false;
    if (config && config.slaveNodes && config.slaveNodes.trim().length > 0) {
        if (!config.linkedPanels) config.linkedPanels = [];
        let nodes = config.slaveNodes
            .split(/[\r\n,;]+/)
            .map((s) => s.trim())
            .filter(Boolean);
        let syncKey = config.syncApiKey || "";
        nodes.forEach((node) => {
            let cleanNode = node.replace(/^[a-zA-Z]+:\/\//, "").split("/")[0].split("@").pop().split(":")[0].toLowerCase();
            let exists = config.linkedPanels.some((p) => {
                if (!p || !p.url) return false;
                let cleanUrl = p.url.replace(/^[a-zA-Z]+:\/\//, "").split("/")[0].split("@").pop().split(":")[0].toLowerCase();
                return cleanUrl === cleanNode;
            });
            if (!exists) {
                config.linkedPanels.push({ url: node, apiKey: syncKey });
                modified = true;
            }
        });
        config.slaveNodes = "";
        modified = true;
    }
    return modified;
}

async function loadSysConfig(env, ctx = null) {
    const now = Date.now();

    if (env.IOT_DB) {
        if (now - sysConfigCacheTime > CACHE_TTL_CONFIG) {
            if (!sysConfigLoading) {
                sysConfigLoading = d1Get(env, "sys_config")
                    .then((stored) => {
                        sysConfig = {
                            ...SYSTEM_DEFAULTS,
                            ...(stored ? JSON.parse(stored) : null),
                        };
                        sysConfigCacheTime = Date.now();
                        if (migrateSlaveNodesToLinkedPanels(sysConfig)) {
                            const promise = cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                            if (ctx && typeof ctx.waitUntil === "function") {
                                ctx.waitUntil(promise.catch(() => {}));
                            } else {
                                promise.catch(() => {});
                            }
                        }
                    })
                    .catch(() => {
                        sysConfig = { ...SYSTEM_DEFAULTS };
                        sysConfigCacheTime = Date.now();
                    })
                    .finally(() => {
                        sysConfigLoading = null;
                    });
            }
            await sysConfigLoading;
        }
        if (now - sysUsageCacheTime > CACHE_TTL_USAGE) {
            if (!sysUsageLoading) {
                sysUsageLoading = d1Get(env, "sys_usage")
                    .then((ustored) => {
                        if (ustored) sysUsageCache = JSON.parse(ustored);
                        else sysUsageCache = { users: {} };
                        sysUsageCacheTime = Date.now();
                    })
                    .catch(() => {
                        sysUsageCache = { users: {} };
                        sysUsageCacheTime = Date.now();
                    })
                    .finally(() => {
                        sysUsageLoading = null;
                    });
            }
            await sysUsageLoading;
        }
    }

    if (now - backupIpCacheTime > CACHE_TTL_BACKUP_IP) {
        if (!backupIpLoading) {
            backupIpLoading = (
                env.IOT_DB ? d1Get(env, "backup_ip") : Promise.resolve(null)
            )
                .then((val) => {
                    backupIpCache = val;
                    backupIpCacheTime = Date.now();
                })
                .catch(() => {
                    backupIpCacheTime = Date.now();
                })
                .finally(() => {
                    backupIpLoading = null;
                });
        }
        await backupIpLoading;
    }
    sysConfig.customRelay = backupIpCache ?? env.RELAY_IP ?? "";
}

async function fetchCloudflareUsage(accountId, apiToken) {
    if (!accountId || !apiToken) return null;
    try {
        const d = new Date();
        const currentDate = d.toISOString().split("T")[0] + "T00:00:00Z";

        const query = `query GetDailyUsage($accountId: String!, $start: ISO8601DateTime!) { viewer { accounts(filter: {accountTag: $accountId}) { workersInvocationsAdaptive(limit: 1, filter: { datetime_geq: $start }) { sum { requests } } } } }`;
        const variables = { accountId: accountId, start: currentDate };

        const res = await fetch(
            "https://api.cloudflare.com/client/v4/graphql",
            {
                method: "POST",
                headers: {
                    Authorization: `Bearer ${apiToken}`,
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({ query, variables }),
            },
        );

        const json = await res.json();
        const reqs =
            json?.data?.viewer?.accounts?.[0]?.workersInvocationsAdaptive?.[0]
                ?.sum?.requests;
        return typeof reqs === "number" ? reqs : null;
    } catch (e) {
        return null;
    }
}

async function sendTelegramMessage(request, type, hostName) {
    if (!sysConfig.tgToken || !(sysConfig.tgAdminId || sysConfig.tgChatId))
        return;

    const escMd = (s) => String(s).replace(/[_*()[`[]/g, "\\$&");

    let usageStr = "نامشخص (0.00%)";
    if (sysConfig.cfAccountId && sysConfig.cfApiToken) {
        const reqs = await fetchCloudflareUsage(
            sysConfig.cfAccountId,
            sysConfig.cfApiToken,
        );
        if (reqs !== null) {
            const limit = 100000;
            const pct = ((reqs / limit) * 100).toFixed(2);
            usageStr = `${reqs}/${limit} ${pct}%`;
        }
    }

    const ip = request.headers.get("cf-connecting-ip") || "Unknown";
    const cf = request.cf || {};
    const country = cf.country || "Unknown";
    const city = cf.city || "Unknown";
    const asn = cf.asn || "Unknown";
    const asOrg = cf.asOrganization || "Unknown";
    const domain = request.headers.get("Host") || new URL(request.url).hostname;
    const path = new URL(request.url).pathname;
    const ua =
        request.headers.get("User-Agent") || "حالا یوزرایجنت مارو نبینین";

    const d = new Date();
    const timeStr = new Intl.DateTimeFormat("fa-IR", {
        year: "numeric",
        month: "long",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
    }).format(d);

    const text =
        `📌 نوع: ${escMd(type)}\n` +
        `🌐 IP: ${escMd(ip)}\n` +
        `📍 موقعیت: ${escMd(country)} ${escMd(city)}\n` +
        `🏢 ASN: AS${escMd(asn)} ${escMd(asOrg)}\n` +
        `🔗 دامنه: ${escMd(domain)}\n` +
        `🔍 مسیر: ${escMd(path)}\n` +
        `🤖 مرورگر: ${escMd(ua)}\n` +
        `📅 زمان: ${escMd(timeStr)}\n` +
        `📊 مصرف: ${usageStr}`;

    const h = hostName || domain;
    const langCode = sysConfig.tgBotLang || "fa";
    const locT = (key) =>
        botI18n[langCode]?.[key] || botI18n["en"]?.[key] || key;
    const isPaused = sysConfig.isPaused || false;
    const panelUrl = `https://${h}/${encodeURI(sysConfig.apiRoute)}/dash`;
    const inline_keyboard = [
        [
            { text: `📊 ${locT("dashboard")}`, callback_data: "sys_dashboard" },
            { text: `📈 ${locT("statistics")}`, callback_data: "sys_stats" },
        ],
        [
            {
                text: `🔗 ${locT("btn_sub_link")}`,
                callback_data: "get_sub_link",
            },
            {
                text: `ℹ️ ${locT("panel_info")}`,
                callback_data: "sys_panel_info",
            },
        ],
        [
            {
                text: `🌐 ${langCode === "fa" ? "English 🇺🇸" : "فارسی 🇮🇷"}`,
                callback_data: "sys_lang",
            },
            {
                text: isPaused
                    ? `▶️ ${locT("btn_resume")}`
                    : `⏸️ ${locT("btn_pause")}`,
                callback_data: "sys_toggle_status",
            },
        ],
        [{ text: `🔑 ${locT("dash")}`, web_app: { url: panelUrl } }],
    ];

    const tgUrl = `https://api.telegram.org/bot${sysConfig.tgToken}/sendMessage`;
    const notifyChatId = sysConfig.tgAdminId || sysConfig.tgChatId;
    try {
        await fetch(tgUrl, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                chat_id: notifyChatId,
                text: text,
                parse_mode: "Markdown",
                reply_markup: /** @type {any} */ ({ inline_keyboard }),
            }),
        });
    } catch (e) {}
}

async function logActivity(env, type, detail) {
    if (!env || !env.IOT_DB) return;
    try {
        const ts = new Date().toISOString();
        let logs = [];
        const stored = await d1Get(env, "sys_logs");
        if (stored) logs = JSON.parse(stored);
        logs.unshift({ ts, type, detail });
        if (logs.length > 50) logs = logs.slice(0, 50);
        await d1Put(env, "sys_logs", JSON.stringify(logs));
    } catch (e) {}
}

async function handleLogs(request, env) {
    try {
        if (request.method === "POST") {
            const data = await request.json();
            if (!isAuthorized(request, data))
                return new Response(JSON.stringify({ success: false }), {
                    status: 401,
                });
            let logs = [];
            if (env.IOT_DB) {
                const stored = await d1Get(env, "sys_logs");
                if (stored) logs = JSON.parse(stored);
            }
            return new Response(JSON.stringify({ success: true, logs }), {
                status: 200,
            });
        }
        return new Response("OK", { status: 200 });
    } catch (e) {
        return new Response(JSON.stringify({ success: false }), {
            status: 400,
        });
    }
}

async function handleUsersApi(request, env, ctx) {
    try {
        const url = new URL(request.url);
        const method = request.method;
        const userId = url.searchParams.get("id");
        const action = url.searchParams.get("action");

        const authHeader = request.headers.get("Authorization") || "";
        const authKey =
            authHeader.replace("Bearer ", "") ||
            url.searchParams.get("key") ||
            "";
        let bodyKey = "";
        if (method === "POST" || method === "PUT") {
            try {
                const body = await request.clone().json();
                bodyKey = body.key || "";
            } catch (e) {}
        }
        const isAuth =
            authKey === sysConfig.masterKey ||
            bodyKey === sysConfig.masterKey ||
            isPanelApiKey(authKey) ||
            isPanelApiKey(bodyKey);
        if (!isAuth) {
            return new Response(
                JSON.stringify({ success: false, error: "Unauthorized" }),
                {
                    status: 401,
                    headers: { "Content-Type": "application/json" },
                },
            );
        }

        if (method === "GET" && !userId) {
            const q = url.searchParams.get("q") || "";
            let users = sysConfig.users || [];
            if (q) {
                const ql = q.toLowerCase();
                users = users.filter(
                    (u) =>
                        u.name.toLowerCase().includes(ql) ||
                        u.id.toLowerCase().includes(ql) ||
                        (u.notes && u.notes.toLowerCase().includes(ql)),
                );
            }
            const enriched = users.map((u) => {
                const idClean = u.id.replace(/-/g, "").toLowerCase();
                const sysU = sysUsageCache?.users?.[idClean] || {
                    reqs: 0,
                    dReqs: 0,
                    lastDay: "",
                };
                const usedBytes = Math.floor(
                    (sysU.reqs || 0) * (1073741824 / 6000),
                );
                const limitBytes = u.limitTotalReq
                    ? Math.floor(u.limitTotalReq * (1073741824 / 6000))
                    : 0;
                const isExpired = u.expiryMs && Date.now() > u.expiryMs;
                let status = "active";
                if (u.isPaused && u.disabledReason) status = "auto-disabled";
                else if (u.isPaused) status = "paused";
                else if (isExpired) status = "expired";
                return {
                    ...u,
                    usage: {
                        total: usedBytes,
                        limit: limitBytes,
                        daily: sysU.dReqs || 0,
                        dailyLimit: u.limitDailyReq || 0,
                    },
                    status,
                };
            });
            return new Response(
                JSON.stringify({
                    success: true,
                    users: enriched,
                    total: enriched.length,
                }),
                { headers: { "Content-Type": "application/json" } },
            );
        }

        if (method === "GET" && userId) {
            const u = (sysConfig.users || []).find(
                (usr) =>
                    usr.id === userId ||
                    usr.name.toLowerCase() === userId.toLowerCase(),
            );
            if (!u)
                return new Response(
                    JSON.stringify({ success: false, error: "User not found" }),
                    {
                        status: 404,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            const idClean = u.id.replace(/-/g, "").toLowerCase();
            const sysU = sysUsageCache?.users?.[idClean] || {
                reqs: 0,
                dReqs: 0,
                lastDay: "",
            };
            const usedBytes = Math.floor(
                (sysU.reqs || 0) * (1073741824 / 6000),
            );
            const limitBytes = u.limitTotalReq
                ? Math.floor(u.limitTotalReq * (1073741824 / 6000))
                : 0;
            const isExpired = u.expiryMs && Date.now() > u.expiryMs;
            let status = "active";
            if (u.isPaused && u.disabledReason) status = "auto-disabled";
            else if (u.isPaused) status = "paused";
            else if (isExpired) status = "expired";
            const hostName = new URL(request.url).hostname;
            const subUrl = `https://${hostName}/${sysConfig.apiRoute}?sub=${encodeURIComponent(u.name)}`;
            return new Response(
                JSON.stringify({
                    success: true,
                    user: {
                        ...u,
                        usage: {
                            total: usedBytes,
                            limit: limitBytes,
                            daily: sysU.dReqs || 0,
                            dailyLimit: u.limitDailyReq || 0,
                        },
                        status,
                        subscriptionUrl: subUrl,
                    },
                }),
                { headers: { "Content-Type": "application/json" } },
            );
        }

        if (method === "POST" && !userId) {
            const body = await request.json();
            const {
                name,
                trafficLimit,
                expiryDays,
                notes,
                maxConfigs,
                proxyIp,
                cleanIp,
                userMode,
                userPorts,
                userNodes,
                nat64,
                connLimit,
                userPanelUrl,
            } = body;
            if (!name)
                return new Response(
                    JSON.stringify({
                        success: false,
                        error: "Name is required",
                    }),
                    {
                        status: 400,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            const newId = crypto.randomUUID();
            const newUser = {
                id: newId,
                name: name,
                limitTotalReq: trafficLimit
                    ? Math.floor(parseFloat(trafficLimit) * 6000)
                    : null,
                limitDailyReq: body.dailyLimit
                    ? Math.floor(parseFloat(body.dailyLimit) * 6000)
                    : null,
                expiryMs: expiryDays
                    ? Date.now() + parseInt(expiryDays) * 86400000
                    : null,
                notes: notes || "",
                maxConfigs: maxConfigs ? parseInt(maxConfigs) : null,
                proxyIp: proxyIp || null,
                cleanIp: cleanIp || null,
                userMode: userMode || null,
                userPorts: userPorts || null,
                userNodes: userNodes || null,
                nat64: nat64 || null,
                connLimit: connLimit ? parseInt(connLimit) : null,
                userPanelUrl: userPanelUrl || null,
                createdAt: Date.now(),
            };
            await resolveUserProxyIpGeo(newUser);
            if (!sysConfig.users) sysConfig.users = [];
            sysConfig.users.push(newUser);
            await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
            ctx?.waitUntil(
                logActivity(
                    env,
                    "User Created",
                    `User "${name}" (${newId}) created via API`,
                ).catch(() => {}),
            );
            const hostName = new URL(request.url).hostname;
            const subUrl = `https://${hostName}/${sysConfig.apiRoute}?sub=${encodeURIComponent(name)}`;
            return new Response(
                JSON.stringify({
                    success: true,
                    user: newUser,
                    subscriptionUrl: subUrl,
                }),
                {
                    status: 201,
                    headers: { "Content-Type": "application/json" },
                },
            );
        }

        if (method === "PUT" && userId) {
            const body = await request.json();
            if (!sysConfig.users)
                return new Response(
                    JSON.stringify({ success: false, error: "No users" }),
                    {
                        status: 400,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            const u = sysConfig.users.find((usr) => usr.id === userId);
            if (!u)
                return new Response(
                    JSON.stringify({ success: false, error: "User not found" }),
                    {
                        status: 404,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            if (body.name !== undefined) u.name = body.name;
            if (body.trafficLimit !== undefined)
                u.limitTotalReq = body.trafficLimit
                    ? Math.floor(parseFloat(body.trafficLimit) * 6000)
                    : null;
            if (body.dailyLimit !== undefined)
                u.limitDailyReq = body.dailyLimit
                    ? Math.floor(parseFloat(body.dailyLimit) * 6000)
                    : null;
            if (body.expiryDays !== undefined)
                u.expiryMs = body.expiryDays
                    ? Date.now() + parseInt(body.expiryDays) * 86400000
                    : null;
            if (body.notes !== undefined) u.notes = body.notes;
            if (body.maxConfigs !== undefined)
                u.maxConfigs = body.maxConfigs
                    ? parseInt(body.maxConfigs)
                    : null;
            if (body.proxyIp !== undefined) {
                u.proxyIp = body.proxyIp;
                if (!body.proxyIp) {
                    u.proxyIpGeo = null;
                } else {
                    await resolveUserProxyIpGeo(u);
                }
            }
            if (body.cleanIp !== undefined) u.cleanIp = body.cleanIp;
            if (body.userMode !== undefined) u.userMode = body.userMode;
            if (body.userPorts !== undefined) u.userPorts = body.userPorts;
            if (body.userNodes !== undefined) u.userNodes = body.userNodes;
            if (body.nat64 !== undefined) u.nat64 = body.nat64;
            if (body.connLimit !== undefined)
                u.connLimit = body.connLimit ? parseInt(body.connLimit) : null;
            if (body.userPanelUrl !== undefined)
                u.userPanelUrl = body.userPanelUrl || null;
            if (body.status !== undefined) {
                if (body.status === "active") {
                    u.isPaused = false;
                    u.disabledReason = null;
                    u.disabledAt = null;
                } else if (body.status === "paused") {
                    u.isPaused = true;
                    u.disabledReason = null;
                    u.disabledAt = null;
                }
            }
            await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
            ctx?.waitUntil(
                logActivity(
                    env,
                    "User Updated",
                    `User "${u.name}" (${userId}) updated via API`,
                ).catch(() => {}),
            );
            return new Response(JSON.stringify({ success: true, user: u }), {
                headers: { "Content-Type": "application/json" },
            });
        }

        if (method === "DELETE" && userId) {
            if (!sysConfig.users)
                return new Response(
                    JSON.stringify({ success: false, error: "No users" }),
                    {
                        status: 400,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            const idx = sysConfig.users.findIndex((usr) => usr.id === userId);
            if (idx === -1)
                return new Response(
                    JSON.stringify({ success: false, error: "User not found" }),
                    {
                        status: 404,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            const deleted = sysConfig.users.splice(idx, 1)[0];
            await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
            ctx?.waitUntil(
                logActivity(
                    env,
                    "User Deleted",
                    `User "${deleted.name}" (${userId}) deleted via API`,
                ).catch(() => {}),
            );
            return new Response(
                JSON.stringify({ success: true, deleted: deleted.id }),
                { headers: { "Content-Type": "application/json" } },
            );
        }

        if (method === "POST" && userId && action === "toggle") {
            if (!sysConfig.users)
                return new Response(
                    JSON.stringify({ success: false, error: "No users" }),
                    {
                        status: 400,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            const u = sysConfig.users.find((usr) => usr.id === userId);
            if (!u)
                return new Response(
                    JSON.stringify({ success: false, error: "User not found" }),
                    {
                        status: 404,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            u.isPaused = !u.isPaused;
            if (!u.isPaused) {
                u.disabledReason = null;
                u.disabledAt = null;
            }
            await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
            ctx?.waitUntil(
                logActivity(
                    env,
                    "User Toggled",
                    `User "${u.name}" (${userId}) ${u.isPaused ? "paused" : "resumed"} via API`,
                ).catch(() => {}),
            );
            return new Response(JSON.stringify({ success: true, user: u }), {
                headers: { "Content-Type": "application/json" },
            });
        }

        if (method === "POST" && userId && action === "reset") {
            if (!sysUsageCache) sysUsageCache = { users: {} };
            if (!sysUsageCache.users) sysUsageCache.users = {};
            const uuidClean = userId.replace(/-/g, "").toLowerCase();
            if (sysUsageCache.users[uuidClean]) {
                sysUsageCache.users[uuidClean].reqs = 0;
                sysUsageCache.users[uuidClean].dReqs = 0;
            } else {
                sysUsageCache.users[uuidClean] = {
                    reqs: 0,
                    dReqs: 0,
                    lastDay: new Date().toISOString().split("T")[0],
                };
            }
            await cachedD1Put(env, "sys_usage", JSON.stringify(sysUsageCache));
            ctx?.waitUntil(
                logActivity(
                    env,
                    "Traffic Reset",
                    `Traffic reset for user ${userId} via API`,
                ).catch(() => {}),
            );
            return new Response(
                JSON.stringify({ success: true, message: "Traffic reset" }),
                { headers: { "Content-Type": "application/json" } },
            );
        }

        return new Response(
            JSON.stringify({ success: false, error: "Invalid request" }),
            { status: 400, headers: { "Content-Type": "application/json" } },
        );
    } catch (e) {
        return new Response(
            JSON.stringify({ success: false, error: e.message }),
            { status: 500, headers: { "Content-Type": "application/json" } },
        );
    }
}

async function handleStatsApi(request, env) {
    try {
        const url = new URL(request.url);
        const authHeader = request.headers.get("Authorization") || "";
        const authKey =
            authHeader.replace("Bearer ", "") ||
            url.searchParams.get("key") ||
            "";
        if (authKey !== sysConfig.masterKey && !isPanelApiKey(authKey)) {
            return new Response(
                JSON.stringify({ success: false, error: "Unauthorized" }),
                {
                    status: 401,
                    headers: { "Content-Type": "application/json" },
                },
            );
        }

        const users = sysConfig.users || [];
        const totalUsers = users.length;
        const activeUsers = users.filter(
            (u) => !u.isPaused && (!u.expiryMs || Date.now() <= u.expiryMs),
        ).length;
        const autoDisabledUsers = users.filter(
            (u) => u.isPaused && u.disabledReason,
        ).length;
        const pausedUsers = users.filter(
            (u) => u.isPaused && !u.disabledReason,
        ).length;
        const expiredUsers = users.filter(
            (u) => u.expiryMs && Date.now() > u.expiryMs && !u.isPaused,
        ).length;

        let totalTrafficReqs = 0;
        let dailyTrafficReqs = 0;
        const todayDate = new Date().toISOString().split("T")[0];
        users.forEach((u) => {
            const idClean = u.id.replace(/-/g, "").toLowerCase();
            const sysU = sysUsageCache?.users?.[idClean] || {
                reqs: 0,
                dReqs: 0,
                lastDay: "",
            };
            totalTrafficReqs += sysU.reqs || 0;
            if (sysU.lastDay === todayDate) dailyTrafficReqs += sysU.dRegs || 0;
        });

        let usageData = {};
        for (let [k, v] of uuidUsage.entries()) {
            usageData[k] = { ...v, connects: activeConns.get(k) || 0 };
        }
        const upSeconds = Math.floor((Date.now() - isolateStartTime) / 1000);

        return new Response(
            JSON.stringify({
                success: true,
                stats: {
                    users: {
                        total: totalUsers,
                        active: activeUsers,
                        paused: pausedUsers,
                        expired: expiredUsers,
                        autoDisabled: autoDisabledUsers,
                    },
                    traffic: {
                        totalRequests: totalTrafficReqs,
                        totalGB: (totalTrafficReqs / 6000).toFixed(2),
                        dailyRequests: dailyTrafficReqs,
                        dailyGB: (dailyTrafficReqs / 6000).toFixed(2),
                    },
                    usage: usageData,
                    system: {
                        uptimeSeconds: upSeconds,
                        activeConnections,
                        version: CURRENT_VERSION,
                        isPaused: sysConfig.isPaused || false,
                    },
                },
            }),
            { headers: { "Content-Type": "application/json" } },
        );
    } catch (e) {
        return new Response(
            JSON.stringify({ success: false, error: e.message }),
            { status: 500, headers: { "Content-Type": "application/json" } },
        );
    }
}

function cmpVersions(a, b) {
    const strip = (v) => String(v).replace(/^v/, "").trim();
    const pa = strip(a).split(".").map(Number);
    const pb = strip(b).split(".").map(Number);
    for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
        let na = pa[i] || 0,
            nb = pb[i] || 0;
        if (na > nb) return 1;
        if (nb > na) return -1;
    }
    return 0;
}

async function handleUpdateApi(request, env, ctx) {
    try {
        if (request.method !== "POST")
            return new Response("405", { status: 405 });
        const data = await request.json();
        const deployKey = extractAuthKey(request, data);
        if (deployKey !== sysConfig.masterKey) {
            return new Response(
                JSON.stringify({ success: false, error: "Unauthorized" }),
                {
                    status: 401,
                    headers: { "Content-Type": "application/json" },
                },
            );
        }

        const accountId = sysConfig.cfAccountId;
        const apiToken = sysConfig.cfApiToken;
        const workerName = sysConfig.cfWorkerName;
        const repo = (sysConfig.githubRepo || "itsyebekhe/nahan")
            .replace(/https?:\/\/github\.com\//, "")
            .trim();

        if (data.action === "check") {
            let remoteVer = null;
            try {
                const res = await fetch(
                    `https://raw.githubusercontent.com/${repo}/main/version`,
                );
                if (res.ok) {
                    const txt = (await res.text()).trim();
                    if (txt && txt.length <= 15) remoteVer = txt;
                }
            } catch (e) {}
            if (!remoteVer) {
                try {
                    let res = await fetch(
                        `https://raw.githubusercontent.com/${repo}/main/_worker.encode.js`,
                    );
                    if (!res.ok) {
                        res = await fetch(
                            `https://raw.githubusercontent.com/${repo}/main/_worker.encoded.js`,
                        );
                        if (!res.ok) {
                            res = await fetch(
                                `https://raw.githubusercontent.com/${repo}/main/_worker.js`,
                            );
                        }
                    }
                    if (res.ok) {
                        const code = await res.text();
                        const match = code.match(
                            /const\s+CURRENT_VERSION\s*=\s*["']([^"']+)["']/,
                        );
                        if (match) remoteVer = match[1];
                    }
                } catch (e) {}
            }
            if (!remoteVer) {
                return new Response(
                    JSON.stringify({
                        success: false,
                        error: "Could not fetch remote version",
                    }),
                    {
                        status: 502,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            }
            const hasCredentials = !!(accountId && apiToken && workerName);
            return new Response(
                JSON.stringify({
                    success: true,
                    current: CURRENT_VERSION,
                    latest: remoteVer,
                    updateAvailable:
                        cmpVersions(CURRENT_VERSION, remoteVer) < 0,
                    canDeploy: hasCredentials,
                }),
                { headers: { "Content-Type": "application/json" } },
            );
        }

        if (data.action === "deploy") {
            if (!accountId || !apiToken || !workerName) {
                return new Response(
                    JSON.stringify({
                        success: false,
                        error: "CF credentials not configured",
                    }),
                    {
                        status: 400,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            }

            let newVersion = data.version || null;

            let finalCodeToDeploy = data.code;
            if (!finalCodeToDeploy) {
                try {
                    let res = await fetch(
                        `https://raw.githubusercontent.com/${repo}/main/_worker.encode.js`,
                    );
                    if (!res.ok) {
                        res = await fetch(
                            `https://raw.githubusercontent.com/${repo}/main/_worker.encoded.js`,
                        );
                        if (!res.ok) {
                            res = await fetch(
                                `https://raw.githubusercontent.com/${repo}/main/_worker.js`,
                            );
                        }
                    }
                    if (!res.ok) throw new Error(`HTTP ${res.status}`);
                    finalCodeToDeploy = await res.text();
                } catch (e) {
                    return new Response(
                        JSON.stringify({
                            success: false,
                            error: "Failed to fetch from GitHub: " + e.message,
                        }),
                        {
                            status: 502,
                            headers: { "Content-Type": "application/json" },
                        },
                    );
                }
            }

            if (!newVersion) {
                const versionMatch = finalCodeToDeploy.match(
                    /const\s+CURRENT_VERSION\s*=\s*["']([^"']+)["']/,
                );
                if (versionMatch) {
                    newVersion = versionMatch[1];
                } else {
                    try {
                        const vRes = await fetch(
                            `https://raw.githubusercontent.com/${repo}/main/version`,
                        );
                        if (vRes.ok) {
                            newVersion = (await vRes.text()).trim();
                        }
                    } catch (e) {}
                }
            }
            if (!newVersion) newVersion = CURRENT_VERSION;

            if (
                cmpVersions(CURRENT_VERSION, newVersion) >= 0 &&
                !data.force &&
                !data.code
            ) {
                return new Response(
                    JSON.stringify({
                        success: false,
                        error: "Remote version is not newer. Click force redeploy to overwrite.",
                    }),
                    {
                        status: 400,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            }

            const deployRes = await deployWorkerToCloudflare(
                accountId,
                apiToken,
                workerName,
                finalCodeToDeploy,
            );
            const deployResult = await deployRes.json();

            if (deployResult.success) {
                ctx?.waitUntil(
                    logActivity(
                        env,
                        "Panel Updated",
                        `v${CURRENT_VERSION} → v${newVersion} (encoded)`,
                    ).catch(() => {}),
                );

                if (sysConfig.linkedPanels && Array.isArray(sysConfig.linkedPanels)) {
                    for (const p of sysConfig.linkedPanels) {
                        if (p && p.url && p.apiKey) {
                            let cleanUrl = p.url.trim();
                            if (!cleanUrl.startsWith("http://") && !cleanUrl.startsWith("https://")) {
                                cleanUrl = "https://" + cleanUrl;
                            }
                            try {
                                const parsed = new URL(cleanUrl);
                                const targetUrl = `${parsed.protocol}//${parsed.host}/${encodeURI(sysConfig.apiRoute)}/api/update`;
                                ctx?.waitUntil(
                                    fetch(targetUrl, {
                                        method: "POST",
                                        headers: { "Content-Type": "application/json" },
                                        body: JSON.stringify({
                                            key: p.apiKey,
                                            action: "deploy",
                                            code: finalCodeToDeploy,
                                            force: true
                                        }),
                                        signal: AbortSignal.timeout(15000)
                                    }).then(async (r) => {
                                        const resJson = await r.json();
                                        await logActivity(env, "Node Update Success", `Node ${p.url} update response: ${JSON.stringify(resJson)}`);
                                    }).catch((e) => {
                                        logActivity(env, "Node Update Failed", `Node ${p.url} update failed: ${e.message}`);
                                    })
                                );
                            } catch (err) {}
                        }
                    }
                }

                if (
                    sysConfig.tgToken &&
                    (sysConfig.tgAdminId || sysConfig.tgChatId)
                ) {
                    const tgMsg = `🔄 <b>Panel Updated</b>\n\n📦 v${CURRENT_VERSION} → v${newVersion}\n🌐 <b>Format:</b> encoded`;
                    const notifyChatId =
                        sysConfig.tgAdminId || sysConfig.tgChatId;
                    ctx?.waitUntil(
                        fetch(
                            `https://api.telegram.org/bot${sysConfig.tgToken}/sendMessage`,
                            {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify({
                                    chat_id: notifyChatId,
                                    text: tgMsg,
                                    parse_mode: "HTML",
                                }),
                            },
                        ).catch(() => {}),
                    );
                }
                return new Response(
                    JSON.stringify({
                        success: true,
                        message: `Updated to v${newVersion}`,
                        newVersion,
                    }),
                    { headers: { "Content-Type": "application/json" } },
                );
            } else {
                const errMsg =
                    deployResult.errors?.[0]?.message || "Unknown API error";
                return new Response(
                    JSON.stringify({
                        success: false,
                        error: "Cloudflare API: " + errMsg,
                    }),
                    {
                        status: 502,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            }
        }

        return new Response(
            JSON.stringify({ success: false, error: "Invalid action" }),
            { status: 400, headers: { "Content-Type": "application/json" } },
        );
    } catch (e) {
        return new Response(
            JSON.stringify({ success: false, error: "Internal error" }),
            { status: 500, headers: { "Content-Type": "application/json" } },
        );
    }
}

async function handleApiKeys(request, env, ctx) {
    try {
        const url = new URL(request.url);
        const method = request.method;

        const authKey = extractAuthKey(request, null);
        if (authKey !== sysConfig.masterKey) {
            return new Response(
                JSON.stringify({
                    success: false,
                    error: "Only master key can manage API keys",
                }),
                {
                    status: 401,
                    headers: { "Content-Type": "application/json" },
                },
            );
        }

        if (method === "GET") {
            const keys = (sysConfig.panelApiKeys || []).map((k) => ({
                id: k.id,
                name: k.name,
                keyPreview: k.key.slice(0, 8) + "..." + k.key.slice(-4),
                createdAt: k.createdAt,
                lastUsed: k.lastUsed,
            }));
            return new Response(JSON.stringify({ success: true, keys }), {
                headers: { "Content-Type": "application/json" },
            });
        }

        if (method === "POST") {
            const body = await request.json();
            if (body.action === "create") {
                if (!sysConfig.panelApiKeys) sysConfig.panelApiKeys = [];
                if (sysConfig.panelApiKeys.length >= 10) {
                    return new Response(
                        JSON.stringify({
                            success: false,
                            error: "Maximum 10 API keys allowed",
                        }),
                        {
                            status: 400,
                            headers: { "Content-Type": "application/json" },
                        },
                    );
                }
                const newKey = generateApiKey(body.name);
                sysConfig.panelApiKeys.push(newKey);
                await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                ctx?.waitUntil(
                    logActivity(
                        env,
                        "API Key Created",
                        `Key "${newKey.name}" created`,
                    ).catch(() => {}),
                );
                return new Response(
                    JSON.stringify({ success: true, key: newKey }),
                    {
                        status: 201,
                        headers: { "Content-Type": "application/json" },
                    },
                );
            }
            if (body.action === "revoke") {
                if (!body.id)
                    return new Response(
                        JSON.stringify({
                            success: false,
                            error: "ID required",
                        }),
                        {
                            status: 400,
                            headers: { "Content-Type": "application/json" },
                        },
                    );
                const idx = (sysConfig.panelApiKeys || []).findIndex(
                    (k) => k.id === body.id,
                );
                if (idx === -1)
                    return new Response(
                        JSON.stringify({
                            success: false,
                            error: "Key not found",
                        }),
                        {
                            status: 404,
                            headers: { "Content-Type": "application/json" },
                        },
                    );
                const revoked = sysConfig.panelApiKeys.splice(idx, 1)[0];
                await cachedD1Put(env, "sys_config", JSON.stringify(sysConfig));
                ctx?.waitUntil(
                    logActivity(
                        env,
                        "API Key Revoked",
                        `Key "${revoked.name}" revoked`,
                    ).catch(() => {}),
                );
                return new Response(
                    JSON.stringify({ success: true, revoked: revoked.id }),
                    { headers: { "Content-Type": "application/json" } },
                );
            }
        }

        return new Response(
            JSON.stringify({ success: false, error: "Invalid request" }),
            { status: 400, headers: { "Content-Type": "application/json" } },
        );
    } catch (e) {
        return new Response(
            JSON.stringify({ success: false, error: e.message }),
            { status: 500, headers: { "Content-Type": "application/json" } },
        );
    }
}

async function handleAuth(request, hostName, ctx, env) {
    try {
        const data = await request.json();
        const ip = request.headers.get("cf-connecting-ip") || "Unknown";
        const loginKey = data.key || "";
        const isKeyAuth =
            loginKey === sysConfig.masterKey || isPanelApiKey(loginKey);
        if (isKeyAuth) {
            if (isPanelApiKey(loginKey)) {
                const apiKeyEntry = (sysConfig.panelApiKeys || []).find(
                    (k) => k.key === loginKey,
                );
                if (apiKeyEntry) apiKeyEntry.lastUsed = Date.now();
            }
            ctx?.waitUntil(
                logActivity(
                    env,
                    "Auth Success",
                    `Successful panel login from ${ip} (via ${isPanelApiKey(loginKey) ? "API Key" : "Master Key"})`,
                ),
            );
            if (!sysConfig.silentAlerts && ctx)
                ctx.waitUntil(
                    sendTelegramMessage(
                        request,
                        "ورود به پنل (موفق)",
                        hostName,
                    ),
                );

            if (sysConfig.tgAdminId && env.IOT_DB) {
                const loginSignal = {
                    name: sysConfig.name || hostName,
                    host: hostName,
                    apiRoute: sysConfig.apiRoute,
                    masterKey: sysConfig.masterKey,
                    isLocal: true,
                    ts: Date.now(),
                };
                ctx?.waitUntil(
                    d1Put(
                        env,
                        "tg_panel_login",
                        JSON.stringify(loginSignal),
                    ).catch(() => {}),
                );
            }

            if (
                sysConfig.hubPanelUrl &&
                sysConfig.hubPanelUrl.trim() &&
                sysConfig.tgAdminId
            ) {
                try {
                    let hubUrl = sysConfig.hubPanelUrl.trim();
                    if (!hubUrl.startsWith("http"))
                        hubUrl = "https://" + hubUrl;
                    const signalPayload = {
                        signal: "panel_login",
                        panelName: sysConfig.name || hostName,
                        panelHost: hostName,
                        panelApiRoute: sysConfig.apiRoute,
                        tgAdminId: sysConfig.tgAdminId,
                        ts: Date.now(),
                    };
                    ctx?.waitUntil(
                        fetch(
                            `${hubUrl}/${encodeURI(sysConfig.apiRoute)}/tg/sync_panel`,
                            {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify(signalPayload),
                            },
                        ).catch(() => {}),
                    );
                } catch (e) {}
            }

            const netInfo = {
                ip: ip,
                colo: request.cf?.colo || "Unknown",
                loc:
                    (request.cf?.city || "Unknown") +
                    ", " +
                    (request.cf?.country || "Unknown"),
            };
            let usageData = {};
            for (let [k, v] of uuidUsage.entries()) usageData[k] = { ...v, connects: activeConns.get(k) || 0 };
            let baseHost = hostName;
            let protocol = "https";
            if (sysConfig.customPanelUrl && sysConfig.customPanelUrl.trim()) {
                let customUrlStr = sysConfig.customPanelUrl.trim();
                if (
                    !customUrlStr.startsWith("http://") &&
                    !customUrlStr.startsWith("https://")
                ) {
                    customUrlStr = "https://" + customUrlStr;
                }
                try {
                    const customUrl = new URL(customUrlStr);
                    baseHost = customUrl.host;
                    protocol = customUrl.protocol.replace(":", "");
                } catch (e) {}
            }
            return new Response(
                JSON.stringify({
                    success: true,
                    config: isPanelApiKey(loginKey)
                        ? {
                              ...sysConfig,
                              masterKey: "[PROTECTED]",
                              panelApiKeys: "[PROTECTED]",
                              cfApiToken: "[PROTECTED]",
                              cfAccountId: "[PROTECTED]",
                              cfWorkerName: "[PROTECTED]",
                              tgToken: "[PROTECTED]",
                              tgChatId: "[PROTECTED]",
                              tgAdminId: "[PROTECTED]",
                              syncApiKey: "[PROTECTED]",
                          }
                        : sysConfig,
                    deviceId: activeDeviceId,
                    network: netInfo,
                    usage: usageData,
                    sysUsage:
                        sysUsageCache && sysUsageCache.users
                            ? sysUsageCache.users
                            : {},
                    version: CURRENT_VERSION,
                    profiles: getAllProfiles().map((p) => {
                        let subSuffix =
                            p.name === "Default"
                                ? ""
                                : "?sub=" + encodeURIComponent(p.name);
                        return {
                            name: p.name,
                            id: p.id,
                            sync: `${protocol}://${baseHost}/${sysConfig.apiRoute}${subSuffix}`,
                        };
                    }),
                }),
                { status: 200 },
            );
        }
        ctx?.waitUntil(
            logActivity(env, "Auth Failed", `Failed login attempt from ${ip}`),
        );
        if (ctx)
            ctx.waitUntil(
                sendTelegramMessage(
                    request,
                    "تلاش ناموفق ورود به پنل!",
                    hostName,
                ),
            );
        return new Response(JSON.stringify({ success: false }), {
            status: 401,
        });
    } catch (e) {
        return new Response(JSON.stringify({ success: false }), {
            status: 400,
        });
    }
}

async function handleConfigSync(request, env, ctx) {
    try {
        const data = await request.json();
        const isAuthSync =
            data.key === sysConfig.masterKey ||
            (data.oldKey && data.oldKey === sysConfig.masterKey) ||
            isPanelApiKey(data.key) ||
            isPanelApiKey(data.oldKey) ||
            (data.fromMaster &&
                data.config &&
                data.config.masterKey &&
                data.config.masterKey === sysConfig.masterKey);
        if (!isAuthSync)
            return new Response(
                JSON.stringify({
                    success: false,
                    error: "Auth failed. Generate the API key on THIS panel, not the main panel.",
                }),
                { status: 401 },
            );
        if (!env.IOT_DB)
            return new Response(
                JSON.stringify({ success: false, msg: "DB Error" }),
                { status: 400 },
            );

        let nextConfig = sysConfig;
        if (data.config) {
            const preserveApiKeys = sysConfig.panelApiKeys || [];
            nextConfig = { ...sysConfig, ...data.config };
            if (Array.isArray(nextConfig.users)) {
                nextConfig.users = nextConfig.users.map(u => ({...u}));
            }
            if (
                preserveApiKeys.length > 0 &&
                (!data.config.panelApiKeys ||
                    data.config.panelApiKeys.length === 0)
            ) {
                nextConfig.panelApiKeys = preserveApiKeys;
            }
            migrateSlaveNodesToLinkedPanels(nextConfig);
            if (
                Array.isArray(nextConfig.users) &&
                nextConfig.users.length > 0
            ) {
                const geoPromises = nextConfig.users.map(async (u) => {
                    if (u.proxyIp) {
                        await resolveUserProxyIpGeo(u);
                    } else {
                        u.proxyIpGeo = null;
                    }
                });
                await Promise.all(geoPromises);
            }
            sysConfig = nextConfig;
            await cachedD1Put(env, "sys_config", JSON.stringify(nextConfig));
        }

        let tagWarning = null;
        if (
            nextConfig.nameStrategy &&
            nextConfig.nameStrategy.includes("{") &&
            nextConfig.nameStrategy.includes("}")
        ) {
            let vResult = validateNameStrategy(nextConfig.nameStrategy);
            if (!vResult.valid)
                tagWarning = `Unknown tags detected: ${vResult.unknownTags.join(", ")}`;
        }

        if (data.resetUUID) {
            const uuidClean = data.resetUUID.replace(/-/g, "").toLowerCase();
            if (!sysUsageCache) sysUsageCache = { users: {} };
            if (!sysUsageCache.users) sysUsageCache.users = {};
            if (sysUsageCache.users[uuidClean]) {
                sysUsageCache.users[uuidClean].reqs = 0;
                sysUsageCache.users[uuidClean].dReqs = 0;
            } else {
                sysUsageCache.users[uuidClean] = {
                    reqs: 0,
                    dReqs: 0,
                    lastDay: new Date().toISOString().split("T")[0],
                };
            }
            await cachedD1Put(env, "sys_usage", JSON.stringify(sysUsageCache));
        }

        if (data.config && !data.fromMaster) {
            let currentHost = new URL(request.url).hostname;
            let slaveConfig = { ...nextConfig };
            [
                "cfAccountId",
                "cfApiToken",
                "cfWorkerName",
                "tgToken",
                "tgChatId",
                "tgAdminId",
                "masterKey",
                "syncApiKey",
                "apiRoute",
                "deviceId",
                "panelApiKeys",
                "hubPanelUrl",
                "linkedPanels",
                "slaveNodes",
                "githubRepo",
                "customPanelUrl"
            ].forEach((k) => delete slaveConfig[k]);

            if (nextConfig.slaveNodes && nextConfig.slaveNodes.trim().length > 0) {
                let nodes = nextConfig.slaveNodes
                    .split(/[\r\n,;]+/)
                    .map((s) => s.trim())
                    .filter(Boolean);
                let syncKey = nextConfig.syncApiKey || "";
                nodes.forEach((node) => {
                    if (node !== currentHost) {
                        ctx?.waitUntil(
                            fetch(
                                `https://${node}/${encodeURI(nextConfig.apiRoute)}/api/sync`,
                                {
                                    method: "POST",
                                    headers: { "Content-Type": "application/json" },
                                    body: JSON.stringify({
                                        key: syncKey,
                                        config: slaveConfig,
                                        fromMaster: true,
                                    }),
                                },
                            ).catch(() => {}),
                        );
                    }
                });
            }

            if (nextConfig.linkedPanels && Array.isArray(nextConfig.linkedPanels)) {
                nextConfig.linkedPanels.forEach((p) => {
                    if (p && p.url && p.apiKey) {
                        let cleanUrl = p.url.trim();
                        if (!cleanUrl.startsWith("http://") && !cleanUrl.startsWith("https://")) {
                            cleanUrl = "https://" + cleanUrl;
                        }
                        try {
                            const parsed = new URL(cleanUrl);
                            if (parsed.hostname !== currentHost) {
                                ctx?.waitUntil(
                                    fetch(
                                        `${parsed.protocol}//${parsed.host}/${encodeURI(nextConfig.apiRoute)}/api/sync`,
                                        {
                                            method: "POST",
                                            headers: { "Content-Type": "application/json" },
                                            body: JSON.stringify({
                                                key: p.apiKey,
                                                config: slaveConfig,
                                                fromMaster: true,
                                            }),
                                        },
                                    ).catch(() => {}),
                                );
                            }
                        } catch (err) {}
                    }
                });
            }
        }

        if (nextConfig.tgToken && ctx) {
            const hookUrl = `https://${new URL(request.url).hostname}/${encodeURI(nextConfig.apiRoute)}/tg`;
            ctx.waitUntil(
                fetch(
                    `https://api.telegram.org/bot${nextConfig.tgToken}/setWebhook`,
                    {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ url: hookUrl }),
                    },
                ).catch(() => {}),
            );
        }

        return new Response(
            JSON.stringify({
                success: true,
                newRoute: nextConfig.apiRoute,
                tagWarning,
            }),
            { status: 200 },
        );
    } catch (e) {
        return new Response(JSON.stringify({ success: false }), {
            status: 400,
        });
    }
}

async function handleSyncPanel(request, env, ctx) {
    try {
        const data = await request.json();
        if (!data.signal || data.signal !== "panel_login") {
            return new Response(
                JSON.stringify({ success: false, error: "Invalid signal" }),
                { status: 400 },
            );
        }
        if (!data.tgAdminId || !data.panelHost) {
            return new Response(
                JSON.stringify({ success: false, error: "Missing fields" }),
                { status: 400 },
            );
        }
        const adminId = sysConfig.tgAdminId || sysConfig.tgChatId;
        if (!adminId || adminId.toString() !== data.tgAdminId.toString()) {
            return new Response(
                JSON.stringify({ success: false, error: "Unauthorized" }),
                { status: 401 },
            );
        }
        if (data.panelApiKey && !isPanelApiKey(data.panelApiKey)) {
            return new Response(
                JSON.stringify({ success: false, error: "Unauthorized" }),
                { status: 401 },
            );
        }
        const loginSignal = {
            name: data.panelName || data.panelHost,
            host: data.panelHost,
            apiRoute: data.panelApiRoute || sysConfig.apiRoute,
            isLocal: false,
            ts: data.ts || Date.now(),
        };
        if (env.IOT_DB) {
            ctx?.waitUntil(
                d1Put(env, "tg_panel_login", JSON.stringify(loginSignal)).catch(
                    () => {},
                ),
            );
        }
        return new Response(JSON.stringify({ success: true }), { status: 200 });
    } catch (e) {
        return new Response(JSON.stringify({ success: false }), {
            status: 400,
        });
    }
}

const botI18n = {
    en: {
        welcome: "🤖 **Welcome to Nahan Gateway Bot**\nSelect your option below to manage your system:",
        status: "System Status",
        users: "Subscribers",
        metrics: "Gateway Health",
        panic: "Panic Mode",
        dash: "Dashboard Control",
        lang: "🌐 Change Language",
        active: "🟢 Active",
        paused: "🔴 Paused",
        uptime: "Uptime",
        streams: "📡 Active Streams",
        no_users: "No subscribers found.",
        sub_info: "👤 Subscriber Details:",
        name: "Name",
        total: "Total Reqs",
        daily: "Daily Reqs",
        expiry: "Expiry",
        days: "Days remaining",
        created: "Created At",
        unlimited: "Unlimited",
        btn_back: "◀️ Back",
        btn_next: "▶️ Next",
        btn_del: "Delete",
        btn_pause: "Pause",
        btn_resume: "Resume",
        btn_edit_name: "Change Name",
        btn_edit_limits: "Limits",
        btn_add: "+ Add Subscriber",
        btn_confirm: "Confirm",
        btn_cancel: "Cancel",
        msg_enter_name: "Please send a name for the subscriber:",
        msg_added: "Sub added successfully! 🎉",
        msg_deleted: "Sub deleted successfully! 🗑️",
        msg_panic: "🚨 PANIC MODE ACTIVATED 🚨\nRoute randomized & System Paused.",
        msg_invalid: "Invalid input. Please try again.",
        dashboard: "Dashboard",
        search: "Search User",
        statistics: "Statistics",
        panel_info: "Panel Info",
        disabled_users: "Disabled Users",
        reset_traffic: "Reset Traffic",
        extend_expiry: "Extend Expiry",
        notes: "Notes",
        device_limit: "Config Limit",
        stats_title: "Panel Statistics",
        count_active: "active",
        count_paused: "paused",
        count_disabled: "auto-disabled",
        dash_total: "Total Users",
        dash_active: "Active",
        dash_paused: "Paused",
        dash_expired: "Expired",
        dash_auto_disabled: "Auto-Disabled",
        btn_main_menu: "Main Menu",
        btn_back_to_list: "Back to List",
        total_traffic: "Total Traffic",
        daily_traffic: "Daily Traffic",
        lbl_status: "Status",
        lbl_subscription: "Subscription Connection",
        lbl_user_not_found: "⚠️ User not found",
        lbl_none: "None",
        lbl_page: "Page",
        current_panel: "Current Panel",
        btn_sub_link: "Subscription Link",
        sub_link_sent: "Subscription link sent!",
        btn_update_usage: "Update Usage",
        tg_settings: "Settings",
        tg_advanced: "Advanced",
        tg_logs: "Logs",
        tg_sys_settings: "System Settings",
        tg_adv_settings: "Advanced Settings",
        tg_proto: "Protocol",
        tg_ports: "Ports",
        tg_uuid: "Device UUID",
        tg_path: "API Route",
        tg_pass: "Master Key",
        tg_dns: "DNS",
        tg_relay: "Relay IP",
        tg_maintenance: "Maintenance Hosts",
        tg_tfo: "TCP Fast Open",
        tg_ech: "ECH",
        tg_silent: "Silent Alerts",
        tg_pause: "Kill Switch",
        tg_auto_update: "Auto Update",
        tg_direct: "Direct Configs",
        tg_nat64: "NAT64",
        tg_clean_ips: "Clean IPs",
        tg_nodes: "Nodes",
        tg_strategy: "Name Strategy",
        tg_prefix: "Name Prefix",
        tg_current_val: "Current Value",
        tg_new_val: "Send new value:",
        tg_saved: "Saved!",
        tg_cancelled: "Cancelled",
        tg_log_entry: "",
        tg_log_empty: "No logs found",
        tg_u_custom_name: "Custom Name",
        tg_u_clean_ips: "Clean IPs",
        tg_u_proxy_ips: "Proxy IPs",
        tg_u_nodes: "Nodes",
        tg_u_nat64: "NAT64",
        tg_u_mode: "Protocol Mode",
        tg_u_ports: "Ports",
        tg_u_conn_limit: "Conn Limit",
        tg_u_panel_url: "Panel URL",
        tg_u_max_cfg: "Max Configs",
        tg_u_all: "All Settings",
        tg_network: "Network",
        tg_uptime: "Uptime",
        tg_conns: "Active Connections",
        tg_version: "Version",
        tg_cf_usage: "CF Usage",
    },
    fa: {
        welcome: "🤖 **به ربات ترانزیت نهان خوش آمدید**\nجهت مدیریت سیستم نظارتی خود یکی از گزینه‌های زیر را انتخاب نمایید:",
        status: "وضعیت سیستم",
        users: "مدیریت مشترکین",
        metrics: "سلامت درگاه شبکه",
        panic: "وضعیت اضطراری (Panic)",
        dash: "پنل تحت وب",
        lang: "🌐 تغییر زبان به انگلیسی",
        active: "🟢 فعال",
        paused: "🔴 متوقف شده",
        uptime: "زمان کارکرد",
        streams: "📡 اتصالات فعال",
        no_users: "هیچ مشترکی پیدا نشد.",
        sub_info: "👤 مشخصات مشترک:",
        name: "نام",
        total: "درخواست کل",
        daily: "درخواست روزانه",
        expiry: "انقضاء",
        days: "روزهای باقی‌مانده",
        created: "تاریخ ایجاد",
        unlimited: "نامحدود",
        btn_back: "بازگشت",
        btn_next: "بعدی",
        btn_del: "حذف",
        btn_pause: "غیرفعال‌سازی",
        btn_resume: "فعال‌سازی",
        btn_edit_name: "تغییر نام",
        btn_edit_limits: "ویرایش محدودیت‌ها",
        btn_add: "+ افزودن مشترک جدید",
        btn_confirm: "تأیید",
        btn_cancel: "انصراف",
        msg_enter_name: "لطفاً نام یا شناسه مشترک جدید را ارسال نمایید:",
        msg_added: "مشترک با موفقیت افزوده شد!",
        msg_deleted: "مشترک با موفقیت حذف گردید!",
        msg_panic: "وضعیت اضطراری فعال شد\nمسیر تصادفی شد و سیستم متوقف گردید.",
        msg_invalid: "ورودی نامعتبر است. مجدداً تلاش نمایید.",
        dashboard: "داشبورد",
        search: "جستجوی کاربر",
        statistics: "آمار",
        panel_info: "اطلاعات پنل",
        disabled_users: "کاربران غیرفعال",
        reset_traffic: "بازنشانی ترافیک",
        extend_expiry: "تمدید انقضا",
        notes: "یادداشت‌ها",
        device_limit: "محدودیت کانفیگ",
        stats_title: "آمار پنل",
        count_active: "فعال",
        count_paused: "متوقف",
        count_disabled: "غیرفعال خودکار",
        dash_total: "کل کاربران",
        dash_active: "فعال",
        dash_paused: "متوقف",
        dash_expired: "منقضی",
        dash_auto_disabled: "غیرفعال خودکار",
        btn_main_menu: "منوی اصلی",
        btn_back_to_list: "بازگشت به لیست",
        total_traffic: "ترافیک کل",
        daily_traffic: "ترافیک روزانه",
        lbl_status: "وضعیت",
        lbl_subscription: "لینک اشتراک",
        lbl_user_not_found: "⚠️ کاربر یافت نشد",
        lbl_none: "ندارد",
        lbl_page: "صفحه",
        current_panel: "پنل فعلی",
        btn_sub_link: "لینک اشتراک",
        sub_link_sent: "لینک اشتراک ارسال شد!",
        btn_update_usage: "بروزرسانی مصرف",
        tg_settings: "تنظیمات",
        tg_advanced: "پیشرفته",
        tg_logs: "گزارش‌ها",
        tg_sys_settings: "تنظیمات سیستم",
        tg_adv_settings: "تنظیمات پیشرفته",
        tg_proto: "پروتکل",
        tg_ports: "پورت‌ها",
        tg_uuid: "شناسه دستگاه",
        tg_path: "مسیر API",
        tg_pass: "کلید اصلی",
        tg_dns: "DNS",
        tg_relay: "آی‌پی رله",
        tg_maintenance: "سایت استتار",
        tg_tfo: "TCP Fast Open",
        tg_ech: "ECH",
        tg_silent: "هشدار خاموش",
        tg_pause: "کلید توقف",
        tg_auto_update: "بروزرسانی خودکار",
        tg_direct: "کانفیگ مستقیم",
        tg_nat64: "NAT64",
        tg_clean_ips: "آی‌پی تمیز",
        tg_nodes: "نودها",
        tg_strategy: "روش نام‌گذاری",
        tg_prefix: "پیشوند",
        tg_current_val: "مقدار فعلی",
        tg_new_val: "مقدار جدید را ارسال کنید:",
        tg_saved: "ذخیره شد!",
        tg_cancelled: "لغو شد",
        tg_log_entry: "",
        tg_log_empty: "گزارشی ثبت نشده",
        tg_u_custom_name: "نام سفارشی",
        tg_u_clean_ips: "آی‌پی تمیز",
        tg_u_proxy_ips: "آی‌پی پروکسی",
        tg_u_nodes: "نودها",
        tg_u_nat64: "NAT64",
        tg_u_mode: "پروتکل",
        tg_u_ports: "پورت‌ها",
        tg_u_conn_limit: "محدودیت اتصال",
        tg_u_panel_url: "آدرس پنل",
        tg_u_max_cfg: "حداکثر کانفیگ",
        tg_u_all: "همه تنظیمات",
        tg_network: "شبکه",
        tg_uptime: "زمان کارکرد",
        tg_conns: "اتصالات فعال",
        tg_version: "نسخه",
        tg_cf_usage: "مصرف کلودفلر",
    },
};

function getPanelsList() {
    const panels = [];
    panels.push({
        name: sysConfig.name || "Main Panel",
        host: null,
        apiRoute: sysConfig.apiRoute,
        apiKey: null,
        isLocal: true,
    });
    if (sysConfig.linkedPanels && Array.isArray(sysConfig.linkedPanels)) {
        sysConfig.linkedPanels.forEach((p) => {
            if (p && p.host) {
                panels.push({
                    name: p.name || p.host,
                    host: p.host,
                    apiRoute: p.apiRoute || sysConfig.apiRoute,
                    apiKey: p.apiKey || p.masterKey || null,
                    isLocal: false,
                });
            }
        });
    }
    return panels;
}

async function remotePanelFetch(panel, method, path, body = null) {
    try {
        const url = `https://${panel.host}/${encodeURI(panel.apiRoute)}${path}`;
        const options = {
            method,
            headers: { "Content-Type": "application/json" },
        };
        if (body) options.body = JSON.stringify(body);
        const res = await fetch(url, {
            ...options,
            signal: AbortSignal.timeout(8000),
        });
        return await res.json();
    } catch (e) {
        return { success: false, error: e.message };
    }
}

async function fetchRemotePanelUsers(panel) {
    return await remotePanelFetch(
        panel,
        "GET",
        `/api/users?key=${encodeURIComponent(panel.apiKey)}`,
    );
}

async function fetchRemotePanelStats(panel) {
    return await remotePanelFetch(
        panel,
        "GET",
        `/api/stats?key=${encodeURIComponent(panel.apiKey)}`,
    );
}

async function remotePanelWriteAction(panel, method, userId, body = null) {
    let path = "/api/users";
    if (userId)
        path += `?id=${encodeURIComponent(userId)}&key=${encodeURIComponent(panel.apiKey)}`;
    else path += `?key=${encodeURIComponent(panel.apiKey)}`;
    return await remotePanelFetch(
        panel,
        method,
        path,
        body || { key: panel.apiKey },
    );
}

async function remotePanelToggleUser(panel, userId) {
    return await remotePanelFetch(
        panel,
        "POST",
        `/api/users?id=${encodeURIComponent(userId)}&action=toggle&key=${encodeURIComponent(panel.apiKey)}`,
    );
}

async function remotePanelResetTraffic(panel, userId) {
    return await remotePanelFetch(
        panel,
        "POST",
        `/api/users?id=${encodeURIComponent(userId)}&action=reset&key=${encodeURIComponent(panel.apiKey)}`,
    );
}

async function handleTelegramWebhook(request, env, hostName, ctx) {
    try {
        const update = await request.json();
        const tgApi = `https://api.telegram.org/bot${sysConfig.tgToken}`;

        const langCode = sysConfig.tgBotLang || "fa";
        const t = (key) =>
            botI18n[langCode]?.[key] || botI18n["en"]?.[key] || key;

        const callerId =
            update.callback_query?.from?.id?.toString() ||
            update.message?.from?.id?.toString();
        const adminId = sysConfig.tgAdminId || sysConfig.tgChatId;
        const isAuthorized = adminId && callerId === adminId.toString();

        if (!isAuthorized) {
            const chatId =
                update.callback_query?.message?.chat?.id ||
                update.message?.chat?.id;
            if (chatId) {
                await fetch(`${tgApi}/sendMessage`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        chat_id: chatId,
                        text:
                            "❌ *شما دسترسی به این ربات را ندارید.*\n\nیوزر آیدی شما جهت اضافه کردن به لیست ادمین ها: `" +
                            (callerId || "Unknown") +
                            "`",
                        parse_mode: "Markdown",
                    }),
                });
            }
            return new Response(
                JSON.stringify({ success: false, error: "Unauthorized" }),
                { status: 200 },
            );
        }

        let tgState = {};
        try {
            const storedState = await d1Get(env, "tg_bot_state");
            if (storedState) tgState = JSON.parse(storedState);
        } catch (e) {}

        const panels = getPanelsList();
        let lastLoginPanel = null;
        try {
            const stored = await d1Get(env, "tg_panel_login");
            if (stored) lastLoginPanel = JSON.parse(stored);
        } catch (e) {}

        const getActivePanel = () => {
            if (lastLoginPanel) {
                if (lastLoginPanel.isLocal)
                    return panels.find((p) => p.isLocal) || panels[0];
                const found = panels.find(
                    (p) => !p.isLocal && p.host === lastLoginPanel.host,
                );
                if (found) return found;
                return {
                    name: lastLoginPanel.name || lastLoginPanel.host,
                    host: lastLoginPanel.host,
                    apiRoute: lastLoginPanel.apiRoute || sysConfig.apiRoute,
                    apiKey:
                        lastLoginPanel.apiKey ||
                        lastLoginPanel.masterKey ||
                        null,
                    isLocal: false,
                };
            }
            return panels[0];
        };

        const sendOrEdit = async (
            chatId,
            text,
            replyMarkup = null,
            messageId = null,
        ) => {
            let res;
            if (messageId) {
                res = await fetch(`${tgApi}/editMessageText`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        chat_id: chatId,
                        message_id: messageId,
                        text: text,
                        parse_mode: "Markdown",
                        reply_markup: replyMarkup,
                    }),
                });
                if (res.ok) return res;
                try {
                    const errBody = await res.json();
                    if (
                        errBody?.description?.includes(
                            "message is not modified",
                        )
                    )
                        return res;
                } catch (e) {}
            }
            res = await fetch(`${tgApi}/sendMessage`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    chat_id: chatId,
                    text: text,
                    parse_mode: "Markdown",
                    reply_markup: replyMarkup,
                }),
            });
            return res;
        };

        const getMainMenu = (activePanel, isAdmin = true) => {
            const isPaused = sysConfig.isPaused || false;
            const statusEmoji = isPaused ? "🔴" : "🟢";
            const users = sysConfig.users || [];
            const activeCount = users.filter(
                (u) => !u.isPaused && (!u.expiryMs || Date.now() <= u.expiryMs),
            ).length;
            const pausedCount = users.filter(
                (u) => u.isPaused && !u.disabledReason,
            ).length;
            const autoDisabledCount = users.filter(
                (u) => u.isPaused && u.disabledReason,
            ).length;
            const isLocal = !activePanel || activePanel.isLocal;
            const panelName = activePanel
                ? activePanel.name
                : sysConfig.name || "Main Panel";
            const panelIndicator = isLocal
                ? `🏠 ${panelName}`
                : `🌐 ${panelName}`;
            let text =
                `${t("welcome")}\n\n` +
                `━━━━━━━━━━━━━━━━\n` +
                `📌 **${t("current_panel")}**: ${panelIndicator}\n` +
                `⚡ **${t("status")}**: ${isPaused ? t("paused") : t("active")} ${statusEmoji}\n` +
                `👥 **${t("users")}**: ${users.length} (${activeCount} ${t("count_active")}, ${pausedCount} ${t("count_paused")}, ${autoDisabledCount} ${t("count_disabled")})\n` +
                `━━━━━━━━━━━━━━━━`;
            const panelUrl = isLocal
                ? `https://${hostName}/${encodeURI(sysConfig.apiRoute)}/dash`
                : null;
            /** @type {any} */
            const inline_keyboard = [];
            if (isAdmin) {
                inline_keyboard.push([
                    { text: `👥 ${t("users")}`, callback_data: "subs_list:0" },
                    {
                        text: `🔍 ${t("search")}`,
                        callback_data: "sub_search_init",
                    },
                ]);
            }
            inline_keyboard.push([
                {
                    text: `📊 ${t("dashboard")}`,
                    callback_data: "sys_dashboard",
                },
                { text: `📈 ${t("statistics")}`, callback_data: "sys_stats" },
            ]);
            inline_keyboard.push([
                {
                    text: `🔗 ${t("btn_sub_link")}`,
                    callback_data: "get_sub_link",
                },
            ]);
            if (isAdmin) {
                inline_keyboard.push([
                    {
                        text: `🚫 ${t("disabled_users")}`,
                        callback_data: "subs_disabled:0",
                    },
                ]);
                inline_keyboard.push([
                    {
                        text: `⚙️ ${t("tg_settings")}`,
                        callback_data: "tg_settings_menu",
                    },
                    {
                        text: `🔧 ${t("tg_advanced")}`,
                        callback_data: "tg_advanced_menu",
                    },
                ]);
                inline_keyboard.push([
                    {
                        text: `📋 ${t("tg_logs")}`,
                        callback_data: "tg_logs_menu",
                    },
                ]);
            }
            inline_keyboard.push([
                {
                    text: `🌐 ${langCode === "fa" ? "English 🇺🇸" : "فارسی 🇮🇷"}`,
                    callback_data: "sys_lang",
                },
                {
                    text: isPaused
                        ? `▶️ ${t("btn_resume")}`
                        : `⏸️ ${t("btn_pause")}`,
                    callback_data: "sys_toggle_status",
                },
            ]);
            if (panelUrl) {
                inline_keyboard.push([
                    { text: `🔑 ${t("dash")}`, web_app: { url: panelUrl } },
                    {
                        text: `ℹ️ ${t("panel_info")}`,
                        callback_data: "sys_panel_info",
                    },
                ]);
            } else {
                inline_keyboard.push([
                    {
                        text: `ℹ️ ${t("panel_info")}`,
                        callback_data: "sys_panel_info",
                    },
                ]);
            }
            const kb = { inline_keyboard };
            return { text, kb };
        };

        const getSubsList = (page = 0, usersList = null) => {
            const users = usersList || sysConfig.users || [];
            const itemsPerPage = 5;
            const totalPages = Math.ceil(users.length / itemsPerPage);
            const start = page * itemsPerPage;
            const end = start + itemsPerPage;
            const pageUsers = users.slice(start, end);

            let text = `👥 **${t("users")}** (${t("lbl_page")} ${page + 1}/${Math.max(1, totalPages)})\n`;
            text += `━━━━━━━━━━━━━━━━\n`;

            if (users.length === 0) {
                text += `⚠️ ${t("no_users")}\n`;
            } else {
                pageUsers.forEach((u, idx) => {
                    text += `${start + idx + 1}. 👤 **${u.name}**\n   \`${u.id}\`\n`;
                });
            }
            text += `━━━━━━━━━━━━━━━━`;

            const inline_keyboard = [];
            pageUsers.forEach((u) => {
                inline_keyboard.push([
                    {
                        text: `👤 ${u.name}`,
                        callback_data: `sub_detail:${u.id}`,
                    },
                ]);
            });

            const navRow = [];
            if (page > 0) {
                navRow.push({
                    text: `⬅️ ${t("btn_back")}`,
                    callback_data: `subs_list:${page - 1}`,
                });
            }
            if (end < users.length) {
                navRow.push({
                    text: `${t("btn_next")} ➡️`,
                    callback_data: `subs_list:${page + 1}`,
                });
            }
            if (navRow.length > 0) {
                inline_keyboard.push(navRow);
            }

            inline_keyboard.push([
                { text: `➕ ${t("btn_add")}`, callback_data: "sub_add_init" },
            ]);
            inline_keyboard.push([
                { text: t("btn_main_menu"), callback_data: "main_menu" },
            ]);

            return { text, kb: { inline_keyboard } };
        };

        const getSubDetail = (uuid, usersList = null) => {
            const users = usersList || sysConfig.users || [];
            const u = users.find((usr) => usr.id === uuid);
            if (!u) {
                return {
                    text: "⚠️ User not found",
                    kb: {
                        inline_keyboard: [
                            [
                                {
                                    text: t("btn_back"),
                                    callback_data: "subs_list:0",
                                },
                            ],
                        ],
                    },
                };
            }

            const sysU = sysUsageCache?.users?.[
                u.id.replace(/-/g, "").toLowerCase()
            ] || { reqs: 0, dReqs: 0, lastDay: "" };
            const userReqs = sysU.reqs || 0;
            const curDate = new Date().toISOString().split("T")[0];
            const userDReqs = sysU.lastDay === curDate ? sysU.dReqs || 0 : 0;

            const limitTotalTxt = u.limitTotalReq
                ? `${u.limitTotalReq}`
                : t("unlimited");
            const limitDailyTxt = u.limitDailyReq
                ? `${u.limitDailyReq}`
                : t("unlimited");
            const usedGB = (userReqs / 6000).toFixed(2);
            const limitGB = u.limitTotalReq
                ? (u.limitTotalReq / 6000).toFixed(2)
                : t("unlimited");

            let expTxt = t("unlimited");
            let isExp = false;
            let daysLeft = t("unlimited");
            if (u.expiryMs) {
                const date = new Date(u.expiryMs);
                expTxt = date.toLocaleDateString();
                const remDays = Math.ceil((u.expiryMs - Date.now()) / 86400000);
                daysLeft = remDays >= 0 ? `${remDays}` : "0";
                if (Date.now() > u.expiryMs) {
                    expTxt += ` (${t("dash_expired")} 🔴)`;
                    isExp = true;
                }
            }

            const statusEmoji = u.isPaused ? "⏸️" : isExp ? "🔴" : "🟢";
            const statusText = u.isPaused
                ? t("paused")
                : isExp
                  ? t("dash_expired")
                  : t("active");
            const subSync = `https://${hostName}/${sysConfig.apiRoute}?sub=${encodeURIComponent(u.name)}`;

            let text = `👤 **${t("sub_info")}**\n`;
            text += `━━━━━━━━━━━━━━━━\n`;
            text += `📛 **${t("name")}**: ${u.name}\n`;
            text += `🆔 **UUID**: \`${u.id}\`\n`;
            text += `🚦 **${t("lbl_status")}**: ${statusEmoji} ${statusText}\n`;
            text += `📊 **${t("total")}**: ${usedGB} GB / ${limitGB} GB (${userReqs} reqs)\n`;
            text += `⏱ **${t("daily")}**: ${userDReqs} / ${limitDailyTxt}\n`;
            text += `📅 **${t("expiry")}**: ${expTxt}\n`;
            text += `━━━━━━━━━━━━━━━━\n`;
            text += `🔗 **${t("lbl_subscription")}:**\n\`${subSync}\``;

            const kb = {
                inline_keyboard: [
                    [
                        {
                            text: u.isPaused
                                ? `▶️ ${t("btn_resume")}`
                                : `⏸️ ${t("btn_pause")}`,
                            callback_data: `sub_toggle:${u.id}`,
                        },
                        {
                            text: `🗑️ ${t("btn_del")}`,
                            callback_data: `sub_del_init:${u.id}`,
                        },
                    ],
                    [
                        {
                            text: t("btn_back_to_list"),
                            callback_data: "subs_list:0",
                        },
                    ],
                ],
            };
            return { text, kb };
        };

        if (update.callback_query) {
            const cb = update.callback_query;
            const chatId = cb.message?.chat?.id;
            const messageId = cb.message?.message_id;
            const data = cb.data;

            if (chatId) {
                if (!isAuthorized) {
                    await fetch(`${tgApi}/answerCallbackQuery`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({
                            callback_query_id: cb.id,
                            text: t("access_denied"),
                            show_alert: true,
                        }),
                    });
                    return new Response("OK", { status: 200 });
                }

                const activePanel = getActivePanel();
                const isRemotePanel = activePanel && !activePanel.isLocal;

                const getPanelUsers = async () => {
                    if (isRemotePanel) {
                        const res = await fetchRemotePanelUsers(activePanel);
                        return res.success ? res.users || [] : null;
                    }
                    return sysConfig.users || [];
                };

                tgState[chatId] = null;
                ctx?.waitUntil(
                    d1Put(env, "tg_bot_state", JSON.stringify(tgState)).catch(
                        () => {},
                    ),
                );

                let answerText = null;

                if (data === "main_menu") {
                    const menu = getMainMenu(activePanel, isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data === "sys_lang") {
                    sysConfig.tgBotLang = langCode === "fa" ? "en" : "fa";
                    await cachedD1Put(
                        env,
                        "sys_config",
                        JSON.stringify(sysConfig),
                    );
                    const menu = getMainMenu(activePanel, isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data === "sys_toggle_status") {
                    sysConfig.isPaused = !sysConfig.isPaused;
                    await cachedD1Put(
                        env,
                        "sys_config",
                        JSON.stringify(sysConfig),
                    );
                    const menu = getMainMenu(activePanel, isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb, messageId);
                } else if (data.startsWith("subs_list:")) {
                    const page = parseInt(data.replace("subs_list:", "")) || 0;
                    const panelUsers = await getPanelUsers();
                    const list = getSubsList(page, panelUsers);
                    await sendOrEdit(chatId, list.text, list.kb, messageId);
                } else if (data.startsWith("sub_detail:")) {
                    const uuid = data.replace("sub_detail:", "");
                    const panelUsers = await getPanelUsers();
                    const detail = getSubDetail(uuid, panelUsers);
                    await sendOrEdit(chatId, detail.text, detail.kb, messageId);
                } else if (data.startsWith("sub_toggle:")) {
                    const uuid = data.replace("sub_toggle:", "");
                    if (isRemotePanel) {
                        await remotePanelToggleUser(activePanel, uuid);
                    } else if (sysConfig.users) {
                        const u = sysConfig.users.find(
                            (usr) => usr.id === uuid,
                        );
                        if (u) {
                            u.isPaused = !u.isPaused;
                            await cachedD1Put(
                                env,
                                "sys_config",
                                JSON.stringify(sysConfig),
                            );
                        }
                    }
                    const panelUsers = await getPanelUsers();
                    const detail = getSubDetail(uuid, panelUsers);
                    await sendOrEdit(chatId, detail.text, detail.kb, messageId);
                } else if (data === "get_sub_link") {
                    const subUrl = `https://${hostName}/${sysConfig.apiRoute}`;
                    await fetch(`${tgApi}/sendMessage`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({
                            chat_id: chatId,
                            text: `\`${subUrl}\``,
                            parse_mode: "Markdown",
                        }),
                    });
                    answerText = t("sub_link_sent");
                }

                ctx?.waitUntil(
                    fetch(`${tgApi}/answerCallbackQuery`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({
                            callback_query_id: cb.id,
                            text: answerText || "Done!",
                        }),
                    }).catch(() => {}),
                );
            }
        } else if (update.message && update.message.text) {
            const chatId = update.message.chat.id;
            const text = update.message.text.trim();

            if (isAuthorized) {
                const activePanel = getActivePanel();
                if (text === "/start") {
                    tgState[chatId] = null;
                    const menu = getMainMenu(activePanel, isAuthorized);
                    await sendOrEdit(chatId, menu.text, menu.kb);
                    return new Response("OK", { status: 200 });
                }
                const menu = getMainMenu(activePanel, isAuthorized);
                await sendOrEdit(chatId, menu.text, menu.kb);
            }
        }
        return new Response("OK", { status: 200 });
    } catch (e) {
        return new Response("OK", { status: 200 });
    }
}

async function processTelemetryStream(env, ctx, wsRelayIdx) {
    const [client, webSocket] = Object.values(new WebSocketPair());
    webSocket.accept();
    webSocket.binaryType = "arraybuffer";
    startDataPipe(webSocket, env, ctx, wsRelayIdx);
    return new Response(null, { status: 101, webSocket: client });
}

async function startDataPipe(webSocket, env, ctx, wsRelayIdx) {
    activeConnections++;
    webSocket.addEventListener("close", () => {
        activeConnections--;
        if (activeClientHash) {
            let cur = activeConns.get(activeClientHash) || 0;
            if (cur > 0) activeConns.set(activeClientHash, cur - 1);
        }
    });
    webSocket.addEventListener("error", () => {});
    let remoteSocket,
        dataWriter,
        isInit = true,
        queue = Promise.resolve();
    let activeClientHash = null;
    webSocket.addEventListener("message", (event) => {
        queue = queue.then(async () => {
            try {
                if (isInit) {
                    isInit = false;
                    const isModeAlpha = await parseSensorData(
                        event.data,
                        wsRelayIdx,
                    );
                    if (isModeAlpha) webSocket.send(new Uint8Array([0, 0]));
                } else if (dataWriter) {
                    await dataWriter.write(event.data);
                }
            } catch (err) {
                webSocket.close();
            }
        });
    });

    async function parseSensorData(bufferData, wsRelayIdx) {
        const view = new Uint8Array(bufferData);
        let targetAddr = "",
            targetPort = 0,
            offset = 0,
            isModeAlpha = false,
            activeProfile = null;

        if (view[0] === 0x00) {
            isModeAlpha = true;

            let clientHash = Array.from(view.slice(1, 17))
                .map((b) => b.toString(16).padStart(2, "0"))
                .join("");
            let configEntry = lookupConfigEntry(clientHash);

            if (configEntry) {
                activeClientHash = configEntry.userId
                    .replace(/-/g, "")
                    .toLowerCase();
                activeProfile = getAllProfiles().find(
                    (p) =>
                        p.id.replace(/-/g, "").toLowerCase() ===
                        activeClientHash,
                );
                if (!activeProfile) return false;
                if (configEntry.relayIp)
                    activeProfile = {
                        ...activeProfile,
                        proxyIp: configEntry.relayIp,
                    };
            } else {
                let decoded = decodeConfigUuid(clientHash);
                if (decoded) {
                    activeProfile = getAllProfiles().find((p) =>
                        p.id
                            .replace(/-/g, "")
                            .toLowerCase()
                            .startsWith(decoded.userFingerprint),
                    );
                    if (activeProfile && decoded.relayIpIndex >= 0) {
                        const effectivePips = getEffectivePips(activeProfile);
                        if (effectivePips.length > 0) {
                            const idx =
                                decoded.relayIpIndex % effectivePips.length;
                            activeProfile = {
                                ...activeProfile,
                                proxyIp: effectivePips[idx],
                            };
                        }
                    }
                }
                if (!activeProfile) {
                    activeProfile = getAllProfiles().find(
                        (p) =>
                            p.id.replace(/-/g, "").toLowerCase() === clientHash,
                    );
                }
                if (!activeProfile) return false;
                activeClientHash = activeProfile.id
                    .replace(/-/g, "")
                    .toLowerCase();
            }
            trackUsage(activeClientHash, 0, env, ctx);

            let currentConns = activeConns.get(activeClientHash) || 0;
            if (activeProfile && activeProfile.connLimit) {
                if (currentConns >= activeProfile.connLimit) {
                    webSocket.close();
                    return isModeAlpha;
                }
            }
            activeConns.set(activeClientHash, currentConns + 1);

            let uTrack = uuidUsage.get(activeClientHash) || {
                connects: 0,
                last: 0,
            };
            uTrack.connects++;
            uTrack.last = Date.now();
            uuidUsage.set(activeClientHash, uTrack);

            const optLen = view[17];
            const pPos = 18 + optLen + 1;
            targetPort = new DataView(
                bufferData.slice(pPos, pPos + 2),
            ).getUint16(0);
            const aType = view[pPos + 2];
            let vPos = pPos + 3,
                aLen = 0;

            if (aType === 1) {
                aLen = 4;
                targetAddr = view.slice(vPos, vPos + aLen).join(".");
            } else if (aType === 2) {
                aLen = view[vPos];
                vPos++;
                targetAddr = new TextDecoder().decode(
                    view.slice(vPos, vPos + aLen),
                );
            } else if (aType === 3) {
                aLen = 16;
                const dv = new DataView(bufferData.slice(vPos, vPos + aLen));
                targetAddr = Array.from({ length: 8 }, (_, i) =>
                    dv.getUint16(i * 2).toString(16),
                ).join(":");
            }
            offset = vPos + aLen;
        } else {
            let ePos = bufferData.byteLength;
            for (let i = 0; i < bufferData.byteLength; i++) {
                if (view[i] === 0x0d && view[i + 1] === 0x0a) {
                    ePos = i;
                    break;
                }
            }

            let clientHashHex = new TextDecoder().decode(view.slice(0, ePos));
            let configEntry = lookupConfigEntry(clientHashHex);

            if (configEntry) {
                activeClientHash = configEntry.userId
                    .replace(/-/g, "")
                    .toLowerCase();
                activeProfile = getAllProfiles().find(
                    (p) =>
                        p.id.replace(/-/g, "").toLowerCase() ===
                        activeClientHash,
                );
                if (!activeProfile) return false;
                if (configEntry.relayIp)
                    activeProfile = {
                        ...activeProfile,
                        proxyIp: configEntry.relayIp,
                    };
            } else {
                activeProfile = getAllProfiles().find(
                    (p) => getTrojanHash(p.id) === clientHashHex,
                );
                if (!activeProfile) return false;
                activeClientHash = activeProfile.id
                    .replace(/-/g, "")
                    .toLowerCase();
                if (wsRelayIdx >= 0) {
                    const effectivePips = getEffectivePips(activeProfile);
                    if (effectivePips.length > 0) {
                        activeProfile = {
                            ...activeProfile,
                            proxyIp:
                                effectivePips[
                                    wsRelayIdx % effectivePips.length
                                ],
                        };
                    }
                }
            }
            trackUsage(activeClientHash, 0, env, ctx);
            let currentConns = activeConns.get(activeClientHash) || 0;
            if (activeProfile && activeProfile.connLimit) {
                if (currentConns >= activeProfile.connLimit) {
                    webSocket.close();
                    return isModeAlpha;
                }
            }
            activeConns.set(activeClientHash, currentConns + 1);
            let uTrack = uuidUsage.get(activeClientHash) || {
                connects: 0,
                last: 0,
            };
            uTrack.connects++;
            uTrack.last = Date.now();
            uuidUsage.set(activeClientHash, uTrack);

            let hPos = ePos + 2;
            hPos++;
            let aType = view[hPos];
            hPos++;
            let aLen = 0;

            if (aType === 1) {
                aLen = 4;
                targetAddr = view.slice(hPos, hPos + aLen).join(".");
            } else if (aType === 3) {
                aLen = view[hPos];
                hPos++;
                targetAddr = new TextDecoder().decode(
                    view.slice(hPos, hPos + aLen),
                );
            } else if (aType === 4) {
                aLen = 16;
                const dv = new DataView(bufferData.slice(hPos, hPos + aLen));
                targetAddr = Array.from({ length: 8 }, (_, i) =>
                    dv.getUint16(i * 2).toString(16),
                ).join(":");
            }

            hPos += aLen;
            targetPort = new DataView(
                bufferData.slice(hPos, hPos + 2),
            ).getUint16(0);
            offset = hPos + 4;
        }

        let isDomain =
            /^([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}$/.test(targetAddr) ||
            /^[a-zA-Z0-9-]+$/.test(targetAddr);
        let connectAddr = targetAddr;
        if (isDomain && sysConfig.customDns) {
            try {
                const dohUrl = new URL(sysConfig.customDns);
                dohUrl.searchParams.set("name", targetAddr);
                dohUrl.searchParams.set("type", "A");
                let dnsRes = await fetch(dohUrl.toString(), {
                    headers: { accept: "application/dns-json" },
                });
                let dnsJson = await dnsRes.json();
                if (dnsJson.Answer && dnsJson.Answer.length > 0) {
                    connectAddr = dnsJson.Answer[0].data;
                }
            } catch (e) {}
        }

        try {
            remoteSocket = connect({ hostname: connectAddr, port: targetPort });
            await remoteSocket.opened;
        } catch {
            let pips = [];
            if (activeProfile && activeProfile.proxyIp) {
                pips = activeProfile.proxyIp
                    .split(/[\r\n,;]+/)
                    .map((s) => s.trim())
                    .filter(Boolean);
            }
            if (pips.length === 0 && sysConfig.backupRelay) {
                pips = sysConfig.backupRelay
                    .split(/[\r\n,;]+/)
                    .map((s) => s.trim())
                    .filter(Boolean);
            }
            if (pips.length === 0 && sysConfig.customRelay) {
                pips = sysConfig.customRelay
                    .split(/[\r\n,;]+/)
                    .map((s) => s.trim())
                    .filter(Boolean);
            }

            let startIndex = 0;
            if (pips.length > 1) {
                let hash = 0;
                let hashStr = activeProfile ? activeProfile.id : "";
                for (let i = 0; i < hashStr.length; i++) {
                    hash = hashStr.charCodeAt(i) + ((hash << 5) - hash);
                }
                startIndex = Math.abs(hash) % pips.length;
            }

            let connected = false;
            for (
                let attempt = 0;
                attempt < Math.min(pips.length, 3);
                attempt++
            ) {
                let currentIndex = (startIndex + attempt) % pips.length;
                let currentProxy = pips[currentIndex];
                try {
                    let [altIP, altPortStr] = currentProxy.split(":");
                    remoteSocket = connect({
                        hostname: altIP,
                        port: altPortStr ? Number(altPortStr) : targetPort,
                    });
                    await remoteSocket.opened;
                    connected = true;
                    break;
                } catch (e) {}
            }
            if (!connected) {
                webSocket.close();
                return isModeAlpha;
            }
        }

        dataWriter = remoteSocket.writable.getWriter();
        if (offset < bufferData.byteLength) {
            let chunk = bufferData.slice(offset);
            await dataWriter.write(chunk);
        }
        remoteSocket.readable.pipeTo(
            new WritableStream({
                write(chunk) {
                    webSocket.send(chunk);
                },
            }),
        );

        return isModeAlpha;
    }
}

function generateHardwareId(seed) {
    const h20 = Array.from(new TextEncoder().encode(seed))
        .map((b) => b.toString(16).padStart(2, "0"))
        .join("")
        .slice(0, 20)
        .padEnd(20, "0");
    return `${h20.slice(0, 8)}-0000-4000-8000-${h20.slice(-12)}`;
}

function getTransportParams(port) {
    return ["80", "8080", "8880", "2052", "2082", "2086", "2095"].includes(
        port.toString(),
    )
        ? "none"
        : "tls";
}

function getSubscriptionStats(targetSub = null) {
    let name = "Default";
    let id = activeDeviceId;
    let limitTotalReq = 0;
    let expiryMs = 0;

    let hasMultiUser = sysConfig.users && sysConfig.users.length > 0;
    if (hasMultiUser && targetSub) {
        let user = sysConfig.users.find(
            (u) =>
                u.name.toLowerCase() === targetSub.toLowerCase() ||
                u.id === targetSub,
        );
        if (user) {
            name = user.name;
            id = user.id;
            limitTotalReq = user.limitTotalReq || 0;
            expiryMs = user.expiryMs || 0;
        }
    } else if (!hasMultiUser) {
        limitTotalReq = sysConfig.limitTotalReq || 0;
        expiryMs = sysConfig.expiryMs || 0;
    }

    let idClean = id.replace(/-/g, "").toLowerCase();
    let sysU = sysUsageCache?.users?.[idClean] || { reqs: 0, dReqs: 0 };
    let totalReqs = sysU.reqs || 0;

    let totalGb = (totalReqs / 6000).toFixed(2);
    let limitTotalGb = limitTotalReq
        ? (limitTotalReq / 6000).toFixed(2)
        : "Unlimited";

    let expiryDateTxt = "Never Expire";
    let remDaysTxt = "Never Expire";
    if (expiryMs) {
        let exp = new Date(expiryMs);
        expiryDateTxt = exp.toISOString().split("T")[0];
        let remDays = Math.ceil(
            (expiryMs - Date.now()) / (1000 * 60 * 60 * 24),
        );
        remDaysTxt = remDays >= 0 ? `${remDays} Days Left` : "Expired";
    }

    return {
        usedStr: `Used: ${totalGb} GB / ${limitTotalGb} GB`,
        expiryStr: `Expiry: ${expiryDateTxt} (${remDaysTxt})`,
    };
}

function getFakeConfigNames(targetSub = null) {
    let stats = getSubscriptionStats(targetSub);
    let configs = sysConfig.fakeConfigs || [
        { name: "📊 {usage}", enabled: true },
        { name: "📅 {expiry}", enabled: true },
    ];
    return configs
        .filter((f) => f && f.enabled && f.name)
        .map((f) => {
            return f.name
                .replace(/\{usage\}/g, stats.usedStr)
                .replace(/\{expiry\}/g, stats.expiryStr);
        });
}

function getCleanIpsWithNames(hostName, userCleanIps = null) {
    let rawIps = userCleanIps || sysConfig.cleanIps;
    let entries = rawIps
        ? rawIps
              .split(/[\r\n,;]+/)
              .map((s) => {
                  let t = s.trim();
                  if (!t) return null;
                  let parts = t.split("#");
                  let ip = parts[0].trim();
                  let name = (parts[1] || "").trim();
                  return ip ? { ip, name } : null;
              })
              .filter(Boolean)
        : [];
    if (entries.length === 0)
        entries = [
            {
                ip: hostName.endsWith(".pages.dev")
                    ? sysConfig.metricNode
                    : hostName,
                name: "",
            },
        ];
    return entries;
}

function getAllProfiles(targetSub = null) {
    let list = [{ id: activeDeviceId, name: "Default" }];

    if (sysConfig.users && sysConfig.users.length > 0) {
        let now = Date.now();
        sysConfig.users.forEach((u) => {
            let skip = false;
            if (u.expiryMs && now > u.expiryMs) skip = true;
            if (u.isPaused) skip = true;
            if (
                u.limitTotalReq &&
                sysUsageCache &&
                sysUsageCache.users &&
                sysUsageCache.users[u.id.replace(/-/g, "").toLowerCase()]
            ) {
                if (
                    sysUsageCache.users[u.id.replace(/-/g, "").toLowerCase()]
                        .reqs >= u.limitTotalReq
                )
                    skip = true;
            }
            if (!skip) {
                list.push({
                    id: u.id,
                    name: u.name,
                    proxyIp: u.proxyIp,
                    cleanIp: u.cleanIp || null,
                    userMode: u.userMode || null,
                    userPorts: u.userPorts || null,
                    maxConfigs: u.maxConfigs || null,
                    proxyIpGeo: u.proxyIpGeo || null,
                    userNodes: u.userNodes || null,
                    nat64: u.nat64 || null,
                    connLimit: u.connLimit || null,
                    userPanelUrl: u.userPanelUrl || null,
                });
                registerConfigEntry(u.id, u.id, u.proxyIp || "");
            }
        });
    }

    if (targetSub) {
        list = list.filter(
            (p) => p.name.toLowerCase() === targetSub.toLowerCase() || p.id === targetSub,
        );
    }
    return list;
}

function linkedPanelHost(p) {
    let raw = p && typeof p === "object" ? p.url || "" : p || "";
    raw = String(raw).trim();
    if (!raw) return "";
    raw = raw.replace(/^[a-zA-Z]+:\/\//, "");
    raw = raw.split("/")[0];
    raw = raw.split("@").pop();
    if (raw.startsWith("[")) {
        return raw.slice(0, raw.indexOf("]") + 1);
    }
    return raw.split(":")[0];
}

function getGlobalNodeHosts() {
    let hosts = [];
    if (sysConfig.slaveNodes)
        hosts.push(
            ...sysConfig.slaveNodes
                .split(/[\r\n,;]+/)
                .map((s) => s.trim())
                .filter(Boolean),
        );
    if (Array.isArray(sysConfig.linkedPanels))
        hosts.push(
            ...sysConfig.linkedPanels.map(linkedPanelHost).filter(Boolean),
        );
    return [...new Set(hosts)];
}

function getProxyIpsArray(proxyIpString) {
    if (!proxyIpString) return [];
    return proxyIpString
        .split(/[\r\n,;]+/)
        .map((s) => {
            let trimmed = s.trim();
            if (!trimmed) return "";
            let hostPort = trimmed.split("#")[0].split("@")[0];
            if (hostPort.includes(":") && !hostPort.includes("]")) {
                return hostPort.split(":")[0];
            } else if (hostPort.startsWith("[") && hostPort.includes("]")) {
                return hostPort.split("]")[0].replace("[", "");
            }
            return hostPort;
        })
        .filter(Boolean);
}

function ipv4ToNat64(ipv4, prefix) {
    if (!prefix || !ipv4) return null;
    let parts = ipv4.split(".");
    if (parts.length !== 4 || parts.some((p) => isNaN(parseInt(p))))
        return null;
    let hex = parts
        .map((p) => parseInt(p).toString(16).padStart(2, "0"))
        .join("");
    let suffix = hex.match(/.{1,4}/g).join(":");
    return prefix.replace(/\/\d+$/, "").replace(/:$/, "") + "::" + suffix;
}

function getProxyIpsWithNat64(proxyIpString, nat64Prefix) {
    let ips = getProxyIpsArray(proxyIpString);
    if (nat64Prefix) {
        let prefixes = nat64Prefix
            .split(/[\r\n,;]+/)
            .map((s) => s.trim())
            .filter(Boolean);
        let nat64Ips = [];
        prefixes.forEach((prefix) => {
            ips.forEach((ip) => {
                if (/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(ip)) {
                    let nat64 = ipv4ToNat64(ip, prefix);
                    if (nat64) nat64Ips.push(nat64);
                }
            });
        });
        ips = ips.concat(nat64Ips);
    }
    return ips;
}

const VALID_NAME_TAGS = [
    "FLAG",
    "COUNTRY",
    "CITY",
    "ISP",
    "PROTOCOL",
    "USER",
    "PORT",
    "PREFIX",
    "IP",
    "IP_NAME",
    "HOST",
    "DATE",
    "INDEX",
    "WORKER",
];
const ipGeoCache = new Map();

function validateNameStrategy(strategy) {
    if (!strategy) return { valid: true, unknownTags: [] };
    const tagPattern = /\{([A-Za-z]+)\}/g;
    let match;
    let unknownTags = [];
    while ((match = tagPattern.exec(strategy)) !== null) {
        let tag = match[1].toUpperCase();
        if (!VALID_NAME_TAGS.includes(tag)) unknownTags.push(match[1]);
    }
    return { valid: unknownTags.length === 0, unknownTags };
}

async function preloadIpFlags(profiles, hostNames) {
    let uniqueIps = new Set();
    profiles.forEach((p) => {
        hostNames.forEach((h) => {
            getCleanIpsWithNames(h, p.cleanIp).forEach((e) => uniqueIps.add(e.ip));
        });
        if (p.proxyIp) {
            getProxyIpsArray(p.proxyIp).forEach((ip) => uniqueIps.add(ip));
        }
    });

    let uncached = Array.from(uniqueIps).filter((ip) => !ipGeoCache.has(ip));
    for (let i = 0; i < uncached.length; i += 100) {
        let batch = uncached.slice(i, i + 100);
        let queries = batch.map((ip) => {
            let clean = ip
                .split(":")[0]
                .replace(/[\[\]]/g, "")
                .split("#")[0]
                .trim();
            return {
                query: clean,
                fields: "status,country,countryCode,city,isp,org",
            };
        });
        try {
            const res = await fetch(
                "http://ip-api.com/batch?fields=status,country,countryCode,city,isp,org",
                {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(queries),
                },
            );
            const results = await res.json();
            batch.forEach((ip, idx) => {
                let data = results[idx];
                if (data && data.status === "success") {
                    const codePoints = data.countryCode
                        .toUpperCase()
                        .split("")
                        .map((char) => 127397 + char.charCodeAt());
                    ipGeoCache.set(ip, {
                        flag: String.fromCodePoint(...codePoints),
                        country: data.country || "Unknown",
                        countryCode: data.countryCode || "",
                        city: data.city || "",
                        isp: data.isp || data.org || "",
                    });
                } else {
                    ipGeoCache.set(ip, {
                        flag: "🌐",
                        country: "Unknown",
                        countryCode: "",
                        city: "",
                        isp: "",
                    });
                }
            });
        } catch (e) {}
    }
}

function getGeoInfo(ip) {
    if (!ip)
        return {
            flag: "🌐",
            country: "Unknown",
            countryCode: "",
            city: "",
            isp: "",
        };
    let clean = ip
        .split(":")[0]
        .replace(/[\[\]]/g, "")
        .split("#")[0]
        .trim();
    return (
        ipGeoCache.get(ip) ||
        ipGeoCache.get(clean) || {
            flag: "🌐",
            country: "Unknown",
            countryCode: "",
            city: "",
            isp: "",
        }
    );
}

async function resolveUserProxyIpGeo(user) {
    if (!user.proxyIp) {
        user.proxyIpGeo = null;
        return;
    }
    let pips = getProxyIpsArray(user.proxyIp);
    if (pips.length === 0) {
        user.proxyIpGeo = null;
        return;
    }
}

function getConfigName(
    type,
    profileName,
    port,
    hostName,
    ip,
    proxyIp = null,
    configIndex = 0,
    ipName = "",
    isDirect = false
) {
    let prefix = sysConfig.namePrefix || "Core";
    let strategy = sysConfig.nameStrategy || "default";
    let typeLab = type === "alpha" ? "V" : "T";

    if (strategy.includes("{") && strategy.includes("}")) {
        let lookupIp = proxyIp || ip;
        let geoInfo = getGeoInfo(lookupIp);
        let protoLab = type === "alpha" ? "VLESS" : "Trojan";
        let now = new Date();
        let dateStr =
            now.getFullYear() +
            "-" +
            String(now.getMonth() + 1).padStart(2, "0") +
            "-" +
            String(now.getDate()).padStart(2, "0");
        let workerName =
            sysConfig.cfWorkerName || sysConfig.name || hostName || "";
        let flagToUse = isDirect ? "☁️" : geoInfo.flag;
        return strategy
            .replace(/{FLAG}/g, flagToUse)
            .replace(/{COUNTRY}/g, geoInfo.country)
            .replace(/{CITY}/g, geoInfo.city)
            .replace(/{ISP}/g, geoInfo.isp)
            .replace(/{PROTOCOL}/g, protoLab)
            .replace(/{USER}/g, profileName)
            .replace(/{PORT}/g, port)
            .replace(/{PREFIX}/g, prefix)
            .replace(/{IP}/g, ip || "")
            .replace(/{IP_NAME}/g, ipName || "")
            .replace(/{HOST}/g, hostName || "")
            .replace(/{DATE}/g, dateStr)
            .replace(/{INDEX}/g, String(configIndex))
            .replace(/{WORKER}/g, workerName);
    }
    return `${typeLab}-Core-${port}`;
}

function calcEffectiveIps(ips, maxCfg, effectiveMode, effectivePorts, pipsCount = 1) {
    if (!maxCfg) return ips;
    let protoCount = effectiveMode === "both" ? 2 : 1;
    let portCount = effectivePorts.length;
    let directMultiplier = sysConfig.enableDirectConfigs ? 2 : 1;
    let multiplier = protoCount * portCount * directMultiplier * Math.max(1, pipsCount);
    let neededIps = Math.max(1, Math.floor(maxCfg / multiplier));
    return ips.slice(0, neededIps);
}

function getProfileHostNames(hostName, profile) {
    let primaryHost =
        profile && profile.userPanelUrl ? profile.userPanelUrl : hostName;
    let names = [];
    if (profile && profile.userNodes && profile.userNodes.trim()) {
        names.push(
            ...profile.userNodes
                .split(/[\r\n,;]+/)
                .map((s) => linkedPanelHost(s.trim()))
                .filter(Boolean),
        );
    } else {
        names.push(linkedPanelHost(primaryHost));
        names.push(...getGlobalNodeHosts());
    }
    return [...new Set(names)];
}

function getEffectiveNat64(userNat64) {
    let parts = [];
    if (userNat64)
        parts.push(
            ...userNat64
                .split(/[\r\n,;]+/)
                .map((s) => s.trim())
                .filter(Boolean),
        );
    if (sysConfig.nat64Prefix)
        parts.push(
            ...sysConfig.nat64Prefix
                .split(/[\r\n,;]+/)
                .map((s) => s.trim())
                .filter(Boolean),
        );
    return [...new Set(parts)].join(",") || null;
}

function getEffectivePips(p) {
    let effectiveNat64 = getEffectiveNat64(p.nat64);
    let pips = getProxyIpsWithNat64(p.proxyIp, effectiveNat64);
    if (pips.length === 0 && sysConfig.backupRelay) {
        pips = getProxyIpsWithNat64(sysConfig.backupRelay, effectiveNat64);
    }
    if (pips.length === 0 && sysConfig.customRelay) {
        pips = getProxyIpsWithNat64(sysConfig.customRelay, effectiveNat64);
    }
    return pips;
}

function parseVlessUri(uri) {
    return null;
}

async function buildUriProfile(hostName, targetSub = null, allowInsecure = false) {
    let ports = ["443"];
    let lines = [];
    let profiles = getAllProfiles(targetSub);
    return lines.join("\n");
}

async function buildYamlProfile(hostName, targetSub = null, allowInsecure = false, env = null) {
    return "";
}

async function buildClashJsonProfile(hostName, targetSub = null, allowInsecure = false, env = null) {
    return {};
}

async function buildVJsonProfile(hostName, targetSub = null, allowInsecure = false, env = null) {
    return {};
}

async function buildSingBoxJsonProfile(hostName, targetSub = null, allowInsecure = false, env = null) {
    return {};
}