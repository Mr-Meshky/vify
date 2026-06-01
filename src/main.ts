import fs from "node:fs";
import path from "node:path";
import { Buffer } from "node:buffer";
import crypto from "node:crypto";
import dns from "node:dns/promises";
import net from "node:net";
import axios from "axios";
import { URL } from "url";
import maxmind, { CountryResponse, Reader } from "maxmind";

/* ================= Config ================= */

const OUTPUT_DIR = "configs";
const COUNTRIES_DIR = path.join(OUTPUT_DIR, "countries");
const ALIVE_DIR = path.join(OUTPUT_DIR, "alive");
fs.mkdirSync(OUTPUT_DIR, { recursive: true });
fs.mkdirSync(COUNTRIES_DIR, { recursive: true });
fs.mkdirSync(ALIVE_DIR, { recursive: true });

const SOURCES = [
  "https://raw.githubusercontent.com/MrAbolfazlNorouzi/iran-configs/refs/heads/main/configs/working-configs.txt",
  "https://raw.githubusercontent.com/arshiacomplus/v2rayExtractor/refs/heads/main/mix/sub.html",
  "https://raw.githubusercontent.com/4n0nymou3/multi-proxy-config-fetcher/refs/heads/main/configs/proxy_configs.txt",
  "https://raw.githubusercontent.com/parvinxs/Submahsanetxsparvin/refs/heads/main/Sub.mahsa.xsparvin",
  "https://raw.githubusercontent.com/mahdibland/V2RayAggregator/master/Eternity.txt",
  "https://raw.githubusercontent.com/Pawdroid/Free-servers/main/sub",
];

const TELEGRAM_CHANNELS = [
  "v2ray_configs_pool",
  "outline_vpn",
  "V2ray_Alpha"
];

const BLOCKED_PROTOCOLS = new Set([
  "https",
  "http",
  "hysteria2",
  "hy2",
  "tuic",
  "hysteria",
]);

const LIGHT_PER_PROTOCOL = 10;
const TCP_TIMEOUT_MS = 3000;
const FETCH_RETRIES = 2;
const PROCESS_CONCURRENCY = 20;

axios.defaults.headers.common["User-Agent"] =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/138.0.0.0";
axios.defaults.timeout = 15000;
axios.defaults.validateStatus = () => true;

/* ================= Types ================= */

interface ProcessedConfig {
  original: string;
  renamed: string;
  proto: string;
  ip: string;
  port: number;
  country: string;
  alive: boolean;
}

interface Stats {
  total: number;
  alive: number;
  dead: number;
  byProtocol: Record<string, { total: number; alive: number }>;
  byCountry: Record<string, { total: number; alive: number }>;
  sources: Record<string, number>;
  note: string;
  generatedAt: string;
}

/* ================= Cache ================= */

const ipCache = new Map<string, string>();
const geoCache = new Map<string, string>();
const tagCache = new Map<string, string>();

const MMDB_PATH = "GeoLite2-Country.mmdb";
const MMDB_URL =
  "https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-Country.mmdb";

let mmdb: Reader<CountryResponse> | null = null;

/* ================= Base64 ================= */

function b64Decode(str: string): string {
  const pad = "=".repeat((4 - (str.length % 4)) % 4);
  return Buffer.from(str + pad, "base64").toString("utf8");
}

function b64Encode(str: string): string {
  return Buffer.from(str, "utf8").toString("base64");
}

/* ================= Parallel ================= */

async function parallelMap<T, R>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const res: R[] = [];
  let i = 0;

  async function worker() {
    while (i < items.length) {
      const idx = i++;
      res[idx] = await fn(items[idx]);
    }
  }

  await Promise.all(Array.from({ length: limit }, worker));
  return res;
}

/* ================= Protocol Utils ================= */

function detectProtocol(link: string): string {
  return link.split("://")[0]?.toLowerCase() || "unknown";
}

function extractVmessFields(link: string): { host: string; port: number } {
  try {
    const raw = link.replace("vmess://", "").trim();
    const cfg = JSON.parse(b64Decode(raw));
    return { host: cfg.add || "", port: parseInt(cfg.port, 10) || 443 };
  } catch {
    return { host: "", port: 443 };
  }
}

function extractHost(link: string): string {
  if (detectProtocol(link) === "vmess") return extractVmessFields(link).host;
  try {
    return new URL(link).hostname;
  } catch {
    return "";
  }
}

function extractPort(link: string): number {
  if (detectProtocol(link) === "vmess") return extractVmessFields(link).port;
  try {
    const port = new URL(link).port;
    if (port) return parseInt(port, 10);
  } catch {}
  return 443;
}

/* ================= Network ================= */

async function resolveIP(host: string): Promise<string> {
  if (ipCache.has(host)) return ipCache.get(host)!;

  let ip = "";
  if (net.isIP(host)) {
    ip = host;
  } else {
    try {
      ip = (await dns.lookup(host)).address;
    } catch {}
  }

  ipCache.set(host, ip);
  return ip;
}

async function tcpProbe(ip: string, port: number): Promise<boolean> {
  if (!net.isIP(ip)) return false;

  return new Promise((resolve) => {
    const socket = new net.Socket();
    const timer = setTimeout(() => {
      socket.destroy();
      resolve(false);
    }, TCP_TIMEOUT_MS);

    socket.connect(port, ip, () => {
      clearTimeout(timer);
      socket.destroy();
      resolve(true);
    });

    socket.on("error", () => {
      clearTimeout(timer);
      resolve(false);
    });
  });
}

/* ================= Fetch ================= */

async function fetchText(url: string, retries = FETCH_RETRIES): Promise<string> {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const r = await axios.get(url);
      const data = r.data || "";
      if (typeof data !== "string") return JSON.stringify(data);

      // Try decoding base64 subscriptions
      const trimmed = data.trim().replace(/\s+/g, "");
      if (trimmed.length > 100 && /^[A-Za-z0-9+/=]+$/.test(trimmed)) {
        try {
          const decoded = b64Decode(trimmed);
          if (decoded.includes("://")) return decoded;
        } catch {}
      }

      return data;
    } catch {
      if (attempt < retries) {
        await new Promise((r) => setTimeout(r, 1000 * (attempt + 1)));
      }
    }
  }
  return "";
}

async function fetchTelegramChannel(channel: string): Promise<string[]> {
  try {
    const r = await axios.get(`https://t.me/s/${channel}`, {
      headers: { Accept: "text/html" },
      timeout: 20000,
    });
    const html: string = r.data || "";
    const matches = html.match(/[a-zA-Z][\w+.-]*:\/\/[^\s"'<>&]+/g) || [];
    return matches.filter((m) => !BLOCKED_PROTOCOLS.has(detectProtocol(m)));
  } catch {
    return [];
  }
}

/* ================= Geo ================= */

// Download MMDB if missing, then open the reader
async function initMMDB(): Promise<void> {
  if (!fs.existsSync(MMDB_PATH)) {
    console.log("  Downloading GeoLite2-Country.mmdb...");
    try {
      const r = await axios.get(MMDB_URL, { responseType: "arraybuffer", timeout: 60000 });
      fs.writeFileSync(MMDB_PATH, Buffer.from(r.data));
      console.log("  Download complete.");
    } catch (e) {
      console.warn("  Failed to download MMDB, will use API fallback.", e);
      return;
    }
  }
  try {
    mmdb = await maxmind.open<CountryResponse>(MMDB_PATH);
    console.log("  MaxMind MMDB loaded.");
  } catch (e) {
    console.warn("  Failed to open MMDB.", e);
  }
}

// MaxMind local lookup (instant, no network)
function lookupMMDB(ip: string): string | null {
  if (!mmdb || !net.isIP(ip)) return null;
  try {
    const result = mmdb.get(ip);
    return result?.country?.iso_code?.toUpperCase() ?? null;
  } catch {
    return null;
  }
}

// Batch lookup via ip-api.com for IPs MaxMind missed (100 per request)
async function batchGeoLookup(ips: string[]): Promise<void> {
  const unknown = ips.filter((ip) => ip && net.isIP(ip) && !geoCache.has(ip));
  if (!unknown.length) return;

  const CHUNK = 100;
  for (let i = 0; i < unknown.length; i += CHUNK) {
    const chunk = unknown.slice(i, i + CHUNK);
    try {
      const r = await axios.post(
        "http://ip-api.com/batch?fields=countryCode,query",
        chunk.map((q) => ({ query: q })),
        { timeout: 10000 },
      );
      for (const item of r.data as { query: string; countryCode: string }[]) {
        const cc = (item.countryCode || "UN").toUpperCase();
        geoCache.set(item.query, cc === "XX" ? "UN" : cc);
      }
    } catch {
      // individual fallback will handle remaining misses
    }
    // ip-api.com free tier: 45 req/min — stay safe at ~38/min
    if (i + CHUNK < unknown.length) {
      await new Promise((r) => setTimeout(r, 1600));
    }
  }
}

// Individual fallback: ipwho.is → UN
async function getCountryCode(ip: string): Promise<string> {
  if (!ip) return "UN";
  if (geoCache.has(ip)) return geoCache.get(ip)!;

  // Try ipwho.is as last resort
  try {
    const r = await axios.get(`https://ipwho.is/${ip}`, { timeout: 8000 });
    const cc = (r.data?.country_code || "UN").toUpperCase();
    geoCache.set(ip, cc);
    return cc;
  } catch {
    geoCache.set(ip, "UN");
    return "UN";
  }
}

function countryFlag(cc: string): string {
  if (cc.length !== 2) return "🏳️";
  return String.fromCodePoint(...[...cc].map((c) => 127397 + c.charCodeAt(0)));
}

async function buildTag(ip: string, country: string): Promise<string> {
  if (tagCache.has(ip)) return tagCache.get(ip)!;
  const tag = `${countryFlag(country)} @MrMeshkyChannel ${crypto.randomInt(100000, 999999)}`;
  tagCache.set(ip, tag);
  return tag;
}

/* ================= Dedup ================= */

function dedupByIPPort(links: string[]): string[] {
  const seen = new Set<string>();
  return links.filter((link) => {
    const host = extractHost(link);
    const port = extractPort(link);
    const key = `${host || link}:${port}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

/* ================= Rename ================= */

function renameVmessSafe(link: string, tag: string): string {
  try {
    const raw = link.replace("vmess://", "").trim();
    const cfg = JSON.parse(b64Decode(raw));
    cfg.ps = tag;
    return `vmess://${b64Encode(JSON.stringify(cfg))}`;
  } catch {
    return link;
  }
}

function renameURLLike(link: string, ip: string, port: number, tag: string): string {
  try {
    const u = new URL(link);
    u.hostname = ip;
    u.port = String(port);
    u.hash = encodeURIComponent(tag);
    return u.toString();
  } catch {
    return link;
  }
}

/* ================= Process ================= */

async function processConfig(link: string): Promise<ProcessedConfig> {
  const proto = detectProtocol(link);
  const host = extractHost(link);
  const port = extractPort(link);

  let ip = "";
  let country = "UN";
  let alive = false;

  if (host) {
    ip = await resolveIP(host);
    country = await getCountryCode(ip);
    if (ip) alive = await tcpProbe(ip, port);
  }

  const effectiveIP = ip || host;
  const tag = await buildTag(effectiveIP, country);

  let renamed = link;
  if (proto === "vmess") {
    renamed = renameVmessSafe(link, tag);
  } else {
    renamed = renameURLLike(link, effectiveIP, port, tag);
  }

  return { original: link, renamed, proto, ip: effectiveIP, port, country, alive };
}

/* ================= Save ================= */

function saveFile(filePath: string, lines: string[]) {
  if (!lines.length) return;
  fs.writeFileSync(filePath, lines.join("\n"), "utf8");
  console.log(`  ✔ ${filePath} (${lines.length})`);
}

function saveStats(configs: ProcessedConfig[], sourceStats: Record<string, number>) {
  const stats: Stats = {
    total: configs.length,
    alive: configs.filter((c) => c.alive).length,
    dead: configs.filter((c) => !c.alive).length,
    byProtocol: {},
    byCountry: {},
    sources: sourceStats,
    note: "alive = TCP port reachable from GitHub Actions runner (not guaranteed to work inside Iran)",
    generatedAt: new Date().toISOString(),
  };

  for (const c of configs) {
    stats.byProtocol[c.proto] ??= { total: 0, alive: 0 };
    stats.byProtocol[c.proto].total++;
    if (c.alive) stats.byProtocol[c.proto].alive++;

    stats.byCountry[c.country] ??= { total: 0, alive: 0 };
    stats.byCountry[c.country].total++;
    if (c.alive) stats.byCountry[c.country].alive++;
  }

  const sortByTotal = (obj: Record<string, { total: number; alive: number }>) =>
    Object.fromEntries(Object.entries(obj).sort(([, a], [, b]) => b.total - a.total));

  stats.byProtocol = sortByTotal(stats.byProtocol);
  stats.byCountry = sortByTotal(stats.byCountry);

  fs.writeFileSync(
    path.join(OUTPUT_DIR, "stats.json"),
    JSON.stringify(stats, null, 2),
    "utf8",
  );
  console.log(`  ✔ stats.json`);
}

/* ================= Main ================= */

async function main() {
  console.log("🚀 Fetching VPN configs...\n");

  // Init MaxMind MMDB (download if missing)
  console.log("🗺️  Loading MaxMind GeoLite2...");
  await initMMDB();

  const rawSet = new Set<string>();
  const sourceStats: Record<string, number> = {};

  // HTTP/GitHub sources
  console.log("📥 HTTP sources:");
  for (const url of SOURCES) {
    const raw = await fetchText(url);
    const matches = raw.match(/[a-zA-Z][\w+.-]*:\/\/[^\s]+/g) || [];
    const valid = matches.filter((l) => !BLOCKED_PROTOCOLS.has(detectProtocol(l)));
    sourceStats[url] = valid.length;
    console.log(`  ${String(valid.length).padStart(4)} ← ${url}`);
    valid.forEach((l) => rawSet.add(l));
  }

  // Telegram channels
  console.log("\n📡 Telegram channels:");
  for (const ch of TELEGRAM_CHANNELS) {
    const links = await fetchTelegramChannel(ch);
    sourceStats[`t.me/s/${ch}`] = links.length;
    console.log(`  ${String(links.length).padStart(4)} ← @${ch}`);
    links.forEach((l) => rawSet.add(l));
  }

  console.log(`\n📊 Raw total:          ${rawSet.size}`);
  const deduped = dedupByIPPort([...rawSet]);
  console.log(`📊 After IP:port dedup: ${deduped.length}`);

  // Phase 1: Resolve all IPs in parallel (populate ipCache)
  console.log("\n🔍 Phase 1: Resolving IPs...");
  await parallelMap(deduped, PROCESS_CONCURRENCY, async (link) => {
    const host = extractHost(link);
    if (host) await resolveIP(host);
  });

  // Phase 2: Geo lookup — MaxMind local first, ip-api.com batch for misses
  const uniqueIPs = [...new Set([...ipCache.values()].filter(Boolean))];
  console.log(`🌍 Phase 2: Geo lookup for ${uniqueIPs.length} unique IPs...`);

  let mmdbHits = 0;
  for (const ip of uniqueIPs) {
    const cc = lookupMMDB(ip);
    if (cc) { geoCache.set(ip, cc); mmdbHits++; }
  }
  console.log(`   MaxMind hits: ${mmdbHits}/${uniqueIPs.length}`);

  const remaining = uniqueIPs.filter((ip) => !geoCache.has(ip));
  if (remaining.length) {
    console.log(`   ip-api.com batch for ${remaining.length} remaining IPs...`);
    await batchGeoLookup(remaining);
  }
  console.log(`   Total geo resolved: ${geoCache.size}/${uniqueIPs.length}`);

  // Phase 3: TCP probe + rename (IP & geo served from cache)
  console.log("\n⚙️  Phase 3: TCP probe + rename...");
  const configs = await parallelMap(deduped, PROCESS_CONCURRENCY, processConfig);

  const aliveConfigs = configs.filter((c) => c.alive);
  console.log(`\n✅ Alive: ${aliveConfigs.length}  ❌ Dead: ${configs.length - aliveConfigs.length}`);

  // --- Save ---
  console.log("\n💾 Saving files:");

  // all.txt — every config regardless of liveness
  saveFile(path.join(OUTPUT_DIR, "all.txt"), configs.map((c) => c.renamed));

  // alive.txt — only TCP-reachable from runner
  saveFile(path.join(OUTPUT_DIR, "alive.txt"), aliveConfigs.map((c) => c.renamed));

  // light.txt — top LIGHT_PER_PROTOCOL alive configs per protocol
  const byProtoAlive: Record<string, string[]> = {};
  for (const c of aliveConfigs) {
    byProtoAlive[c.proto] ??= [];
    byProtoAlive[c.proto].push(c.renamed);
  }
  const lightLinks = Object.values(byProtoAlive).flatMap((l) => l.slice(0, LIGHT_PER_PROTOCOL));
  saveFile(path.join(OUTPUT_DIR, "light.txt"), lightLinks);

  // Per-protocol files (all)
  const byProtoAll: Record<string, string[]> = {};
  for (const c of configs) {
    byProtoAll[c.proto] ??= [];
    byProtoAll[c.proto].push(c.renamed);
  }
  for (const [proto, links] of Object.entries(byProtoAll)) {
    saveFile(path.join(OUTPUT_DIR, `${proto}.txt`), links);
  }

  // Per-protocol alive files
  for (const [proto, links] of Object.entries(byProtoAlive)) {
    saveFile(path.join(ALIVE_DIR, `${proto}.txt`), links);
  }

  // Per-country files (alive only)
  const byCountry: Record<string, string[]> = {};
  for (const c of aliveConfigs) {
    byCountry[c.country] ??= [];
    byCountry[c.country].push(c.renamed);
  }
  for (const [cc, links] of Object.entries(byCountry)) {
    saveFile(path.join(COUNTRIES_DIR, `${cc}.txt`), links);
  }

  // Stats
  saveStats(configs, sourceStats);

  console.log("\n✅ Done.");
}

main().catch((e) => {
  console.error("Fatal error:", e);
  process.exit(1);
});
