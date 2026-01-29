import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import dns from "node:dns/promises";
import net from "node:net";
import axios from "axios";
import { URL } from "node:url";

/* ================= Config ================= */

const OUTPUT_DIR = "configs";
fs.mkdirSync(OUTPUT_DIR, { recursive: true });

const SOURCES = [
  "https://raw.githubusercontent.com/MrAbolfazlNorouzi/iran-configs/refs/heads/main/configs/working-configs.txt",
  "https://raw.githubusercontent.com/arshiacomplus/v2rayExtractor/refs/heads/main/mix/sub.html",
  "https://www.v2nodes.com/subscriptions/country/all/?key=CCAD69583DBA2BF",
  "https://raw.githubusercontent.com/4n0nymou3/multi-proxy-config-fetcher/refs/heads/main/configs/proxy_configs.txt",
];

axios.defaults.headers.common["User-Agent"] =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/138.0.0.0";
axios.defaults.timeout = 15000;
axios.defaults.validateStatus = () => true;

const ipCache = new Map<string, string>();
const tagCache = new Map<string, string>();

/* ================= Base64 ================= */

function b64Decode(str: string): string {
  const pad = "=".repeat((4 - (str.length % 4)) % 4);
  return Buffer.from(str + pad, "base64").toString("utf8");
}

function b64Encode(str: string): string {
  return Buffer.from(str, "utf8").toString("base64");
}

/* ================= Utils ================= */

async function parallelMap<T, R>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const result: R[] = [];
  let i = 0;

  async function worker() {
    while (i < items.length) {
      const idx = i++;
      result[idx] = await fn(items[idx]);
    }
  }

  await Promise.all(Array.from({ length: limit }, worker));

  return result;
}

function detectProtocol(link: string): string {
  return link.split("://")[0]?.toLowerCase() || "unknown";
}

function extractHost(link: string): string {
  try {
    return new URL(link).hostname;
  } catch {
    return "";
  }
}

async function resolveIP(host: string): Promise<string> {
  if (ipCache.has(host)) return ipCache.get(host)!;

  let ip = host;
  if (!net.isIP(host)) {
    try {
      ip = (await dns.lookup(host)).address;
    } catch {}
  }

  ipCache.set(host, ip);
  return ip;
}

async function fetchText(url: string): Promise<string> {
  try {
    const res = await axios.get(url);
    return res.data || "";
  } catch {
    return "";
  }
}

/* ================= Tag ================= */

const geoCache = new Map<string, string>();

async function getCountryCode(ip: string): Promise<string> {
  if (geoCache.has(ip)) return geoCache.get(ip)!;

  try {
    const res = await axios.get(`https://ipwho.is/${ip}`);
    const cc = (res.data?.country_code || "UN").toUpperCase();
    geoCache.set(ip, cc);
    return cc;
  } catch {
    return "UN";
  }
}

function countryFlag(cc: string): string {
  if (cc.length !== 2) return "🏳️";
  return String.fromCodePoint(...[...cc].map((c) => 127397 + c.charCodeAt(0)));
}

async function buildTag(ip: string): Promise<string> {
  if (tagCache.has(ip)) return tagCache.get(ip)!;

  const cc = await getCountryCode(ip);
  const flag = countryFlag(cc);
  const rand = crypto.randomInt(100000, 999999);
  const tag = `${flag} @MrMeshkyChannel ${rand}`;

  tagCache.set(ip, tag);
  return tag;
}
/* ================= Rename ================= */

function renameVmess(
  link: string,
  ip: string,
  port: number,
  tag: string,
): string {
  try {
    const raw = link.replace("vmess://", "");
    const cfg = JSON.parse(b64Decode(raw));
    cfg.add = ip;
    cfg.port = port;
    cfg.ps = tag;
    return `vmess://${b64Encode(JSON.stringify(cfg))}#${encodeURIComponent(tag)}`;
  } catch {
    return link;
  }
}

function renameURLLike(
  link: string,
  ip: string,
  port: number,
  tag: string,
): string {
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

async function renameLink(link: string): Promise<string> {
  const proto = detectProtocol(link);
  const host = extractHost(link);
  if (!host) return link;

  const ip = await resolveIP(host);
  const tag = await buildTag(ip);

  return proto === "vmess"
    ? renameVmess(link, ip, 443, tag)
    : renameURLLike(link, ip, 443, tag);
}

/* ================= Save ================= */

function saveFile(filePath: string, lines: string[]) {
  if (!lines.length) return;
  fs.writeFileSync(filePath, lines.join("\n"), "utf8");
  console.log(`✔ Saved ${filePath} (${lines.length})`);
}

function groupByProtocol(links: string[]) {
  return links.reduce<Record<string, string[]>>((acc, link) => {
    const proto = detectProtocol(link);
    acc[proto] ??= [];
    acc[proto].push(link);
    return acc;
  }, {});
}

/* ================= Main ================= */

async function main() {
  console.log("🚀 Fetching VPN configs...");

  const allLinks = new Set<string>();

  for (const url of SOURCES) {
    const raw = await fetchText(url);
    const matches = raw.match(/[a-zA-Z][\w+.-]*:\/\/[^\s]+/g) || [];
    console.log(`Fetched ${matches.length} from ${url}`);
    matches.forEach((l) => allLinks.add(l));
  }

  const uniqueLinks = [...allLinks];
  const grouped = groupByProtocol(uniqueLinks);

  const renamedAll = await parallelMap(uniqueLinks, 20, renameLink);

  saveFile(path.join(OUTPUT_DIR, "all.txt"), renamedAll);
  saveFile(path.join(OUTPUT_DIR, "light.txt"), renamedAll.slice(0, 30));

  for (const proto of Object.keys(grouped)) {
    const renamed = await parallelMap(grouped[proto], 20, renameLink);
    saveFile(path.join(OUTPUT_DIR, `${proto}.txt`), renamed);
  }

  console.log("✅ Done FAST.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
