// main.js — OSM Med Spa Seeder (Apify Actor)
// Finds med spas in a bounding box via OpenStreetMap Overpass,
// extracts websites → root domains, and writes a clean dataset.

import { Actor } from 'apify';
import fetch from 'node-fetch';

const OVERPASS_ENDPOINTS = [
  'https://overpass-api.de/api/interpreter',
  'https://overpass.kumi.systems/api/interpreter',
  'https://overpass.openstreetmap.ru/api/interpreter',
];

function buildRegex(keywords) {
  // (?i) = case-insensitive; join keywords with |
  const safe = keywords.map((k) => k.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  return `(?i)(${safe.join('|')})`;
}

function rootDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url.startsWith('http') ? url : `http://${url}`);
    const h = u.hostname.toLowerCase();
    return h.startsWith('www.') ? h.slice(4) : h;
  } catch {
    return '';
  }
}

function categoryFromTags(tags) {
  if (tags.leisure === 'spa') return 'spa';
  if (tags.amenity === 'clinic') return 'clinic';
  if (tags.shop === 'beauty') return 'beauty';
  return tags.amenity || tags.leisure || tags.shop || '';
}

function cityFromTags(tags, fallbackCity) {
  return (
    tags['addr:city'] ||
    tags['is_in:city'] ||
    tags['addr:town'] ||
    tags['addr:village'] ||
    fallbackCity
  );
}

function evidenceNote(tags, keywords) {
  const n = (tags.name || '').toLowerCase();
  const hits = keywords.filter((k) => n.includes(k.toLowerCase()));
  return hits.length ? `name_keywords=${hits.join(',')}` : '';
}

function dedupeRows(rows) {
  const seen = new Set();
  const out = [];
  for (const r of rows) {
    const key = r.domain || `${r.name.toLowerCase()}|${r.city.toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(r);
  }
  return out;
}

async function queryOverpass(ql) {
  let lastErr;
  for (const ep of OVERPASS_ENDPOINTS) {
    try {
      const res = await fetch(ep, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams({ data: ql }),
      });
      if (!res.ok) throw new Error(`Overpass HTTP ${res.status}`);
      return await res.json();
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr || new Error('All Overpass endpoints failed');
}

Actor.main(async () => {
  const input = (await Actor.getInput()) || {};

  // Defaults target San Antonio med spas; customize via INPUT
  const {
    bbox = { south: 29.10, west: -98.85, north: 29.75, east: -98.10 },
    keywords = ['med spa', 'medspa', 'aesthetic', 'inject', 'botox', 'laser', 'hydrafacial'],
    city = 'San Antonio',
  } = input;

  const nameRegex = buildRegex(keywords);

  // Overpass QL: nodes/ways with spa/beauty/clinic and name matching our regex
  const ql = `
    [out:json][timeout:120];
    (
      node["leisure"="spa"]["name"~"${nameRegex}"](${bbox.south},${bbox.west},${bbox.north},${bbox.east});
      way ["leisure"="spa"]["name"~"${nameRegex}"](${bbox.south},${bbox.west},${bbox.north},${bbox.east});
      node["shop"="beauty"]["name"~"${nameRegex}"](${bbox.south},${bbox.west},${bbox.north},${bbox.east});
      way ["shop"="beauty"]["name"~"${nameRegex}"](${bbox.south},${bbox.west},${bbox.north},${bbox.east});
      node["amenity"="clinic"]["name"~"${nameRegex}"](${bbox.south},${bbox.west},${bbox.north},${bbox.east});
      way ["amenity"="clinic"]["name"~"${nameRegex}"](${bbox.south},${bbox.west},${bbox.north},${bbox.east});
    );
    out center tags;
  `;

  const json = await queryOverpass(ql);
  const elements = json?.elements || [];

  const rows = elements
    .map((el) => {
      const tags = el.tags || {};
      const name = (tags.name || '').trim();
      if (!name) return null;

      const website = tags.website || tags['contact:website'] || '';
      const domain = rootDomain(website);
      const lat = el.lat ?? el.center?.lat ?? null;
      const lon = el.lon ?? el.center?.lon ?? null;
      const category = categoryFromTags(tags);
      const cityPicked = cityFromTags(tags, city);
      const note = evidenceNote(tags, keywords);
      const confidence = domain ? 1.0 : note ? 0.7 : 0.5;
      const osmType = el.type === 'node' ? 'node' : 'way';
      const sourceUrl = `https://www.openstreetmap.org/${osmType}/${el.id}`;

      return {
        name,
        domain,
        city: cityPicked,
        category,
        source_url: sourceUrl,
        lat: typeof lat === 'number' ? lat.toFixed(6) : '',
        lon: typeof lon === 'number' ? lon.toFixed(6) : '',
        confidence: confidence.toFixed(2),
        notes: note,
      };
    })
    .filter(Boolean);

  const deduped = dedupeRows(rows);

  // Write dataset rows (export as CSV/JSON in Apify console)
  await Actor.pushData(deduped);

  // Save a tiny run summary
  await Actor.setValue('RUN-SUMMARY', {
    found: rows.length,
    deduped: deduped.length,
    bbox,
    keywords,
    city,
    hint: 'Open the Dataset tab to export CSV.',
  });
});
