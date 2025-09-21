// main.js — OSM Med Spa Seeder (resilient, no node-fetch)
// Queries OpenStreetMap Overpass for med-spa-like places in a bbox,
// extracts website → root domain, and writes a clean dataset.
// Accepts either:
//  A) Flat fields: bbox_south/west/north/east (strings ok), keyword_list, city
//  B) Nested: { bbox:{south,west,north,east}, keywords:[...], city }

import { Actor, log } from 'apify';

const OVERPASS_ENDPOINTS = [
  'https://overpass-api.de/api/interpreter',
  'https://overpass.kumi.systems/api/interpreter',
  'https://overpass.openstreetmap.ru/api/interpreter',
];

const UA = 'osm-medspa-seeder/1.0 (+contact@example.org)';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function toNum(v, def) { const n = Number(v); return Number.isFinite(n) ? n : def; }

function normalizeInput(input = {}) {
  const bbox = input.bbox || {
    south: toNum(input.bbox_south, 29.10),
    west:  toNum(input.bbox_west, -98.85),
    north: toNum(input.bbox_north, 29.75),
    east:  toNum(input.bbox_east, -98.10),
  };
  const defaultKws = ['med spa','medspa','aesthetic','inject','botox','laser','hydrafacial'];
  const keywords = Array.isArray(input.keywords) && input.keywords.length
    ? input.keywords
    : (typeof input.keyword_list === 'string'
        ? input.keyword_list.split(',').map(s => s.trim()).filter(Boolean)
        : defaultKws);
  const city = input.city || 'San Antonio';

  // sanity clamp
  const ok = (
    -90 <= bbox.south && bbox.south <= 90 &&
    -90 <= bbox.north && bbox.north <= 90 &&
    -180 <= bbox.west && bbox.west <= 180 &&
    -180 <= bbox.east && bbox.east <= 180
  );
  if (!ok) throw new Error('Invalid bbox coordinates');
  return { bbox, keywords, city };
}

function buildRegex(keywords) {
  const safe = (keywords || []).map(k => String(k).trim()).filter(Boolean)
    .map(k => k.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  const joined = safe.length ? safe.join('|') : 'med\\s*spa|medspa|aesthetic|inject|botox|laser|hydrafacial';
  return `(?i)(${joined})`;
}

function rootDomain(url) {
  if (!url) return '';
  try {
    const u = new URL(url.startsWith('http') ? url : `http://${url}`);
    const h = u.hostname.toLowerCase();
    return h.startsWith('www.') ? h.slice(4) : h;
  } catch { return ''; }
}

function categoryFromTags(tags) {
  if (tags.leisure === 'spa') return 'spa';
  if (tags.amenity === 'clinic') return 'clinic';
  if (tags.shop === 'beauty') return 'beauty';
  return tags.amenity || tags.leisure || tags.shop || '';
}

function cityFromTags(tags, fallbackCity) {
  return tags['addr:city'] || tags['is_in:city'] || tags['addr:town'] || tags['addr:village'] || fallbackCity;
}

function evidenceNote(tags, keywords) {
  const n = (tags.name || '').toLowerCase();
  const hits = (keywords || []).map(String).filter(Boolean).map(k => k.toLowerCase()).filter(k => n.includes(k));
  return hits.length ? `name_keywords=${hits.join(',')}` : '';
}

function dedupeRows(rows) {
  const seen = new Set(); const out = [];
  for (const r of rows) {
    const key = r.domain || `${(r.name||'').toLowerCase()}|${(r.city||'').toLowerCase()}`;
    if (seen.has(key)) continue; seen.add(key); out.push(r);
  }
  return out;
}

async function queryOverpass(ql) {
  let lastErr;
  for (const ep of OVERPASS_ENDPOINTS) {
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        log.info(`Overpass → ${ep} (attempt ${attempt})`);
        const res = await fetch(ep, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
            'User-Agent': UA,
          },
          body: new URLSearchParams({ data: ql }),
        });
        const ct = res.headers.get('content-type') || '';
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        if (!ct.includes('application/json')) throw new Error(`Unexpected content-type: ${ct}`);
        return await res.json();
      } catch (e) {
        lastErr = e; log.warning(`Overpass failed (${ep}) attempt ${attempt}: ${e.message}`);
        await sleep(700 * attempt);
      }
    }
  }
  throw lastErr || new Error('All Overpass endpoints failed');
}

await Actor.main(async () => {
  try {
    const input = await Actor.getInput() || {};
    const { bbox, keywords, city } = normalizeInput(input);
    const nameRegex = buildRegex(keywords);

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
    const elements = Array.isArray(json?.elements) ? json.elements : [];

    const rows = elements.map(el => {
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
      const confidence = domain ? 1.0 : (note ? 0.7 : 0.5);
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
    }).filter(Boolean);

    const deduped = dedupeRows(rows);
    await Actor.pushData(deduped);
    await Actor.setValue('RUN-SUMMARY', {
      found: rows.length, deduped: deduped.length, bbox, keywords, city,
      hint: 'Open the Dataset tab to export CSV.',
    });
  } catch (err) {
    log.error(`Fatal error: ${err.message}`);
    await Actor.setValue('ERROR', { message: err.message, stack: err.stack });
    // Don't rethrow → finish gracefully
  }
});
