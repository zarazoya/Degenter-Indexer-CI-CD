// core/pool_state.js
import { DB } from '../lib/db.js';
import BatchQueue from '../lib/batch.js';

const UPSERT_SQL = `
  INSERT INTO pool_state(pool_id, reserve_base_base, reserve_quote_base, updated_at)
  VALUES %VALUES%
  ON CONFLICT (pool_id) DO UPDATE SET
    reserve_base_base  = EXCLUDED.reserve_base_base,
    reserve_quote_base = EXCLUDED.reserve_quote_base,
    updated_at         = now()
`;

function sqlValues(rows) {
  const vals = [];
  const args = [];
  let i = 1;
  for (const r of rows) {
    vals.push(`($${i++},$${i++},$${i++}, now())`);
    args.push(r.pool_id, r.reserve_base_base, r.reserve_quote_base);
  }
  return { text: UPSERT_SQL.replace('%VALUES%', vals.join(',')), args };
}

function dedupeLastWins(items) {
  // Map by pool_id; keep the LAST occurrence (latest state in the batch)
  const m = new Map();
  for (const it of items) m.set(it.pool_id, it);
  return Array.from(m.values());
}

const stateQueue = new BatchQueue({
  maxItems: Number(process.env.STATE_BATCH_MAX || 400),
  maxWaitMs: Number(process.env.STATE_BATCH_WAIT_MS || 120),
  flushFn: async (items) => {
    // collapse duplicates to avoid "ON CONFLICT ... affect row a second time"
    const compact = dedupeLastWins(items);
    if (compact.length === 0) return;
    const { text, args } = sqlValues(compact);
    await DB.query(text, args);
  }
});

export async function upsertPoolState(pool_id, baseDenom, quoteDenom, res1d, res1a, res2d, res2a) {
  if (!res1d || !res2d || !res1a || !res2a) return;
  let base = null, quote = null;
  if (res1d === baseDenom && res2d === quoteDenom) { base = res1a; quote = res2a; }
  else if (res2d === baseDenom && res1d === quoteDenom) { base = res2a; quote = res1a; }
  if (!base || !quote) return;
  stateQueue.push({ pool_id, reserve_base_base: base, reserve_quote_base: quote });
}

export async function drainPoolState() {
  await stateQueue.drain();
}
