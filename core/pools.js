// core/pools.js
import { DB } from '../lib/db.js';
import { upsertTokenMinimal } from './tokens.js';
import { info } from '../lib/log.js';

export async function upsertPool({ pairContract, baseDenom, quoteDenom, pairType, createdAt, height, txHash, signer, factoryContract}) {
  const { rows: dexRows } = await DB.query(
    `SELECT dex_id, chain_id FROM dex_catalogue WHERE factory_contract = $1 LIMIT 1`,
    [factoryContract] // currently just 1 factory (OroSwap)
  );

  let dex_id = dexRows?.[0]?.dex_id || null;
  let chain_id = dexRows?.[0]?.chain_id || null;

  // 🧠 Auto-register unseen factories
  if (!dex_id) {
    const dexKey = factoryContract.slice(0, 12);
    const { rows } = await DB.query(
      `INSERT INTO dex_catalogue (dex_name, factory_contract, chain_id)
       VALUES ($1, $2, $3)
       ON CONFLICT (factory_contract) DO NOTHING
       RETURNING dex_id`,
      ['UnknownDEX', factoryContract, 1] // chain_id=1 = ZigChain, you can adjust if multi-chain later
    );
    dex_id = rows?.[0]?.dex_id || dex_id;
    chain_id = chain_id || 1; // fallback chain_id
  }


  const baseId  = await upsertTokenMinimal(baseDenom);
  const quoteId = await upsertTokenMinimal(quoteDenom);
  const isUzig  = (quoteDenom === 'uzig');
  const { rows } = await DB.query(
    `INSERT INTO pools(pair_contract, base_token_id, quote_token_id, pair_type, is_uzig_quote, created_at, created_height, created_tx_hash, signer, dex_id, chain_id)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
     ON CONFLICT (pair_contract) DO UPDATE SET
       base_token_id = EXCLUDED.base_token_id,
       quote_token_id = EXCLUDED.quote_token_id,
       pair_type = EXCLUDED.pair_type,
       dex_id = EXCLUDED.dex_id,
       chain_id = EXCLUDED.chain_id
     RETURNING pool_id`,
     [pairContract, baseId, quoteId, String(pairType), isUzig, createdAt, height, txHash, signer, dex_id, chain_id]
  );
  info('POOL UPSERT:', pairContract, `${baseDenom}/${quoteDenom}`, pairType, `(DEX=${dex_id || 'none'}, CHAIN=${chain_id || 'none'})`, 'pool_id=', rows[0].pool_id);
  return rows[0].pool_id;
}

export async function poolWithTokens(pairContract) {
  const { rows } = await DB.query(`
    SELECT p.pool_id, p.is_uzig_quote,
           b.token_id AS base_id, b.denom AS base_denom, COALESCE(b.exponent,6) AS base_exp,
           q.token_id AS quote_id, q.denom AS quote_denom, COALESCE(q.exponent,6) AS quote_exp
    FROM pools p
    JOIN tokens b ON b.token_id=p.base_token_id
    JOIN tokens q ON q.token_id=p.quote_token_id
    WHERE p.pair_contract=$1`, [pairContract]);
  return rows[0] || null;
}

