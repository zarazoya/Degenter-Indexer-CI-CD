// core/portfolio.js
import { DB } from '../lib/db.js';

export async function updateWalletPortfolio(walletAddress) {
   console.log("[portfolio] running for wallet:", walletAddress);
  if (!walletAddress) return;

  // Compute total holdings in ZIG + USD
  const { rows } = await DB.query(`
    SELECT 
      SUM((h.balance_base::NUMERIC / POW(10, t.exponent)) * COALESCE(p.price_in_zig, 0)) AS total_zig,
      COUNT(DISTINCT h.token_id) AS token_count
    FROM holders h
    JOIN tokens t USING (token_id)
    LEFT JOIN prices p USING (token_id)
    WHERE h.address = $1 AND h.balance_base::NUMERIC > 0
  `, [walletAddress]);

  const totalZig = Number(rows?.[0]?.total_zig || 0);
  const tokenCount = Number(rows?.[0]?.token_count || 0);

  // Convert ZIG → USD if you have exchange_rates
  const { rows: exr } = await DB.query(`
    SELECT zig_usd AS usd_rate FROM exchange_rates ORDER BY ts DESC LIMIT 1
  `);
  const zigUsd = Number(exr?.[0]?.usd_rate || 0.0);
  const totalUsd = totalZig * zigUsd;

  await DB.query(`
    INSERT INTO wallet_portfolios(address, total_value_zig, total_value_usd, total_tokens, last_updated)
    VALUES ($1,$2,$3,$4,now())
    ON CONFLICT (address)
    DO UPDATE SET
      total_value_zig = EXCLUDED.total_value_zig,
      total_value_usd = EXCLUDED.total_value_usd,
      total_tokens = EXCLUDED.total_tokens,
      last_updated = now()
  `, [walletAddress, totalZig, totalUsd, tokenCount]);

  // ───────────────────────────────────────────────
  // Per-token breakdown into wallet_portfolio_tokens
  // ───────────────────────────────────────────────
  const { rows: tokenRows } = await DB.query(`
    SELECT 
      h.token_id,
      (h.balance_base::NUMERIC / POW(10, t.exponent)) AS balance_token,
      COALESCE(p.price_in_zig, 0) AS price_in_zig
    FROM holders h
    JOIN tokens t USING (token_id)
    LEFT JOIN prices p USING (token_id)
    WHERE h.address = $1 AND h.balance_base::NUMERIC > 0
  `, [walletAddress]);

  for (const r of tokenRows) {
    const tokenValueZig = r.balance_token * r.price_in_zig;
    const tokenValueUsd = tokenValueZig * zigUsd;

    await DB.query(`
      INSERT INTO wallet_portfolio_tokens(address, token_id, value_zig, value_usd, updated_at)
      VALUES ($1,$2,$3,$4,now())
      ON CONFLICT (address, token_id) DO UPDATE SET
        value_zig = EXCLUDED.value_zig,
        value_usd = EXCLUDED.value_usd,
        updated_at = now()
    `, [walletAddress, r.token_id, tokenValueZig, tokenValueUsd]);
  }

}
