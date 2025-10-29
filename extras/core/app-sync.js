// core/app-sync.js
import 'dotenv/config';
import { DB_APP, DB_INDEXER, init, close } from '../lib/db.js';

/**
 * Sync one wallet’s balances and total portfolio value
 * from the Indexer DB → App DB.
 */
export async function syncWallet(address) {
  if (!address) throw new Error('❌ Missing wallet address');
  console.log(`\n🔄 Syncing wallet: ${address}`);

  // 🧩 1️⃣ Fetch all holdings for this wallet
  const { rows: holdings } = await DB_INDEXER.query(
    `SELECT token_id, balance_base::NUMERIC AS balance_base
     FROM holders
     WHERE address = $1 AND balance_base::NUMERIC > 0`,
    [address]
  );

  if (!holdings.length) {
    console.log('⚠️ No active holdings found.');
    return;
  }

  // 🧩 2️⃣ Get latest ZIG→USD rate
  const { rows: exr } = await DB_INDEXER.query(
    `SELECT zig_usd FROM exchange_rates ORDER BY ts DESC LIMIT 1`
  );
  const zigUsd = Number(exr?.[0]?.zig_usd || 0.0);

  // 🧩 3️⃣ Process each token and store in wallet_assets
  for (const h of holdings) {
    const tokenId = h.token_id;
    const balanceBase = Number(h.balance_base) || 0;

    // Get price in ZIG
    const { rows: pr } = await DB_INDEXER.query(
      `SELECT price_in_zig FROM prices WHERE token_id=$1 ORDER BY updated_at DESC LIMIT 1`,
      [tokenId]
    );
    const priceInZig = Number(pr?.[0]?.price_in_zig || 0);

    // Compute values
    const valueZig = balanceBase * priceInZig;
    const valueUsd = valueZig * zigUsd;

    // Insert or update in App DB
    await DB_APP.query(
      `
      INSERT INTO wallet_portfolio.wallet_assets (wallet_id, token_id, balance, value_zig, value_usd, updated_at)
      VALUES (
        (SELECT wallet_id FROM wallet_portfolio.wallets WHERE address = $1),
        $2, $3, $4, $5, now()
      )
      ON CONFLICT (wallet_id, token_id)
      DO UPDATE SET
        balance = EXCLUDED.balance,
        value_zig = EXCLUDED.value_zig,
        value_usd = EXCLUDED.value_usd,
        updated_at = now();
      `,
      [address, tokenId, balanceBase, valueZig, valueUsd]
    );

    console.log(
      `✅ Token ${tokenId}: balance=${balanceBase.toFixed(2)}, value_zig=${valueZig.toFixed(
        4
      )}, value_usd=${valueUsd.toFixed(4)}`
    );
  }

  // 🧩 4️⃣ Aggregate totals into wallet_portfolio
  const { rows: totals } = await DB_APP.query(
    `SELECT
        SUM(value_zig) AS total_zig,
        SUM(value_usd) AS total_usd
     FROM wallet_portfolio.wallet_assets
     WHERE wallet_id = (SELECT wallet_id FROM wallet_portfolio.wallets WHERE address = $1)`,
    [address]
  );

  const totalZig = Number(totals?.[0]?.total_zig || 0);
  const totalUsd = Number(totals?.[0]?.total_usd || 0);

  await DB_APP.query(
    `
    INSERT INTO wallet_portfolio.wallet_portfolio (wallet_id, total_value_zig, total_value_usd, last_updated)
    VALUES ((SELECT wallet_id FROM wallet_portfolio.wallets WHERE address = $1), $2, $3, now())
    ON CONFLICT (wallet_id)
    DO UPDATE SET
      total_value_zig = EXCLUDED.total_value_zig,
      total_value_usd = EXCLUDED.total_value_usd,
      last_updated = now();
    `,
    [address, totalZig, totalUsd]
  );

  console.log(`\n💰 Total Portfolio → ${totalZig.toFixed(4)} ZIG | $${totalUsd.toFixed(4)} USD`);
  console.log(`✅ Wallet ${address} synced successfully.\n`);
}

/**
 * Run directly from CLI: node core/app-sync.js zig1abc...
 */
if (process.argv[2]) {
  const addr = process.argv[2];
  await init();
  await syncWallet(addr).catch((e) => console.error('❌ Error:', e.message));
  await close();
} else {
  console.log('Usage: node core/app-sync.js <wallet_address>');
}
