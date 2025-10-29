
-- ClickHouse-friendly version of your schema
-- No partitions, all tables have primary keys, relations preserved.
-- Run this in your zigindexer DB (or in a new DB). It is idempotent for CREATEs.

-- ====================================================================
-- ENUMS (idempotent)
-- ====================================================================
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname='token_type') THEN
    CREATE TYPE token_type AS ENUM ('native','factory','ibc','cw20');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname='pair_type') THEN
    CREATE TYPE pair_type AS ENUM ('xyk','concentrated','custom-concentrated');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname='trade_action') THEN
    CREATE TYPE trade_action AS ENUM ('swap','provide','withdraw');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname='trade_direction') THEN
    CREATE TYPE trade_direction AS ENUM ('buy','sell','provide','withdraw');
  END IF;
END$$;

-- ====================================================================
-- BASE TABLES
-- ====================================================================

-- TOKENS
CREATE TABLE IF NOT EXISTS public.tokens (
  token_id           BIGSERIAL PRIMARY KEY,
  denom              TEXT NOT NULL UNIQUE,
  type               token_type NOT NULL DEFAULT 'factory',
  name               TEXT,
  symbol             TEXT,
  display            TEXT,
  exponent           SMALLINT NOT NULL DEFAULT 6,
  image_uri          TEXT,
  website            TEXT,
  twitter            TEXT,
  telegram           TEXT,
  max_supply_base    NUMERIC(78,0),
  total_supply_base  NUMERIC(78,0),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  description        TEXT
);
CREATE INDEX IF NOT EXISTS idx_tokens_created_at ON public.tokens(created_at);

-- POOLS
CREATE TABLE IF NOT EXISTS public.pools (
  pool_id            BIGSERIAL PRIMARY KEY,
  pair_contract      TEXT NOT NULL UNIQUE,
  base_token_id      BIGINT NOT NULL REFERENCES public.tokens(token_id),
  quote_token_id     BIGINT NOT NULL REFERENCES public.tokens(token_id),
  lp_token_denom     TEXT,
  pair_type          TEXT NOT NULL,
  is_uzig_quote      BOOLEAN NOT NULL DEFAULT FALSE,
  factory_contract   TEXT,
  router_contract    TEXT,
  created_at         TIMESTAMPTZ,
  created_height     BIGINT,
  created_tx_hash    TEXT,
  signer             TEXT
);

ALTER TABLE public.pools
ADD COLUMN IF NOT EXISTS dex_id BIGINT REFERENCES public.dex_catalogue(dex_id),
ADD COLUMN IF NOT EXISTS chain_id BIGINT REFERENCES public.chain_catalogue(chain_id);

CREATE INDEX IF NOT EXISTS idx_pools_created_at      ON public.pools(created_at);
CREATE INDEX IF NOT EXISTS idx_pools_pair_contract   ON public.pools(pair_contract);
CREATE INDEX IF NOT EXISTS idx_pools_base_token_id   ON public.pools(base_token_id);
CREATE INDEX IF NOT EXISTS idx_pools_quote_token_id  ON public.pools(quote_token_id);
CREATE INDEX IF NOT EXISTS idx_pools_base_quote      ON public.pools(base_token_id, quote_token_id);
CREATE INDEX IF NOT EXISTS idx_pools_pair_type       ON public.pools(pair_type);

-- ====================================================================
-- TRADES (flat table, always has PK)
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.trades (
  trade_id                    BIGSERIAL PRIMARY KEY,
  pool_id                     BIGINT NOT NULL REFERENCES public.pools(pool_id),
  pair_contract               TEXT NOT NULL,
  action                      trade_action NOT NULL,
  direction                   trade_direction NOT NULL,
  offer_asset_denom           TEXT,
  offer_amount_base           NUMERIC(78,0),
  ask_asset_denom             TEXT,
  ask_amount_base             NUMERIC(78,0),
  return_amount_base          NUMERIC(78,0),
  is_router                   BOOLEAN NOT NULL DEFAULT FALSE,
  reserve_asset1_denom        TEXT,
  reserve_asset1_amount_base  NUMERIC(78,0),
  reserve_asset2_denom        TEXT,
  reserve_asset2_amount_base  NUMERIC(78,0),
  height                      BIGINT,
  tx_hash                     TEXT,
  signer                      TEXT,
  msg_index                   INT,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Unique index to preserve original uniqueness intent (tx_hash + pool + msg + time)
CREATE UNIQUE INDEX IF NOT EXISTS uq_trades_tx_pool_msg_time
  ON public.trades (tx_hash, pool_id, msg_index, created_at);

CREATE INDEX IF NOT EXISTS idx_trades_time               ON public.trades(created_at);
CREATE INDEX IF NOT EXISTS idx_trades_signer             ON public.trades(signer);
CREATE INDEX IF NOT EXISTS idx_trades_action_signer_time ON public.trades(action, signer, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_signer_time        ON public.trades(signer, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_pool_time          ON public.trades(pool_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_height             ON public.trades(height);
CREATE INDEX IF NOT EXISTS idx_trades_tx                 ON public.trades(tx_hash);

-- ====================================================================
-- HOLDERS + HOLDER STATS
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.holders (
  token_id         BIGINT NOT NULL REFERENCES public.tokens(token_id),
  address          TEXT   NOT NULL,
  balance_base     NUMERIC(78,0) NOT NULL,
  updated_at       TIMESTAMPTZ NOT NULL,
  last_seen_height BIGINT,
  PRIMARY KEY (token_id, address)
);
CREATE INDEX IF NOT EXISTS idx_holders_token_time ON public.holders(token_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_holders_address    ON public.holders(address);

CREATE TABLE IF NOT EXISTS public.token_holders_stats (
  token_id       BIGINT PRIMARY KEY REFERENCES public.tokens(token_id),
  holders_count  BIGINT NOT NULL,
  updated_at     TIMESTAMPTZ NOT NULL
);

-- ====================================================================
-- PRICES / PRICE TICKS (flat)
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.prices (
  price_id       BIGSERIAL PRIMARY KEY,
  token_id       BIGINT NOT NULL REFERENCES public.tokens(token_id),
  pool_id        BIGINT NOT NULL REFERENCES public.pools(pool_id),
  price_in_zig   NUMERIC(38,18) NOT NULL,
  is_pair_native BOOLEAN NOT NULL,
  updated_at     TIMESTAMPTZ NOT NULL,
  UNIQUE (token_id, pool_id)
);
CREATE INDEX IF NOT EXISTS idx_prices_token_time ON public.prices(token_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_prices_pool_time  ON public.prices(pool_id,  updated_at DESC);

CREATE TABLE IF NOT EXISTS public.price_ticks (
  id              BIGSERIAL PRIMARY KEY,
  pool_id         BIGINT NOT NULL REFERENCES public.pools(pool_id),
  token_id        BIGINT NOT NULL REFERENCES public.tokens(token_id),
  price_in_zig    NUMERIC(38,18) NOT NULL,
  ts              TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_price_ticks_ts ON public.price_ticks(ts);
CREATE INDEX IF NOT EXISTS idx_price_ticks_pool_token ON public.price_ticks(pool_id, token_id);

-- ====================================================================
-- OHLCV 1m (flat)
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.ohlcv_1m (
  id              BIGSERIAL PRIMARY KEY,
  pool_id         BIGINT NOT NULL REFERENCES public.pools(pool_id),
  bucket_start    TIMESTAMPTZ NOT NULL,
  open            NUMERIC(38,18) NOT NULL,
  high            NUMERIC(38,18) NOT NULL,
  low             NUMERIC(38,18) NOT NULL,
  close           NUMERIC(38,18) NOT NULL,
  volume_zig      NUMERIC(38,8)  NOT NULL DEFAULT 0,
  trade_count     INTEGER        NOT NULL DEFAULT 0,
  liquidity_zig   NUMERIC(38,8)
);
CREATE INDEX IF NOT EXISTS idx_ohlcv_pool_time ON public.ohlcv_1m(pool_id, bucket_start DESC);
CREATE INDEX IF NOT EXISTS idx_ohlcv_bucket_start ON public.ohlcv_1m(bucket_start);

-- ====================================================================
-- LIVE POOL STATE (reserves snapshot)
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.pool_state (
  pool_id            BIGINT PRIMARY KEY REFERENCES public.pools(pool_id),
  reserve_base_base  NUMERIC(78,0),
  reserve_quote_base NUMERIC(78,0),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ====================================================================
-- MATRIX TABLES (not partitioned)
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.pool_matrix (
  id                 BIGSERIAL PRIMARY KEY,
  pool_id            BIGINT NOT NULL REFERENCES public.pools(pool_id),
  bucket             TEXT   NOT NULL CHECK (bucket IN ('30m','1h','4h','24h')),
  vol_buy_quote      NUMERIC(38,8) NOT NULL DEFAULT 0,
  vol_sell_quote     NUMERIC(38,8) NOT NULL DEFAULT 0,
  vol_buy_zig        NUMERIC(38,8) NOT NULL DEFAULT 0,
  vol_sell_zig       NUMERIC(38,8) NOT NULL DEFAULT 0,
  tx_buy             INTEGER       NOT NULL DEFAULT 0,
  tx_sell            INTEGER       NOT NULL DEFAULT 0,
  unique_traders     INTEGER       NOT NULL DEFAULT 0,
  tvl_zig            NUMERIC(38,8),
  reserve_base_disp  NUMERIC(38,18),
  reserve_quote_disp NUMERIC(38,18),
  updated_at         TIMESTAMPTZ   NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pool_matrix_updated ON public.pool_matrix(updated_at DESC);

CREATE TABLE IF NOT EXISTS public.token_matrix (
  id           BIGSERIAL PRIMARY KEY,
  token_id     BIGINT NOT NULL REFERENCES public.tokens(token_id),
  bucket       TEXT   NOT NULL CHECK (bucket IN ('30m','1h','4h','24h')),
  price_in_zig NUMERIC(38,18),
  mcap_zig     NUMERIC(38,8),
  fdv_zig      NUMERIC(38,8),
  holders      BIGINT,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_token_matrix_bucket ON public.token_matrix(bucket, updated_at DESC);

CREATE TABLE IF NOT EXISTS public.token_holders_stats_flat (
  id           BIGSERIAL PRIMARY KEY,
  token_id     BIGINT REFERENCES public.tokens(token_id),
  holders_count BIGINT,
  updated_at   TIMESTAMPTZ NOT NULL
);

-- token_security (keeps id PK for replication)
CREATE TABLE IF NOT EXISTS public.token_security (
  id                BIGSERIAL PRIMARY KEY,
  token_id          BIGINT REFERENCES public.tokens(token_id),
  denom             TEXT,
  is_mintable       BOOLEAN,
  can_change_minting_cap BOOLEAN,
  max_supply_base   NUMERIC(78,0),
  total_supply_base NUMERIC(78,0),
  creator_address   TEXT,
  creator_balance_base NUMERIC(78,0),
  creator_pct_of_max NUMERIC(38,8),
  top10_pct_of_max  NUMERIC(38,8),
  holders_count     BIGINT,
  first_seen_at     TIMESTAMPTZ,
  risk_flags        TEXT[],
  checked_at        TIMESTAMPTZ
);

-- Fix token_security schema for JSONB + constraint
ALTER TABLE public.token_security
DROP COLUMN IF EXISTS risk_flags;

ALTER TABLE public.token_security
ADD COLUMN IF NOT EXISTS risk_flags JSONB;

ALTER TABLE public.token_security
ADD CONSTRAINT IF NOT EXISTS uq_token_security_token UNIQUE (token_id);


-- ====================================================================
-- LEADERBOARD & OUTLIERS (non-partitioned)
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.leaderboard_traders (
  id             BIGSERIAL PRIMARY KEY,
  bucket         TEXT   NOT NULL CHECK (bucket IN ('30m','1h','4h','24h')),
  address        TEXT   NOT NULL,
  trades_count   INT    NOT NULL,
  volume_zig     NUMERIC(38,8) NOT NULL,
  gross_pnl_zig  NUMERIC(38,8) NOT NULL,
  updated_at     TIMESTAMPTZ   NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_leaderboard_updated ON public.leaderboard_traders(updated_at DESC);

-- Ensure each trader has only one record per bucket
ALTER TABLE public.leaderboard_traders
  ADD CONSTRAINT uq_leaderboard_bucket_address
  UNIQUE (bucket, address);

CREATE TABLE IF NOT EXISTS public.large_trades (
  id              BIGSERIAL PRIMARY KEY,
  bucket          TEXT NOT NULL CHECK (bucket IN ('30m','1h','4h','24h')),
  pool_id         BIGINT NOT NULL REFERENCES public.pools(pool_id),
  tx_hash         TEXT NOT NULL,
  signer          TEXT,
  value_zig       NUMERIC(38,8) NOT NULL,
  direction       trade_direction NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL,
  inserted_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_large_trades_bucket_time ON public.large_trades(bucket, created_at DESC);

-- ====================================================================
-- FX
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.exchange_rates (
  id       BIGSERIAL PRIMARY KEY,
  ts       TIMESTAMPTZ NOT NULL DEFAULT now(),
  zig_usd  NUMERIC(38,8) NOT NULL
);

-- ====================================================================
-- INDEXER PROGRESS
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.index_state (
  id TEXT PRIMARY KEY,
  last_height BIGINT NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ====================================================================
-- WALLETS / WATCHLIST / ALERTS
-- ====================================================================
CREATE TABLE IF NOT EXISTS public.wallets (
  wallet_id     BIGSERIAL PRIMARY KEY,
  address       TEXT NOT NULL UNIQUE,
  display_name  TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen     TIMESTAMPTZ,
  last_seen_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_wallets_last_seen ON public.wallets(last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_wallets_last_seen_at ON public.wallets(last_seen_at DESC);

CREATE TABLE IF NOT EXISTS public.watchlist (
  id          BIGSERIAL PRIMARY KEY,
  wallet_id   BIGINT NOT NULL REFERENCES public.wallets(wallet_id) ON DELETE CASCADE,
  token_id    BIGINT REFERENCES public.tokens(token_id),
  pool_id     BIGINT REFERENCES public.pools(pool_id),
  note        TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_watchlist_wallet_token UNIQUE (wallet_id, token_id),
  CONSTRAINT uq_watchlist_wallet_pool  UNIQUE (wallet_id, pool_id)
);

CREATE TABLE IF NOT EXISTS public.alerts (
  alert_id        BIGSERIAL PRIMARY KEY,
  wallet_id       BIGINT NOT NULL REFERENCES public.wallets(wallet_id) ON DELETE CASCADE,
  alert_type      TEXT NOT NULL CHECK (alert_type IN ('price_cross','wallet_trade','large_trade','tvl_change')),
  params          JSONB NOT NULL,
  is_active       BOOLEAN NOT NULL DEFAULT TRUE,
  throttle_sec    INT NOT NULL DEFAULT 300,
  last_triggered  TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.alert_events (
  id            BIGSERIAL PRIMARY KEY,
  alert_id      BIGINT NOT NULL REFERENCES public.alerts(alert_id) ON DELETE CASCADE,
  wallet_id     BIGINT NOT NULL REFERENCES public.wallets(wallet_id) ON DELETE CASCADE,
  kind          TEXT NOT NULL,
  payload       JSONB NOT NULL,
  triggered_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_alert_events_alert_time ON public.alert_events(alert_id, triggered_at DESC);

-- ====================================================================
-- UTIL / NORMALIZER (keep existing function)
-- ====================================================================
CREATE OR REPLACE FUNCTION public.norm_twitter_handle(in_raw TEXT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE STRICT AS $$
SELECT lower(
    regexp_replace(
        regexp_replace(
            regexp_replace(
                coalesce(in_raw, ''),
                '^(https?://)?(www\.)?(x|twitter)\.com/', 
                '', 'i'
            ), 
            '^@', '', 'i'
        ), 
        '[/\?\#].*$', '', 'g'
    )
);
$$;
CREATE INDEX IF NOT EXISTS idx_tokens_twitter_handle ON public.tokens (public.norm_twitter_handle(twitter));

-- token_twitter (keep the same shape)
CREATE TABLE IF NOT EXISTS public.token_twitter (
    token_id BIGINT PRIMARY KEY REFERENCES public.tokens(token_id) ON DELETE CASCADE,
    handle TEXT NOT NULL,
    user_id TEXT,
    profile_url TEXT,
    name TEXT,
    is_blue_verified BOOLEAN,
    verified_type TEXT,
    profile_picture TEXT,
    cover_picture TEXT,
    description TEXT,
    location TEXT,
    followers BIGINT,
    following BIGINT,
    favourites_count BIGINT,
    statuses_count BIGINT,
    media_count BIGINT,
    can_dm BOOLEAN,
    created_at_twitter TIMESTAMPTZ,
    possibly_sensitive BOOLEAN,
    is_automated BOOLEAN,
    automated_by TEXT,
    pinned_tweet_ids TEXT[],
    unavailable BOOLEAN,
    unavailable_message TEXT,
    unavailable_reason TEXT,
    raw JSONB,
    last_refreshed TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    last_error_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_token_twitter_handle ON public.token_twitter(handle);
CREATE INDEX IF NOT EXISTS idx_token_twitter_last_refreshed ON public.token_twitter(last_refreshed DESC);

 -- 1) add a proper conflict target for the upsert
CREATE UNIQUE INDEX IF NOT EXISTS ux_ohlcv_pool_bucket
ON public.ohlcv_1m (pool_id, bucket_start);

-- Make the conflict targets exist again.

-- pool_matrix needs uniqueness by (pool_id, bucket)
ALTER TABLE public.pool_matrix
  ADD CONSTRAINT uq_pool_matrix_pool_bucket
  UNIQUE (pool_id, bucket);

-- token_matrix needs uniqueness by (token_id, bucket)
ALTER TABLE public.token_matrix
  ADD CONSTRAINT uq_token_matrix_token_bucket
  UNIQUE (token_id, bucket);

CREATE TABLE IF NOT EXISTS public.chain_catalogue (
  chain_id      SERIAL PRIMARY KEY,
  chain_name    TEXT NOT NULL UNIQUE,
  chain_prefix  TEXT,
  lcd_endpoint  TEXT,
  rpc_endpoint  TEXT,
  explorer_url  TEXT,
  created_at    TIMESTAMPTZ DEFAULT now(),
  CONSTRAINT uq_chain_catalogue_name UNIQUE (chain_name),
  CONSTRAINT uq_chain_catalogue_prefix UNIQUE (chain_prefix)
);


CREATE TABLE IF NOT EXISTS public.dex_catalogue (
  dex_id           SERIAL PRIMARY KEY,
  dex_name         TEXT NOT NULL,
  factory_contract TEXT NOT NULL UNIQUE,
  router_contract  TEXT,
  chain_id         INTEGER REFERENCES public.chain_catalogue(chain_id),
  website          TEXT,
  twitter          TEXT,
  created_at       TIMESTAMPTZ DEFAULT now(),
  CONSTRAINT uq_dex_catalogue_factory UNIQUE (factory_contract),
  CONSTRAINT uq_dex_catalogue_name_chain UNIQUE (dex_name, chain_id)
);
