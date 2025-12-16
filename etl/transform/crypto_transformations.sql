DROP VIEW IF EXISTS crypto_market_summary CASCADE;
DROP VIEW IF EXISTS crypto_price_changes CASCADE;
DROP VIEW IF EXISTS crypto_top_performers CASCADE;
DROP VIEW IF EXISTS crypto_market_overview CASCADE;


CREATE OR REPLACE VIEW crypto_market_summary AS
SELECT 
    id,
    symbol,
    name,
    image,
    current_price,
    market_cap,
    market_cap_rank,
    fully_diluted_valuation,
    total_volume,
    high_24h,
    low_24h,
    price_change_24h,
    price_change_percentage_24h,
    market_cap_change_24h,
    market_cap_change_percentage_24h,
    circulating_supply,
    total_supply,
    max_supply,
    ath,
    ath_change_percentage,
    ath_date,
    atl,
    atl_change_percentage,
    atl_date,
    last_updated,
    CAST(extracted_at AS TIMESTAMP) as extracted_at,
    -- Calculated fields
    CASE 
        WHEN market_cap_rank <= 10 THEN 'Top 10'
        WHEN market_cap_rank <= 50 THEN 'Top 50'
        WHEN market_cap_rank <= 100 THEN 'Top 100'
        ELSE 'Other'
    END as market_tier,
    ROUND((current_price / ath * 100)::numeric, 2) as pct_from_ath,
    ROUND((total_volume / market_cap * 100)::numeric, 2) as volume_to_mcap_ratio
FROM crypto_raw.market_data
WHERE current_price IS NOT NULL
ORDER BY market_cap_rank;

COMMENT ON VIEW crypto_market_summary IS 
'Cleaned cryptocurrency market data with calculated metrics like market tier and distance from ATH';



CREATE OR REPLACE VIEW crypto_price_changes AS
SELECT 
    id,
    symbol,
    name,
    current_price,
    market_cap_rank,
    -- Price changes
    price_change_24h,
    price_change_percentage_24h,
    price_change_percentage_1h_in_currency,
    price_change_percentage_7d_in_currency,
    price_change_percentage_30d_in_currency,
    -- Categorize price movement
    CASE 
        WHEN price_change_percentage_24h > 10 THEN 'Strong Gain'
        WHEN price_change_percentage_24h > 5 THEN 'Moderate Gain'
        WHEN price_change_percentage_24h > 0 THEN 'Slight Gain'
        WHEN price_change_percentage_24h > -5 THEN 'Slight Loss'
        WHEN price_change_percentage_24h > -10 THEN 'Moderate Loss'
        ELSE 'Strong Loss'
    END as price_movement_24h,
    -- Trend indicators
    CASE 
        WHEN price_change_percentage_1h_in_currency > 0 
         AND price_change_percentage_24h > 0 
         AND price_change_percentage_7d_in_currency > 0 
        THEN 'Bullish'
        WHEN price_change_percentage_1h_in_currency < 0 
         AND price_change_percentage_24h < 0 
         AND price_change_percentage_7d_in_currency < 0 
        THEN 'Bearish'
        ELSE 'Mixed'
    END as trend_signal,
    last_updated,
    CAST(extracted_at AS TIMESTAMP) as extracted_at
FROM crypto_raw.market_data
WHERE current_price IS NOT NULL
ORDER BY market_cap_rank;

COMMENT ON VIEW crypto_price_changes IS 
'Price change analysis with categorized movements and trend signals';



CREATE OR REPLACE VIEW crypto_top_performers AS
WITH ranked_coins AS (
    SELECT 
        id,
        symbol,
        name,
        current_price,
        market_cap,
        market_cap_rank,
        price_change_percentage_1h_in_currency as change_1h,
        price_change_percentage_24h as change_24h,
        price_change_percentage_7d_in_currency as change_7d,
        price_change_percentage_30d_in_currency as change_30d,
        total_volume,
        -- Rankings
        ROW_NUMBER() OVER (ORDER BY price_change_percentage_24h DESC) as rank_gainer_24h,
        ROW_NUMBER() OVER (ORDER BY price_change_percentage_24h ASC) as rank_loser_24h,
        ROW_NUMBER() OVER (ORDER BY price_change_percentage_7d_in_currency DESC) as rank_gainer_7d,
        ROW_NUMBER() OVER (ORDER BY price_change_percentage_7d_in_currency ASC) as rank_loser_7d,
        last_updated
    FROM crypto_raw.market_data
    WHERE current_price IS NOT NULL
      AND market_cap_rank <= 250  -- Focus on top coins
)
SELECT 
    id,
    symbol,
    name,
    current_price,
    market_cap,
    market_cap_rank,
    ROUND(change_1h::numeric, 2) as change_1h_pct,
    ROUND(change_24h::numeric, 2) as change_24h_pct,
    ROUND(change_7d::numeric, 2) as change_7d_pct,
    ROUND(change_30d::numeric, 2) as change_30d_pct,
    total_volume,
    CASE 
        WHEN rank_gainer_24h <= 10 THEN 'Top Gainer 24h'
        WHEN rank_loser_24h <= 10 THEN 'Top Loser 24h'
        WHEN rank_gainer_7d <= 10 THEN 'Top Gainer 7d'
        WHEN rank_loser_7d <= 10 THEN 'Top Loser 7d'
    END as performance_category,
    last_updated
FROM ranked_coins
WHERE rank_gainer_24h <= 10 
   OR rank_loser_24h <= 10
   OR rank_gainer_7d <= 10
   OR rank_loser_7d <= 10
ORDER BY 
    CASE 
        WHEN rank_gainer_24h <= 10 THEN rank_gainer_24h
        WHEN rank_loser_24h <= 10 THEN rank_loser_24h
        WHEN rank_gainer_7d <= 10 THEN rank_gainer_7d
        ELSE rank_loser_7d
    END;

COMMENT ON VIEW crypto_top_performers IS 
'Top 10 gainers and losers across 24h and 7d timeframes';



CREATE OR REPLACE VIEW crypto_market_overview AS
WITH market_stats AS (
    SELECT 
        COUNT(*) as total_coins,
        SUM(market_cap) as total_market_cap,
        SUM(total_volume) as total_volume_24h,
        AVG(price_change_percentage_24h) as avg_price_change_24h,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_change_percentage_24h) as median_price_change_24h,
        COUNT(CASE WHEN price_change_percentage_24h > 0 THEN 1 END) as coins_up,
        COUNT(CASE WHEN price_change_percentage_24h < 0 THEN 1 END) as coins_down,
        MAX(CAST(extracted_at AS TIMESTAMP)) as last_update
    FROM crypto_raw.market_data
    WHERE current_price IS NOT NULL
),
global_stats AS (
    SELECT 
        active_cryptocurrencies,
        markets,
        total_market_cap_usd,
        total_volume_usd,
        market_cap_change_percentage_24h,
        btc_dominance,
        eth_dominance,
        CAST(extracted_at AS TIMESTAMP) as global_last_update
    FROM crypto_raw.global_stats
    ORDER BY extracted_at DESC
    LIMIT 1
)
SELECT 
    -- Market stats from our data
    m.total_coins,
    m.total_market_cap,
    m.total_volume_24h,
    ROUND(m.avg_price_change_24h::numeric, 2) as avg_price_change_24h,
    ROUND(m.median_price_change_24h::numeric, 2) as median_price_change_24h,
    m.coins_up,
    m.coins_down,
    ROUND((m.coins_up::numeric / NULLIF(m.total_coins, 0) * 100), 2) as pct_coins_up,
    -- Global stats from CoinGecko
    g.active_cryptocurrencies as total_active_cryptos,
    g.markets as total_markets,
    g.total_market_cap_usd as global_market_cap,
    g.total_volume_usd as global_volume_24h,
    ROUND(g.market_cap_change_percentage_24h::numeric, 2) as global_mcap_change_24h,
    ROUND(g.btc_dominance::numeric, 2) as btc_dominance_pct,
    ROUND(g.eth_dominance::numeric, 2) as eth_dominance_pct,
    -- Timestamps
    m.last_update as market_data_updated,
    g.global_last_update as global_stats_updated
FROM market_stats m
CROSS JOIN global_stats g;

COMMENT ON VIEW crypto_market_overview IS 
'Aggregated market overview combining local and global statistics';


-- ============================================================================
-- 5. TRENDING COINS SUMMARY
-- ============================================================================
-- Current trending cryptocurrencies with enriched data
-- ============================================================================

CREATE OR REPLACE VIEW crypto_trending_summary AS
SELECT 
    t.rank as trending_rank,
    t.id,
    t.name,
    t.symbol,
    t.market_cap_rank,
    t.price_btc,
    t.score as trending_score,
    -- Enrich with market data if available
    m.current_price,
    m.market_cap,
    m.total_volume,
    m.price_change_percentage_24h,
    m.price_change_percentage_7d_in_currency as price_change_percentage_7d,
    CAST(t.extracted_at AS TIMESTAMP) as trending_extracted_at,
    m.last_updated as market_data_updated
FROM crypto_raw.trending_coins t
LEFT JOIN crypto_raw.market_data m ON t.id = m.id
ORDER BY t.rank;

COMMENT ON VIEW crypto_trending_summary IS 
'Trending coins enriched with current market data';


