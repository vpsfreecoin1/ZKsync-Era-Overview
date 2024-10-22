WITH
  raw_tx_tbl AS (
    SELECT
      block_time,
      hash,
      "from" AS from_address,
      success,
      (gas_price * gas_used) / 1e18 AS tx_fee_eth,
      p.price * (gas_price * gas_used) / 1e18 AS tx_fee_usd
    FROM zksync.transactions
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', block_time)
        AND p.blockchain = 'ethereum'
        AND p.symbol = 'WETH'
  ),
  
  transactions_all_time AS (
    SELECT
      COUNT(hash) AS total_tx,
      COUNT(CASE WHEN success = true THEN 1 END) AS successful_tx,
      COUNT(CASE WHEN success = false THEN 1 END) AS failed_tx,
      COUNT(DISTINCT from_address) AS total_addresses
    FROM raw_tx_tbl
  ),
  
  transactions_last_30d AS (
    SELECT
      COUNT(hash) AS total_tx_last_30d,
      COUNT(CASE WHEN success = true THEN 1 END) AS successful_tx_last_30d,
      COUNT(CASE WHEN success = false THEN 1 END) AS failed_tx_last_30d,
      AVG(tx_fee_eth) AS tx_fee_eth_average_last_30d,
      AVG(tx_fee_usd) AS tx_fee_usd_average_last_30d,
      approx_percentile(tx_fee_eth, 0.5) AS tx_fee_eth_median_last_30d,
      approx_percentile(tx_fee_usd, 0.5) AS tx_fee_usd_median_last_30d
    FROM raw_tx_tbl
    WHERE block_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
  ),
  
contracts_raw_tbl AS (
    SELECT 
        COUNT(DISTINCT SUBSTR(topic3, 13, 64)) AS contracts_created,
        COUNT(DISTINCT tx_from) AS contract_creators
    FROM zksync.logs
    WHERE contract_address = 0x0000000000000000000000000000000000008006
    AND topic0 = 0x290afdae231a3fc0bbae8b1af63698b0a1d79b21ad17df0342dfb952fe74f8e5
    AND data = 0x
  )
  
SELECT
    total_tx,
    successful_tx,
    failed_tx,
    100.00 * (CAST(successful_tx_last_30d as DOUBLE) / CAST(total_tx_last_30d as DOUBLE)) as tx_success_percentage_last_30d,
    total_addresses,
    tx_fee_eth_average_last_30d,
    tx_fee_eth_median_last_30d,
    tx_fee_usd_average_last_30d,
    tx_fee_usd_median_last_30d,
    contracts_created,
    contract_creators
FROM transactions_all_time
CROSS JOIN transactions_last_30d
CROSS JOIN contracts_raw_tbl
