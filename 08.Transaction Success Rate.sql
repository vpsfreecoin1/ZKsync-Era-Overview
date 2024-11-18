WITH
  raw_tx_tbl AS (
    SELECT
      DATE_TRUNC('{{Interval}}', block_time) AS tx_date,
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

  transactions_tbl AS (
    SELECT
      tx_date,
      COUNT(hash) AS total_tx,
      COUNT(CASE WHEN success = true THEN 1 END) AS successful_tx,
      COUNT(CASE WHEN success = false THEN 1 END) AS failed_tx,
      AVG(tx_fee_eth) AS tx_fee_eth_average,
      approx_percentile(tx_fee_eth, 0.5) AS tx_fee_eth_median,
      approx_percentile(tx_fee_eth, 0.1) AS tx_fee_eth_percentile_10,
      approx_percentile(tx_fee_eth, 0.9) AS tx_fee_eth_percentile_90,
      AVG(tx_fee_usd) AS tx_fee_usd_average,
      approx_percentile(tx_fee_usd, 0.5) AS tx_fee_usd_median,
      approx_percentile(tx_fee_usd, 0.1) AS tx_fee_usd_percentile_10,
      approx_percentile(tx_fee_usd, 0.9) AS tx_fee_usd_percentile_90
    FROM raw_tx_tbl
    GROUP BY 1    
  ),
  
  cumulative_transactions_tbl AS (
    SELECT
      tx_date,
      SUM(COUNT(hash)) OVER (ORDER BY tx_date ASC) AS cumulative_total_tx,
      SUM(COUNT(CASE WHEN success = true THEN 1 END)) OVER (ORDER BY tx_date ASC) AS cumulative_successful_tx,
      SUM(COUNT(CASE WHEN success = false THEN 1 END)) OVER (ORDER BY tx_date ASC) AS cumulative_failed_tx
    FROM raw_tx_tbl
    GROUP BY 1
  ),
  
  first_tx_tbl AS (
    SELECT 
        MIN(tx_date) AS first_tx_date,
        from_address
    FROM raw_tx_tbl
    WHERE success = true
    GROUP BY 2
  ),
  
  new_addresses_tbl AS (
    SELECT
      first_tx_date,
      COUNT(from_address) AS new_addresses
    FROM first_tx_tbl
    GROUP BY 1
  ),
  
  active_addresses_tbl AS (
    SELECT
        tx_date,
        COUNT(DISTINCT from_address) AS active_addresses
    FROM raw_tx_tbl
    WHERE success = true
    GROUP BY 1
  ),
  
  address_agg_tbl AS (
    SELECT
      a.tx_date,
      a.active_addresses,
      n.new_addresses,
      SUM(n.new_addresses) OVER (ORDER BY n.first_tx_date) AS cumulative_unique_addresses
    FROM active_addresses_tbl AS a
    LEFT JOIN new_addresses_tbl AS n ON a.tx_date = n.first_tx_date
  ),
  
  contracts_raw_tbl AS (
    SELECT 
      DATE_TRUNC('{{Interval}}', block_time)  AS tx_date,
      SUBSTR(topic3, 13, 64) AS contract_address,
      tx_from AS contract_creator
    FROM zksync.logs
    WHERE contract_address = 0x0000000000000000000000000000000000008006
    AND topic0 = 0x290afdae231a3fc0bbae8b1af63698b0a1d79b21ad17df0342dfb952fe74f8e5
    AND data = 0x
  ),
  
  contracts_sum_tbl AS (
    SELECT
      tx_date,
      count(distinct contract_address) AS contracts_created,
      count(distinct contract_creator) AS contract_creators
    FROM contracts_raw_tbl
    GROUP BY 1
  ),
  
  first_create_contract_tx_tbl AS (
    SELECT 
        MIN(tx_date) AS first_create_contract_tx_date,
        contract_creator
    FROM contracts_raw_tbl
    GROUP BY 2
  ),
  
  new_create_contract_addresses_tbl AS (
    SELECT
      first_create_contract_tx_date,
      COUNT(contract_creator) AS new_contract_creators
    FROM first_create_contract_tx_tbl
    GROUP BY 1
  ),
  
  contracts_agg_tbl AS (
      SELECT
        c.tx_date,
        c.contracts_created,
        c.contract_creators,
        SUM(contracts_created) OVER (ORDER BY tx_date ASC) AS cumulative_contracts_created,
        SUM(new_contract_creators) OVER (ORDER BY n.first_create_contract_tx_date) AS cumulative_unique_contract_creators
      FROM contracts_sum_tbl c
      LEFT JOIN new_create_contract_addresses_tbl n ON c.tx_date = n.first_create_contract_tx_date
  )

SELECT
  t.tx_date,
  t.total_tx,
  t.successful_tx,
  t.failed_tx,
  c.cumulative_total_tx,
  c.cumulative_successful_tx,
  c.cumulative_failed_tx,
  CAST(t.successful_tx AS DOUBLE) / CAST(t.total_tx AS DOUBLE) AS tx_success_rate,
  AVG(CAST(t.successful_tx AS DOUBLE) / CAST(t.total_tx AS DOUBLE)) OVER (ORDER BY t.tx_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS tx_success_rate_7day_ma,
  a.active_addresses,
  a.new_addresses,
  a.cumulative_unique_addresses,
  t.tx_fee_eth_average,
  t.tx_fee_eth_median,
  t.tx_fee_eth_percentile_10,
  t.tx_fee_eth_percentile_90,
  t.tx_fee_usd_average,
  t.tx_fee_usd_median,
  t.tx_fee_usd_percentile_10,
  t.tx_fee_usd_percentile_90,
  ctr.contracts_created,
  ctr.contract_creators,
  ctr.cumulative_contracts_created,
  ctr.cumulative_unique_contract_creators
FROM transactions_tbl t
LEFT JOIN cumulative_transactions_tbl c ON t.tx_date = c.tx_date
LEFT JOIN address_agg_tbl a ON t.tx_date = a.tx_date
LEFT JOIN contracts_agg_tbl ctr ON t.tx_date = ctr.tx_date
WHERE t.tx_date >= CURRENT_TIMESTAMP - INTERVAL '{{Period_Days}}' DAY
AND t.tx_date < DATE_TRUNC('{{Interval}}', CURRENT_DATE) -- Exclude current time interval to ensure that only "complete" intervals with full data are included
-- AND t.tx_date <= DATE_ADD('{{Interval}}', -1, CURRENT_DATE) -- Exclude current time date to ensure that only days with a full 24 hours of data are included
ORDER BY 1 asc
