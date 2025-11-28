-- This SQL query returns the source_document (the CPF of the consumer who made a transaction using Pix Dynamic QR Code in POS to a StoneCo merchant), the source_name, the city_code (IBGE code) where each consumer had the most distinct Pix transactions in the last 12 months from their last transaction date, and pix_transactions (the number of distinct Pix Dynamic QR Code in POS transactions considered).

CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.aux_tam_cpfs_by_city` AS

WITH

last_transaction AS (
  SELECT
    source_document,
    transaction_id LIKE 'STONEPOS%' AS is_stonepos,
    MAX(DATE(updated_at)) AS last_transaction_date
  FROM
    `dataplatform-prd.conta_stone.pix_dispatcher_pix_inbound_payment`
  WHERE 1=1
  GROUP BY
    1, 2
),

city AS (
select 
documento target_document, 
CAST(cod_muni_espacial AS INT) cod_muni,
from `dataplatform-prd.economic_research.geo_stoneco`
),

main AS (
  SELECT
    source_document,
    source_name,
    cod_muni,
    is_stonepos,
    COUNT(DISTINCT id) AS pix_transactions
  FROM (select *, transaction_id LIKE 'STONEPOS%' AS is_stonepos from  `dataplatform-prd.conta_stone.pix_dispatcher_pix_inbound_payment`)
  LEFT JOIN
    last_transaction USING (source_document, is_stonepos)
  LEFT JOIN
    city USING (target_document)
  WHERE 1=1
    AND LENGTH(source_document) = 11  -- only CPFs
    AND DATE(updated_at) >= DATE_SUB(last_transaction_date, INTERVAL 12 MONTH)  -- 12 months from each source_document's last transaction
  GROUP BY
    1, 2, 3, 4
)

SELECT
  source_document,
  source_name,
  cod_muni,
  pix_transactions
FROM
  main
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY source_document ORDER BY is_stonepos DESC, pix_transactions DESC) = 1
ORDER BY
  1