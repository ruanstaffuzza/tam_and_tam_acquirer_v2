
DECLARE ref_month  DATE DEFAULT '{{ref_month}}';

--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.v2_aux_tam_ton_to_mmhid` PARTITION BY reference_date AS

INSERT INTO `dataplatform-prd.master_contact.v2_aux_tam_ton_to_mmhid`


-- Step 1: Get distinct document_auth and merchant_market_hierarchy_id


with aux as (
select * from `dataplatform-prd.master_contact.v2_aux_tam_ton_match_part1` 
where reference_date=ref_month
union all   
select * from `dataplatform-prd.master_contact.v2_aux_tam_ton_match_part2` 
where reference_date=ref_month
),

base AS (
  SELECT 
  DISTINCT 
  document, 
  merchant_market_hierarchy_id,
  reference_date
  FROM aux
),

-- Step 2: Identify merchant_market_hierarchy_id with multiple document (nunique > 1)
mmhid_multi_doc AS (
  SELECT merchant_market_hierarchy_id, reference_date
  FROM base
  GROUP BY merchant_market_hierarchy_id, reference_date
  HAVING COUNT(DISTINCT document) > 1
),

-- Step 3: Remove mmhid associated with multiple document, keep only document unique list in these cases
filtered_step1 AS (
  SELECT document, merchant_market_hierarchy_id, reference_date
  FROM base
  left join mmhid_multi_doc USING(merchant_market_hierarchy_id, reference_date) 
  WHERE mmhid_multi_doc.merchant_market_hierarchy_id IS NULL
  UNION DISTINCT

  SELECT DISTINCT document, NULL AS merchant_market_hierarchy_id, reference_date
  FROM base
  left join mmhid_multi_doc USING(merchant_market_hierarchy_id, reference_date) 
  WHERE mmhid_multi_doc.merchant_market_hierarchy_id IS NOT NULL
),

-- Step 4: Identify document associated with more than 5 mmhid
docs_multi_mmhid AS (
  SELECT document, reference_date
  FROM filtered_step1
  GROUP BY document, reference_date
  HAVING COUNT(DISTINCT merchant_market_hierarchy_id) > 5
),

-- Step 5: Remove document with more than 5 mmhid, keep only document unique list in these cases
filtered_step2 AS (
  SELECT document, merchant_market_hierarchy_id, reference_date
  FROM filtered_step1
  left join docs_multi_mmhid USING(document, reference_date)
  where docs_multi_mmhid.document IS NULL

  UNION DISTINCT

  SELECT document, NULL AS merchant_market_hierarchy_id, reference_date
  FROM docs_multi_mmhid
)

-- Final SELECT with merchant_market_hierarchy_id as NULL for missing values
SELECT 
  document document_auth,
  merchant_market_hierarchy_id,
  CURRENT_DATE() AS inserted_at,
  reference_date,
FROM filtered_step2