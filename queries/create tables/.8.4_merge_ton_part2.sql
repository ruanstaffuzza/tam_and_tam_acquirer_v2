DECLARE ref_month  DATE DEFAULT '{{ ref_month }}';


--CREATE OR REPLACE TABLE  `dataplatform-prd.master_contact.v2_aux_tam_ton_match_part2` PARTITION BY reference_date AS

INSERT INTO `dataplatform-prd.master_contact.v2_aux_tam_ton_match_part2`

-- Parâmetros: substitua os nomes reais das tabelas abaixo
WITH

-- Inicial: tabela A completa
df_a_remaining_6 AS (
  SELECT distinct
  document, 
  LEFT(name, 30) AS name, 
  de42,
  reference_date,
  cod_muni,
  --postal_code_7d postal_code_7d_a,
  --de42 AS de42_a,
  --name AS name_a,
  postal_code_7d,
  postal_code_6d,
  postal_code_5d,
  postal_code_4d,
  FROM `dataplatform-prd.master_contact.v2_aux_tam_ton_auth`
  LEFT JOIN (select distinct reference_date, document, from `dataplatform-prd.master_contact.v2_aux_tam_ton_match_part1`) a using(reference_date, document)
  WHERE 1=1
  AND a.document IS NULL
  AND LENGTH(name) >= 4
  AND reference_date = ref_month
),

-- Inicial: tabela M completa
df_m_remaining_6 AS (
  SELECT 
  merchant_market_hierarchy_id, 
  name, 
  de42,
  reference_date,
  cod_muni,
  --postal_code_7d postal_code_7d_m,
  --de42 AS de42_m,
  --name AS name_m,
  postal_code_7d,
  postal_code_6d,
  postal_code_5d,
  postal_code_4d,
   FROM `dataplatform-prd.master_contact.v2_aux_tam_ton_mastercard`
   left join (select distinct reference_date, merchant_market_hierarchy_id, from `dataplatform-prd.master_contact.v2_aux_tam_ton_match_part1`) m using(reference_date, merchant_market_hierarchy_id)
    WHERE 1=1
    AND m.merchant_market_hierarchy_id IS NULL
    AND LENGTH(name) >= 4
    AND reference_date = ref_month
),

-------------------------------------------------------------------------------------
-- 7. Sétimo nível de merge (reference_date, postal_code_5d) - exact match
-------------------------------------------------------------------------------------
merge_level_7 AS (
  SELECT *,
  'reference_date, cod_muni, postal_code_5d, name' AS tipo_merge
  from df_a_remaining_6 a
  INNER JOIN df_m_remaining_6 m using(reference_date, cod_muni, postal_code_5d, name)
),

df_a_remaining_7 AS (
  SELECT * FROM df_a_remaining_6 a
  LEFT JOIN (SELECT DISTINCT document, reference_date, FROM merge_level_7 a) e using(document, reference_date)
  WHERE e.document IS NULL
),

df_m_remaining_7 AS (
  SELECT * FROM df_m_remaining_6 m
  LEFT JOIN (SELECT DISTINCT merchant_market_hierarchy_id, reference_date, FROM merge_level_7) e using(merchant_market_hierarchy_id, reference_date)
  WHERE e.merchant_market_hierarchy_id IS NULL  
),


-------------------------------------------------------------------------------------
-- 8. Nono nível de merge (reference_date, postal_code_4d) - exact match
-------------------------------------------------------------------------------------
merge_level_8 AS (
  SELECT *,
    'reference_date, cod_muni, postal_code_4d, name' AS tipo_merge
  from df_a_remaining_7 a
  INNER JOIN df_m_remaining_7 m using(reference_date, cod_muni, postal_code_4d, name)
),

df_a_remaining_8 AS (
  SELECT * FROM df_a_remaining_7 a
  LEFT JOIN (SELECT DISTINCT document, reference_date, FROM merge_level_8 a) e using(document, reference_date)
  WHERE e.document IS NULL
),

df_m_remaining_8 AS (
  SELECT * FROM df_m_remaining_7 m
  LEFT JOIN (SELECT DISTINCT merchant_market_hierarchy_id, reference_date, FROM merge_level_8) e using(merchant_market_hierarchy_id, reference_date)
  WHERE e.merchant_market_hierarchy_id IS NULL
),



-------------------------------------------------------------------------------------
-- 9. Décimo primeiro nível de merge (reference_date, de42) - exact match
-------------------------------------------------------------------------------------
merge_level_9 AS (
  SELECT *,
    'reference_date, cod_muni, de42, name' AS tipo_merge
  from df_a_remaining_8 a
  INNER JOIN df_m_remaining_8 m using(reference_date, cod_muni, de42, name)
),

df_a_remaining_9 AS (
  SELECT * FROM df_a_remaining_8 a
  LEFT JOIN (SELECT DISTINCT document, reference_date, FROM merge_level_9 a) e using(document, reference_date)
  WHERE e.document IS NULL
),

df_m_remaining_9 AS (
  SELECT * FROM df_m_remaining_8 m
  LEFT JOIN (SELECT DISTINCT merchant_market_hierarchy_id, reference_date, FROM merge_level_9) e using(merchant_market_hierarchy_id, reference_date)
  WHERE e.merchant_market_hierarchy_id IS NULL
),

-------------------------------------------------------------------------------------
-- 10. Décimo segundo nível de merge (reference_date, de42) - fuzz match partial ratio
-------------------------------------------------------------------------------------
merge_level_10 AS (
  SELECT *,
 'reference_date, cod_muni, de42, fuzzy name' AS tipo_merge,
  FROM df_a_remaining_9 a
  INNER JOIN df_m_remaining_9 m
  USING(reference_date, cod_muni, de42)
  WHERE 1=0
  or (length(a.name) >= length(m.name) and EDIT_DISTANCE(LEFT(a.name, length(m.name)), m.name, max_distance => 7) / length(m.name) < 0.2)
  OR (length(m.name) >= length(a.name) and EDIT_DISTANCE(LEFT(m.name, length(a.name)), a.name,  max_distance => 7) / length(a.name) < 0.2)
)


select merchant_market_hierarchy_id, document, reference_date, cod_muni, tipo_merge, from merge_level_7

union all select merchant_market_hierarchy_id, document, reference_date, cod_muni, tipo_merge, from merge_level_8

union all select merchant_market_hierarchy_id, document, reference_date, cod_muni, tipo_merge, from merge_level_9

union all select merchant_market_hierarchy_id, document, reference_date, cod_muni, tipo_merge, from merge_level_10


