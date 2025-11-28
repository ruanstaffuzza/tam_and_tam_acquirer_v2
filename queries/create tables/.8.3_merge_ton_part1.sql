DECLARE ref_month  DATE DEFAULT '{{ ref_month }}';



--CREATE OR REPLACE TABLE  `dataplatform-prd.master_contact.v2_aux_tam_ton_match_part1` PARTITION BY reference_date AS
INSERT INTO `dataplatform-prd.master_contact.v2_aux_tam_ton_match_part1`


-- Parâmetros: substitua os nomes reais das tabelas abaixo
WITH

-- Inicial: tabela A completa
df_a_base AS (
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
  where 1=1
  AND LENGTH(name) >= 4
  and reference_date = ref_month
),

-- Inicial: tabela M completa
df_m_base AS (
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
   where 1=1
   AND LENGTH(name) >= 4
   and reference_date = ref_month
),

------------------------------------------------------------------------------------
-- 1. Primeiro nível de merge (reference_date, postal_code_7d, de42) - exact match
------------------------------------------------------------------------------------
merge_level_1 AS (
  SELECT *,
 'reference_date, cod_muni, postal_code_7d, de42, name' AS tipo_merge
  FROM df_a_base a
  INNER JOIN df_m_base m using(reference_date, cod_muni, de42, postal_code_7d, name)
),

-- IDs matchados neste nível (para exclusão em próximos merges)
df_a_remaining_1 AS (
  SELECT * FROM df_a_base a
  LEFT JOIN (SELECT DISTINCT document, reference_date, FROM merge_level_1 a) e using(document, reference_date)
  WHERE e.document IS NULL
),

df_m_remaining_1 AS (
  SELECT * FROM df_m_base m
  LEFT JOIN (SELECT DISTINCT merchant_market_hierarchy_id, reference_date, FROM merge_level_1) e using(merchant_market_hierarchy_id, reference_date)
  WHERE e.merchant_market_hierarchy_id IS NULL
),

------------------------------------------------------------------------------------
-- 2. Segundo nível de merge (reference_date, postal_code_7d, de42) - fuzz match partial ratio
------------------------------------------------------------------------------------
merge_level_2 AS (
  SELECT *,
 'reference_date, cod_muni, postal_code_7d, de42, fuzzy name' AS tipo_merge,
  FROM df_a_remaining_1 a
  INNER JOIN df_m_remaining_1 m
  USING(reference_date, cod_muni, postal_code_7d, de42)
  WHERE 1=0
  or (length(a.name) >= length(m.name) and EDIT_DISTANCE(LEFT(a.name, length(m.name)), m.name, max_distance => 7) / length(m.name) < 0.2)
  OR (length(m.name) >= length(a.name) and EDIT_DISTANCE(LEFT(m.name, length(a.name)), a.name,  max_distance => 7) / length(a.name) < 0.2)
),


df_a_remaining_2 AS (
  SELECT * FROM df_a_remaining_1 a
  LEFT JOIN (SELECT DISTINCT document, reference_date, FROM merge_level_2 a) e using(document, reference_date)
  WHERE e.document IS NULL
),

df_m_remaining_2 AS (
  SELECT * FROM df_m_remaining_1 m
  LEFT JOIN (SELECT DISTINCT merchant_market_hierarchy_id, reference_date, FROM merge_level_2) e using(merchant_market_hierarchy_id, reference_date)
  WHERE e.merchant_market_hierarchy_id IS NULL
),


------------------------------------------------------------------------------------
-- 3. Terceiro nível de merge (reference_date, postal_code_7d) - exact match
------------------------------------------------------------------------------------

merge_level_3 AS (
  SELECT *,
  'reference_date, cod_muni, postal_code_7d, name' AS tipo_merge
  from df_a_remaining_2 a
  INNER JOIN df_m_remaining_2 m using(reference_date, cod_muni, postal_code_7d, name)
),

df_a_remaining_3 AS (
  SELECT * FROM df_a_remaining_2 a
  LEFT JOIN (SELECT DISTINCT document, reference_date, FROM merge_level_3 a) e using(document, reference_date)
  WHERE e.document IS NULL
),

df_m_remaining_3 AS (
  SELECT * FROM df_m_remaining_2 m
  LEFT JOIN (SELECT DISTINCT merchant_market_hierarchy_id, reference_date, FROM merge_level_3) e using(merchant_market_hierarchy_id, reference_date)
  WHERE e.merchant_market_hierarchy_id IS NULL
),

------------------------------------------------------------------------------------
-- 4. Quarto nível de merge (reference_date, postal_code_7d) - fuzz match partial ratio
------------------------------------------------------------------------------------

merge_level_4 AS (
  SELECT *,
 'reference_date, cod_muni, postal_code_7d, fuzzy name' AS tipo_merge,
  FROM df_a_remaining_3 a
  INNER JOIN df_m_remaining_3 m
  USING(reference_date, cod_muni, postal_code_7d)
  WHERE 1=0
  or (length(a.name) >= length(m.name) and EDIT_DISTANCE(LEFT(a.name, length(m.name)), m.name, max_distance => 7) / length(m.name) < 0.2)
  OR (length(m.name) >= length(a.name) and EDIT_DISTANCE(LEFT(m.name, length(a.name)), a.name,  max_distance => 7) / length(a.name) < 0.2)
),

df_a_remaining_4 AS (
  SELECT * FROM df_a_remaining_3 a
  LEFT JOIN (SELECT DISTINCT document, reference_date, FROM merge_level_4 a) e using(document, reference_date)
  WHERE e.document IS NULL
),

df_m_remaining_4 AS (
  SELECT * FROM df_m_remaining_3 m
  LEFT JOIN (SELECT DISTINCT merchant_market_hierarchy_id, reference_date, FROM merge_level_4) e using(merchant_market_hierarchy_id, reference_date)
  WHERE e.merchant_market_hierarchy_id IS NULL 
),

------------------------------------------------------------------------------------
-- 5. Quinto nível de merge (reference_date, postal_code_6d)
------------------------------------------------------------------------------------

merge_level_5 AS (
  SELECT *,
  'reference_date, cod_muni, postal_code_6d, name' AS tipo_merge
  from df_a_remaining_4 a
  INNER JOIN df_m_remaining_4 m using(reference_date, cod_muni, postal_code_6d, name)
),

df_a_remaining_5 AS (
  SELECT * FROM df_a_remaining_4 a
  LEFT JOIN (SELECT DISTINCT document, reference_date, FROM merge_level_5 a) e using(document, reference_date)
  WHERE e.document IS NULL
),

df_m_remaining_5 AS (
  SELECT * FROM df_m_remaining_4 m
  LEFT JOIN (SELECT DISTINCT merchant_market_hierarchy_id, reference_date, FROM merge_level_5) e using(merchant_market_hierarchy_id, reference_date)
  WHERE e.merchant_market_hierarchy_id IS NULL
),


------------------------------------------------------------------------------------
-- 6. Sexto nível de merge (reference_date, postal_code_6d) fuzzy match partial ratio
------------------------------------------------------------------------------------

merge_level_6 AS (
  SELECT *,
 'reference_date, cod_muni, postal_code_6d, fuzzy name' AS tipo_merge,
  FROM df_a_remaining_5 a
  INNER JOIN df_m_remaining_5 m
  USING(reference_date, cod_muni, postal_code_6d)
  WHERE 1=0
  or (length(a.name) >= length(m.name) and EDIT_DISTANCE(LEFT(a.name, length(m.name)), m.name, max_distance => 7) / length(m.name) < 0.2)
  OR (length(m.name) >= length(a.name) and EDIT_DISTANCE(LEFT(m.name, length(a.name)), a.name,  max_distance => 7) / length(a.name) < 0.2)
),
df_a_remaining_6 AS (
  SELECT * FROM df_a_remaining_5 a
  LEFT JOIN (SELECT DISTINCT document, reference_date, FROM merge_level_6 a) e using(document, reference_date)
  WHERE e.document IS NULL
),
df_m_remaining_6 AS (
  SELECT * FROM df_m_remaining_5 m
  LEFT JOIN (SELECT DISTINCT merchant_market_hierarchy_id, reference_date, FROM merge_level_6) e using(merchant_market_hierarchy_id, reference_date)
  WHERE e.merchant_market_hierarchy_id IS NULL  
)/*,


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

*/

select merchant_market_hierarchy_id, document, reference_date, cod_muni, tipo_merge, from merge_level_1

union all select merchant_market_hierarchy_id, document, reference_date, cod_muni, tipo_merge, from merge_level_2

union all select merchant_market_hierarchy_id, document, reference_date, cod_muni, tipo_merge, from merge_level_3

union all select merchant_market_hierarchy_id, document, reference_date, cod_muni, tipo_merge, from merge_level_4

union all select merchant_market_hierarchy_id, document, reference_date, cod_muni, tipo_merge, from merge_level_5

union all select merchant_market_hierarchy_id, document, reference_date, cod_muni, tipo_merge, from merge_level_6

--union all select merchant_market_hierarchy_id, document, reference_date, cod_muni, tipo_merge, from merge_level_7

--union all select merchant_market_hierarchy_id, document, reference_date, cod_muni, tipo_merge, from merge_level_8

--union all select merchant_market_hierarchy_id, document, reference_date, cod_muni, tipo_merge, from merge_level_9

--union all select merchant_market_hierarchy_id, document, reference_date, cod_muni, tipo_merge, from merge_level_10


