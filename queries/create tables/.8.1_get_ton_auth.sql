DECLARE ref_month  DATE DEFAULT '{{ ref_month }}';

--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.v2_aux_tam_ton_auth` AS

INSERT INTO `dataplatform-prd.master_contact.v2_aux_tam_ton_auth`

with city AS (
select distinct
documento document, 
CAST(cod_muni_espacial AS INT) cod_muni,
from `dataplatform-prd.economic_research.geo_stoneco`
)


, active_base as (
  SELECT distinct 
  document,
  --LAST_DAY(ft.reference_date, MONTH) reference_month,
  ref_month as reference_month,
  company, 
  FROM `dataplatform-prd.master_contact.v2_temp_cross_company` AS ft
  WHERE 1=1
    AND ft.company in ('TON', 'STONE', 'PAGARME')
    and ft.reference_date between DATE_ADD(LAST_DAY(ref_month, MONTH), interval -29 day) and ref_month
    --AND LAST_DAY(ft.reference_date, MONTH) in (select distinct reference_month from final_sem_ton)
    --and LAST_DAY(ft.reference_date, MONTH) = ref_month
)


, stone_included as (
select distinct
reference_month,
document,
from active_base
where company = 'STONE'
)


, active_base_ton as (
select distinct
reference_month,
document,
from active_base
where company in ('TON', 'PAGARME')
)


, auth as (
select 
* except(tax_id, reference_month, parc), 
tax_id as document,
ab.reference_month,
from `dataplatform-prd.master_contact.v2_temp_ruan_autorizador` au
  CROSS JOIN (SELECT DISTINCT reference_month FROM active_base) ab
  WHERE 1=1
    AND au.reference_month BETWEEN DATE_ADD(ab.reference_month, INTERVAL -39 DAY) 
                               AND ab.reference_month
and parc is null
)

, aux_final as (
select distinct
document,
UPPER(TRIM(sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(REGEXP_REPLACE(TRIM(UPPER(name)),  r'[0-9.,\-+]', '')))) name,
postalcode,
city,
smid, 
de42,
TRIM(sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(REGEXP_REPLACE(TRIM(UPPER(de43)),  r'[0-9.,\-+]', ''))) de43,
reference_month reference_date,
cod_muni,
from auth a
left join city c using (document)
left join stone_included using(reference_month, document)
inner join active_base_ton using(document, reference_month)
where 1=1
and stone_included.document is null -- remove stone
)



, aux_final2 as (
select 
* except(postalcode),
REPLACE(postalcode, '-', '') as postal_code1,
--df['inicio'] = df['name'].str[:4]
left(name, 4) as inicio,
from aux_final
where 1=1
and name is not null
)


-- Função CTE genérica: common_treat
, common_treat AS (
  SELECT 
    document,
    REGEXP_REPLACE(name, r'^(PG \*TON |TON |PG \*)', '') AS name,

    de42,
    reference_date,
    CAST(cod_muni AS STRING) AS cod_muni,
    --merchant_market_hierarchy_id,

    -- to_postal_code_7d substituído por REGEXP_REPLACE como placeholder
    LEFT(postal_code1, 7) AS postal_code_7d,
    LEFT(postal_code1, 6) AS postal_code_6d,
    LEFT(postal_code1, 5) AS postal_code_5d,
    LEFT(postal_code1, 4) AS postal_code_4d,

  FROM 
    aux_final2
  WHERE
    name IS NOT NULL
    --AND reference_date > DATE '2024-10-31'
)



SELECT * FROM common_treat 
ORDER BY reference_date DESC
