DECLARE ref_month  DATE DEFAULT '{{ ref_month }}';


--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.v2_aux_tam_ton_mastercard` AS
INSERT INTO `dataplatform-prd.master_contact.v2_aux_tam_ton_mastercard`


with stone_included as (
select distinct
reference_month,
merchant_market_hierarchy_id,
FROM `dataplatform-prd.master_contact.v2_aux_tam_get_document_master`
where subs_asterisk = 'Outros_Stone'
and reference_month = ref_month
)

, big_mmhid as (
  select * from `dataplatform-prd.master_contact.v2_aux_tam_big_mmhid`
  where 1=1
  and reference_month = ref_month
)


, names_city_master_ton as (
select 
distinct 
reference_month, 
merchant_market_hierarchy_id, 
de42_merchant_id,
subs_asterisk,
cod_muni,
postal_code_cleansed, 
nome_limpo,
from `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places` p 
left join `dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city` sa using(reference_month, merchant_market_hierarchy_id)
left join stone_included using(reference_month, merchant_market_hierarchy_id)
left join big_mmhid using(reference_month, merchant_market_hierarchy_id)
where 1=1
and subs_asterisk in ('Outros', 'Ton', 'Pagarme', 'Stone') 
and stonecode_flag
and reference_month = ref_month
and stone_included.merchant_market_hierarchy_id is null
and big_mmhid.merchant_market_hierarchy_id is null
)



, aux_final2 as (
select
* except(reference_month, postal_code_cleansed, nome_limpo),
reference_month as reference_date,
REPLACE(postal_code_cleansed, '-', '') as postal_code1,
nome_limpo as name,
de42_merchant_id as de42,
from names_city_master_ton
where 1=1
and de42_merchant_id is not null
)


-- Função CTE genérica: common_treat
, common_treat AS (
  SELECT 
    --document,
    REGEXP_REPLACE(name, r'^(PG \*TON |TON |PG \*)', '') AS name,

    de42,
    reference_date,
    CAST(cod_muni AS STRING) AS cod_muni,
    merchant_market_hierarchy_id,


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
