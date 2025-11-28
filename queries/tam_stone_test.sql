DECLARE ref_month  DATE DEFAULT '2025-06-01';


CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.temp_ruan_tam_stone` PARTITION BY reference_month AS



with names_city_master_stone as (
select 
distinct 
reference_month, 
merchant_market_hierarchy_id, 
de42_merchant_id,
subs_asterisk,
cod_muni,
from `dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city` a
where 1=1
and subs_asterisk in ('Outros', 'Stone') 
and RIGHT(de42_merchant_id, 6) = '000000'
and reference_month >= ref_month
)

, active_base_stone as (
  SELECT distinct  
  affiliation_id,
  reference_month,
  FROM `dataplatform-prd.master_contact.v2_temp_cross_company` AS ft
  CROSS JOIN (select distinct reference_month from names_city_master_stone)
  WHERE 1=1
    AND ft.company = 'STONE'
    --AND LAST_DAY(ft.reference_date, MONTH) in (select distinct reference_month from names_city_master)
    and ft.reference_date between DATE_ADD(LAST_DAY(reference_month, MONTH), interval -29 day) and reference_month
    --and ft.reference_date between DATE_ADD(LAST_DAY(ref_month, MONTH), interval -29 day) and ref_month
)

, clientes_stone as (
  select distinct
        reference_month,
        dcl.document,
        RPAD(affiliation_id, 15, '0') AS de42_merchant_id,
        TRIM(sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(REGEXP_REPLACE(TRIM(UPPER(trade_name)),  r'[0-9.,\-+]', ''))) nome,
  from active_base_stone
  inner join `dataplatform-treated-prod.cross_company.payments__client_affiliation` dcl using(affiliation_id)
  where dcl.company = 'STONE'
)



, final as (
select distinct 
reference_month,
merchant_market_hierarchy_id mmhid_orig,
COALESCE(IF(big_mmhid.merchant_market_hierarchy_id is null, merchant_market_hierarchy_id, 99), 99) new_mmhid_merge,

coalesce(CAST(IF(big_mmhid.merchant_market_hierarchy_id is null, merchant_market_hierarchy_id, null) AS STRING), '') id_merge_places,
REGEXP_REPLACE(nome, r'[^A-Z]', '') nome_master,
coalesce(cod_muni, 9999) cod_muni,
de42_merchant_id stone_code, 
document,
coalesce(document, '')  doc_unmerge,
REPLACE(nome, ' ', '') nome_merge,
from clientes_stone a
left join names_city_master_stone using(de42_merchant_id, reference_month)
left join `dataplatform-prd.master_contact.v2_aux_tam_big_mmhid` big_mmhid using(merchant_market_hierarchy_id, reference_month)
)


, aux_python_agrupados as (
select 
distinct
reference_month,
cod_muni, 
tam_id, 
nome_master,
coalesce(mmhid_merge, 99) new_mmhid_merge,
coalesce(pre_doc_unmerge, '') doc_unmerge,
coalesce(id_merge_places, '') id_merge_places,
from `dataplatform-prd.master_contact.v3_aux_tam_python_agrupados`
where reference_month >= ref_month
and ingestion_date >= '2025-09-29'
)


select * 
from final
left join aux_python_agrupados using(
reference_month,
new_mmhid_merge,
nome_master, 
cod_muni,
id_merge_places,
doc_unmerge
)
where reference_month >= ref_month