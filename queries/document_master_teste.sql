
DECLARE ref_month  DATE DEFAULT '2025-06-01';


CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.tmp_ruan_test_stone_tam` 
PARTITION BY reference_month 

OPTIONS(
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
)


AS

with names_city_master_stone as (
select 
distinct 
reference_month, 
merchant_market_hierarchy_id, 
de42_merchant_id,
subs_asterisk,
cod_muni,
city_limpo,
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

, stonecode_municipio as (
select distinct
de42 de42_merchant_id,
cod_muni,
reference_month,
REGEXP_REPLACE(TRIM(SPLIT(sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(UPPER(City)), '  ')[0]),  r'[0-9.,\-+*]', '') city_limpo, -- # limpa cidade
from `dataplatform-prd.master_contact.v2_temp_ruan_autorizador` auth
left join `dataplatform-prd.economic_research.geo_cep` geo on auth.PostalCode = geo.cep
where 1=1
and reference_month >= ref_month
)

, final as (

select distinct
reference_month,
merchant_market_hierarchy_id,
a.nome nome_master,
REPLACE(nome, ' ', '') nome_merge,
document, 
s.cod_muni = b.cod_muni muni_match,
STARTS_WITH(b.city_limpo, s.city_limpo) or STARTS_WITH(s.city_limpo, b.city_limpo) city_match,
b.cod_muni cod_muni_master,
s.cod_muni cod_muni_auth,
s.city_limpo city_auth,
b.city_limpo city_master,
de42_merchant_id,
b.de42_merchant_id is not null match_de42_master,
s.de42_merchant_id is not null match_de42_auth,
from clientes_stone a
LEFT join stonecode_municipio s using(de42_merchant_id, reference_month) 
left join names_city_master_stone b using(reference_month, de42_merchant_id)
)

select * 
from final
where reference_month >= ref_month

