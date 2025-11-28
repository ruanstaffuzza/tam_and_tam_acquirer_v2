--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.v2_aux_tam_big_mmhid` AS


DECLARE ref_month  DATE DEFAULT '{{ref_month}}'; INSERT INTO `dataplatform-prd.master_contact.v2_aux_tam_big_mmhid`



with aux1 as (
select *
from (
select 
distinct
reference_month,
merchant_market_hierarchy_id,
nome_limpo nome_master_com_espaco,
REGEXP_REPLACE(nome_limpo, r'[^A-Z]', '') nome_master,
cod_muni,
from  `dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city` 

)
where 1=1
and not starts_with(nome_master, 'PICPAY')
and not starts_with(nome_master, 'GOOGLE')
and not starts_with(nome_master_com_espaco, 'ZUL ')
and not starts_with(nome_master, 'GOLLIN')
and not starts_with(nome_master, 'DEEZER')
and not starts_with(nome_master, 'RECARGA')
and not starts_with(nome_master, 'OLXBRA')
and not starts_with(nome_master, 'GRUPOOLX')
and not starts_with(nome_master, 'AVIANCA')
and not starts_with(nome_master, 'ETSAMERICAN')
and not starts_with(nome_master, 'EMIRATES')
and not starts_with(nome_master, 'AZULLINHA')
and reference_month = ref_month
)



select merchant_market_hierarchy_id , reference_month from (
select merchant_market_hierarchy_id, reference_month,
count(*) as qtd
from (select distinct reference_month, merchant_market_hierarchy_id, nome_master, cod_muni from aux1)
group by 1, 2
)
where qtd>10
