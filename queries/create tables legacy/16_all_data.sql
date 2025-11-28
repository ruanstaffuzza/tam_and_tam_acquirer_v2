CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.aux_tam_descriptors_total` PARTITION BY reference_date AS 

--DECLARE ref_month  DATE DEFAULT '{{ref_month}}';
--INSERT INTO `dataplatform-prd.master_contact.aux_tam_descriptors_total` 

with dados_orig as (
select distinct
DATE(reference_month) reference_date,
IF(big_mmhid.merchant_market_hierarchy_id is null or subs_asterisk='CloudWalk' , CAST(merchant_market_hierarchy_id as STRING), '') merchant_market_hierarchy_id,
subs_asterisk,
COALESCE(if(length(numero_inicio) in (8, 14), numero_inicio, null), '') numero_inicio,
REGEXP_REPLACE(TRIM(REGEXP_REPLACE(nome_limpo, r'^SHOPEE|IFOOD', '')), r'[^A-Z]', '') nome_master,
cod_muni,
RPAD(TRIM(de42_merchant_id), 15, ' ') AS de42,
original_name, 
from  `dataplatform-prd.master_contact.aux_tam_subs_asterisk_city_v2`
left join `dataplatform-prd.master_contact.aux_tam_big_mmhid` big_mmhid using(merchant_market_hierarchy_id, reference_month)
where 1=1
--and subs_asterisk='Outros'
and not (subs_asterisk='Outros' and RIGHT(de42_merchant_id, 6) = '000000') # Stone
--and reference_month = ref_month
)


, tam_id_info as (
select * 
except(numero_inicio, merchant_market_hierarchy_id, reference_date, nome_master), 
COALESCE(numero_inicio, '') numero_inicio, 
COALESCE(CAST(merchant_market_hierarchy_id as STRING), '') merchant_market_hierarchy_id,
reference_date,
REGEXP_REPLACE(nome_master, r'[^A-Z]', '') nome_master,
from `dataplatform-prd.master_contact.aux_tam_pos_python`
where 1=1
--and reference_month = ref_month
)  




select 
tam_id,
reference_date,
IF(merchant_market_hierarchy_id='', null, merchant_market_hierarchy_id) merchant_market_hierarchy_id,
subs_asterisk,
numero_inicio,
nome_master,
cod_muni,
de42,
original_name,
concat(nome_master, IF(subs_asterisk in ('Outros_Stone', 'Ton'), coalesce(cnpj, cpf), coalesce(numero_inicio, ''))) nome_master_mod,
from tam_id_info a
left join dados_orig b using(reference_date, merchant_market_hierarchy_id, subs_asterisk, numero_inicio, nome_master, cod_muni) 
inner join (select distinct tam_id, reference_date from `dataplatform-prd.master_contact.aux_tam_final_tam`) tam_final using(reference_date, tam_id)
where 1=1