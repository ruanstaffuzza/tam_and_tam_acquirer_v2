DECLARE ref_month  DATE DEFAULT '2025-06-01';

CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.tam_acquirer_completa_de42` PARTITION BY reference_date AS 


--INSERT INTO `dataplatform-prd.master_contact.v2_aux_tam_acquirers` 


with dados_orig as (
select distinct
DATE(reference_month) reference_month,
merchant_market_hierarchy_id,
subs_asterisk,
COALESCE(if(length(numero_inicio) in (8, 14), numero_inicio, null), '') numero_inicio,
REGEXP_REPLACE(TRIM(REGEXP_REPLACE(nome_limpo, r'^SHOPEE|IFOOD', '')), r'[^A-Z]', '') nome_master,
cod_muni,
RPAD(TRIM(de42_merchant_id), 15, ' ') AS de42,
original_name, 
CASE WHEN REGEXP_CONTAINS(original_name, r'\p{Ll}') THEN TRUE ELSE FALSE END AS lower_name,
from `dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city`
where 1=1
--and subs_asterisk='Outros'
--and subs_asterisk<> 'delete_asterisk'
and not (subs_asterisk='Outros' and RIGHT(de42_merchant_id, 6) = '000000') # Stone
and reference_month >= ref_month
)

, acquirer as (
select distinct 
DATE(reference_month) reference_month,
merchant_market_hierarchy_id,
de42,
lower_name, 
adquirente_padroes,
from `dataplatform-prd.master_contact.v2_aux_tam_pre_acquirer`
where 1=1
and reference_month >= ref_month
)


, final_acquirer1 as (
select * except(merchant_market_hierarchy_id, adquirente_padroes),
IF(big_mmhid.merchant_market_hierarchy_id is null , CAST(merchant_market_hierarchy_id as STRING), '') merchant_market_hierarchy_id,
coalesce(adquirente_padroes, subs_asterisk) adquirente_padroes
from dados_orig
left join acquirer using(reference_month, merchant_market_hierarchy_id, de42, lower_name)
left join `dataplatform-prd.master_contact.v2_aux_tam_big_mmhid` big_mmhid using(merchant_market_hierarchy_id, reference_month)
where reference_month >= ref_month
)


, issue_get_table as (
select distinct
reference_month,
de42,
count(distinct concat(nome_master, cod_muni)) over(partition by reference_month, de42) issue_get
from final_acquirer1
where adquirente_padroes = 'GetNet'
and REGEXP_CONTAINS(TRIM(de42), r'^00000001[0-8]')
)

, final_acquirer as (
select *
from final_acquirer1
left join issue_get_table using(reference_month, de42)
)










select distinct
tam_id,
DATE(reference_month) reference_date, 
adquirente_padroes as adquirente,
False modelo_previsao,
issue_get,
de42,
mmhid_original,
from (select * 
except(numero_inicio, merchant_market_hierarchy_id, reference_date), 
COALESCE(numero_inicio, '') numero_inicio, 
COALESCE(CAST(merchant_market_hierarchy_id as STRING), '') merchant_market_hierarchy_id,
reference_date reference_month,
from `dataplatform-prd.master_contact.v3_aux_tam_pos_python`) b 
left join final_acquirer c using(reference_month, merchant_market_hierarchy_id, subs_asterisk, numero_inicio, nome_master, cod_muni) 
--inner join (select distinct tam_id, reference_date reference_month from `dataplatform-prd.master_contact.v3_aux_tam_pos_python`) using(tam_id, reference_month)
where 1=1
--and subs_asterisk = 'Outros'
and adquirente_padroes <> 'unknown'
and reference_month >= ref_month

union distinct

SELECT distinct
tam_id,
DATE(reference_date) reference_date,
acquirer adquirente,
TRue modelo_previsao,
null issue_get,
de42,
mmhid_original, 
from `dataplatform-prd.master_contact.tam_acquirer_pre_previsao` a
left join `dataplatform-prd.master_contact.de42_acquirer_model` b using(de42)
where acquirer<>'Cielo'
and date(reference_date) >= ref_month
