DECLARE ref_month  DATE DEFAULT '{{ref_month}}';

--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.v2_aux_tam_acquirers` PARTITION BY reference_date AS 


INSERT INTO `dataplatform-prd.master_contact.v2_aux_tam_acquirers` 


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
and subs_asterisk='Outros'
and not (subs_asterisk='Outros' and stonecode_flag) # Stone
and reference_month = ref_month
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
and reference_month = ref_month
)


, final_acquirer1 as (
select * except(merchant_market_hierarchy_id),
IF(big_mmhid.merchant_market_hierarchy_id is null , CAST(merchant_market_hierarchy_id as STRING), '') merchant_market_hierarchy_id,
from dados_orig
left join acquirer using(reference_month, merchant_market_hierarchy_id, de42, lower_name)
left join `dataplatform-prd.master_contact.v2_aux_tam_big_mmhid` big_mmhid using(merchant_market_hierarchy_id, reference_month)
where reference_month = ref_month
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
DATE(reference_date) reference_date,
CASE WHEN subs_asterisk = 'Outros_Stone' then "Stone"
     WHEN subs_asterisk = 'Outros_Pags' then 'PagSeguro' 
     WHEN subs_asterisk = 'Outros_SumUp' then 'SumUp' 
     WHEN subs_asterisk = 'MercadoPago_subPagarme' then 'MercadoPago'
     else subs_asterisk end as adquirente,
False modelo_previsao,
null issue_get,
from `dataplatform-prd.master_contact.v3_aux_tam_pos_python`
where 1=1
and subs_asterisk <> 'Outros'
{{other_filter}}
and reference_date = ref_month

union distinct 

select distinct
tam_id,
DATE(reference_month) reference_date, 
adquirente_padroes as adquirente,
False modelo_previsao,
issue_get,
from (select * 
except(numero_inicio, merchant_market_hierarchy_id, reference_date), 
COALESCE(numero_inicio, '') numero_inicio, 
COALESCE(CAST(merchant_market_hierarchy_id as STRING), '') merchant_market_hierarchy_id,
reference_date reference_month,
from `dataplatform-prd.master_contact.v3_aux_tam_pos_python`) b 
left join final_acquirer c using(reference_month, merchant_market_hierarchy_id, subs_asterisk, numero_inicio, nome_master, cod_muni) 
--inner join (select distinct tam_id, reference_date reference_month from `dataplatform-prd.master_contact.v3_aux_tam_pos_python`) using(tam_id, reference_month)
where subs_asterisk = 'Outros'
and adquirente_padroes <> 'unknown'
and reference_month = ref_month

union distinct

SELECT distinct
tam_id,
DATE(reference_date) reference_date,
acquirer adquirente,
TRue modelo_previsao,
null issue_get,
from `dataplatform-prd.master_contact.v2_tmp_mid_de42_acquirer_model`
where acquirer<>'Cielo'
and date(reference_date) = ref_month
/*
SELECT 
adquirente, 
AVG(CAST(also_online as INT)*100) share_also_online,
AVG(CAST(only_online as INT)*100) share_only_online,
count(distinct tam_id) qtd
FROM `dataplatform-prd.master_contact.v2_aux_tam_tam_acquirers` 
LEFT JOIN `dataplatform-prd.master_contact.temp_tam_presenca_online` using(tam_id, reference_date)
WHERE reference_date = "2024-04-30" 
and adquirente is not null
group by 1
order by qtd desc
*/