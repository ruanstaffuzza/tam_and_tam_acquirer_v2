
DECLARE ref_month  DATE DEFAULT '{{ref_month}}';



with dados_orig as (
select distinct
DATE(reference_month) reference_month,
IF(subs_asterisk in ('Outros', 'Outros_Pags', 'Outros_SumUp', 'Outros_Stone'), merchant_market_hierarchy_id, null) merchant_market_hierarchy_id,
subs_asterisk,
COALESCE(if(length(numero_inicio) in (8, 14), numero_inicio, null), '') numero_inicio,
REGEXP_REPLACE(REPLACE(nome_limpo,'IFOOD', ''), r'[^A-Z]', '') nome_master,
cod_muni,
RPAD(TRIM(de42_merchant_id), 15, ' ') AS de42,
original_name, 
CASE WHEN REGEXP_CONTAINS(original_name, r'\p{Ll}') THEN TRUE ELSE FALSE END AS lower_name,
--CASE WHEN REGEXP_CONTAINS(original_city, r'\p{Ll}') THEN TRUE ELSE FALSE END AS lower_city, -- Não tem essa coluna
from `dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city`
where 1=1
and subs_asterisk='Outros'
and not (subs_asterisk='Outros' and RIGHT(de42_merchant_id, 6) = '000000') # Stone
and date(reference_month) = ref_month
and date(reference_month) >= '2023-12-01'
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
and date(reference_month) = ref_month
and date(reference_month) >= '2023-12-01'
)


, final_acquirer as (
select * except(merchant_market_hierarchy_id),
IF(big_mmhid.merchant_market_hierarchy_id is null , CAST(merchant_market_hierarchy_id as STRING), '') merchant_market_hierarchy_id,
from dados_orig
left join acquirer using(reference_month, merchant_market_hierarchy_id, de42, lower_name)
left join `dataplatform-prd.master_contact.v2_aux_tam_big_mmhid` big_mmhid using(merchant_market_hierarchy_id, reference_month)

)



select distinct
tam_id,
DATE(reference_month) reference_date, 
de42,
--lower_city,
lower_name, 
from (select * except(numero_inicio, merchant_market_hierarchy_id, reference_date), COALESCE(numero_inicio, '') numero_inicio, 
COALESCE(CAST(merchant_market_hierarchy_id as STRING), '') merchant_market_hierarchy_id,
reference_date reference_month,
from  `dataplatform-prd.master_contact.v3_aux_tam_pos_python`) b 
left join final_acquirer c using(reference_month, merchant_market_hierarchy_id, subs_asterisk, numero_inicio, nome_master, cod_muni) 
--inner join (select distinct tam_id from `dataplatform-prd.master_contact.v3_aux_tam_pos_python`) using(tam_id)
where subs_asterisk = 'Outros'
and adquirente_padroes='unknown'


