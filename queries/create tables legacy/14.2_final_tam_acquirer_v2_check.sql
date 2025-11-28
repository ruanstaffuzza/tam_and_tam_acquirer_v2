DECLARE ref_month  DATE DEFAULT '2025-06-01';


CREATE OR REPLACE TABLE `dataplatform-prd.economic_research.v2_tam_acquirer_completa_de42` PARTITION BY reference_date AS
--INSERT INTO `dataplatform-prd.master_contact.aux_tam_final_tam_acquirer_completa`



with dados_orig as (
select distinct
DATE(reference_month) reference_month,
IF(subs_asterisk in ('Outros', 'Outros_Pags', 'Outros_SumUp', 'Outros_Stone'), merchant_market_hierarchy_id, null) merchant_market_hierarchy_id,
subs_asterisk,
COALESCE(if(length(numero_inicio) in (8, 14), numero_inicio, null), '') numero_inicio,
REGEXP_REPLACE(TRIM(REGEXP_REPLACE(nome_limpo, r'^SHOPEE|IFOOD', '')), r'[^A-Z]', '') nome_master,
cod_muni,
RPAD(TRIM(de42_merchant_id), 15, ' ') AS de42,
original_name, 
from `dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city`
where 1=1
and subs_asterisk='Outros'
and not (subs_asterisk='Outros' and RIGHT(de42_merchant_id, 6) = '000000') # Stone
and date(reference_month) >= ref_month

)

, acquirer as (
select distinct 
DATE(reference_month) reference_month,
merchant_market_hierarchy_id,
de42,
adquirente_padroes,
from `dataplatform-prd.master_contact.v2_aux_tam_pre_acquirer`
where 1=1
and date(reference_month) >= ref_month

)


, final_acquirer as (
select * except(merchant_market_hierarchy_id),
IF(big_mmhid.merchant_market_hierarchy_id is null , CAST(merchant_market_hierarchy_id as STRING), '') merchant_market_hierarchy_id,
from dados_orig
left join acquirer using(reference_month, merchant_market_hierarchy_id, de42)
left join `dataplatform-prd.master_contact.v2_aux_tam_big_mmhid` big_mmhid using(merchant_market_hierarchy_id, reference_month)
)



, get_de42_model as (
select distinct
tam_id,
DATE(reference_month) reference_date, 
de42,
from (select * except(numero_inicio, merchant_market_hierarchy_id, reference_date), COALESCE(numero_inicio, '') numero_inicio, 
COALESCE(CAST(merchant_market_hierarchy_id as STRING), '') merchant_market_hierarchy_id,
reference_date reference_month,
from  `dataplatform-prd.master_contact.v3_aux_tam_pos_python`) b 
left join final_acquirer c using(reference_month, merchant_market_hierarchy_id, subs_asterisk, numero_inicio, nome_master, cod_muni) 
where subs_asterisk = 'Outros'
and adquirente_padroes='unknown'
)


, aux_tam_acquirers as (
    select 
    tam_id, reference_date,
    de42,
    adquirente, modelo_previsao, issue_get
    from  `dataplatform-prd.master_contact.v2_aux_tam_acquirers`
    where not modelo_previsao

    union all

    select distinct 
    tam_id, reference_date, 
    de42,
    prediction as adquirente, True as modelo_previsao, null as issue_get
    from get_de42_model
    left join `dataplatform-prd.master_contact.v2_tmp_mid_de42_acquirer_model` using(de42)
    where rank=1
)



, aux as (
select distinct * except(adquirente, modelo_previsao),
case when adquirente = 'CloudWalk' then 'InfinitePay'
     when issue_get>100 then null
     else adquirente end acquirer,
from aux_tam_acquirers
--where reference_date = ref_month
)

select distinct 
reference_date, tam_id, 
--de42
acquirer, 
from `dataplatform-prd.master_contact.final_tam`
left join aux using(tam_id, reference_date)
where 1=1 
  --and acquirer not in ('possivel_erro', 'Outras', 'Desconhecido')
  --and acquirer is not null
  --and reference_date = ref_month
