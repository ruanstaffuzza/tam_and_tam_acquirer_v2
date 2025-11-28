DECLARE ref_month  DATE DEFAULT '2025-08-31';


with dados_orig as (
select distinct
DATE(reference_month) reference_month,
merchant_market_hierarchy_id,
subs_asterisk,
COALESCE(if(length(numero_inicio) in (8, 14), numero_inicio, null), '') numero_inicio,
REGEXP_REPLACE(TRIM(REGEXP_REPLACE(nome_limpo, r'^SHOPEE|IFOOD', '')), r'[^A-Z]', '') nome_master,
COALESCE(cod_muni, 9999) cod_muni,
RPAD(TRIM(de42_merchant_id), 15, ' ') AS de42,
original_name, 
CASE WHEN REGEXP_CONTAINS(original_name, r'\p{Ll}') THEN TRUE ELSE FALSE END AS lower_name,
from `dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city`
where 1=1
and subs_asterisk='Outros'
and not (subs_asterisk='Outros' and RIGHT(de42_merchant_id, 6) = '000000') # Stone
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
IF(big_mmhid.merchant_market_hierarchy_id is null , CAST(merchant_market_hierarchy_id as STRING), '') merchant_market_hierarchy_id2,
from dados_orig
left join acquirer using(reference_month, merchant_market_hierarchy_id, de42, lower_name)
left join `dataplatform-prd.master_contact.v2_aux_tam_big_mmhid` big_mmhid using(merchant_market_hierarchy_id, reference_month)
where reference_month = ref_month
)



, final_acquirer as (
select *
from (select * 
except(numero_inicio), 
COALESCE(numero_inicio, '') numero_inicio, 
COALESCE(CAST(merchant_market_hierarchy_id as STRING), '') merchant_market_hierarchy_id2,
reference_date reference_month,
from `dataplatform-prd.master_contact.v3_aux_tam_pos_python`) b 
left join final_acquirer1 c using(reference_month, merchant_market_hierarchy_id2, subs_asterisk, numero_inicio, nome_master, cod_muni) 
where 1=1
--and subs_asterisk = 'Outros'
--and adquirente_padroes <> 'unknown'
and reference_month = ref_month
)


select *
from final_acquirer
where 1=1

and merchant_market_hierarchy_id in (

    select distinct merchant_market_hierarchy_id,

    from `dataplatform-prd.master_contact.v3_aux_tam_pos_python` 
    where reference_date = ref_month
    qualify count(distinct tam_id) over(partition by reference_date, merchant_market_hierarchy_id) > 1
    order by rand() limit 1000)

AND merchant_market_hierarchy_id in (
    select distinct 
    merchant_market_hierarchy_id,
    from final_acquirer
    where 1=1
    and adquirente_padroes in ('Cielo', 'GetNet')
)

order by tam_id, merchant_market_hierarchy_id