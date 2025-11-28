

with aux_aug as (
select 
distinct 
merchant_market_hierarchy_id, 
de42_merchant_id,
from `dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city` a
where 1=1
and subs_asterisk in ('Outros', 'Stone') 
and RIGHT(de42_merchant_id, 6) = '000000'
and reference_month = '2025-08-31'
)



, active_base_stone as (
  SELECT distinct  
  RPAD(affiliation_id, 15, '0') AS de42_merchant_id,
  LAST_DAY(ft.reference_date, MONTH) reference_date,
  FROM `dataplatform-prd.master_contact.v2_temp_cross_company` AS ft
  WHERE 1=1
    AND ft.company = 'STONE'
)


, activity_aug_based as (
select distinct
reference_date,
merchant_market_hierarchy_id,
from aux_aug a
inner join active_base_stone b using(de42_merchant_id)
)



, aux_tam as (
select distinct
reference_date,
merchant_market_hierarchy_id,
tam_id,
id_empresa,
b.tam_id is not null as is_stone,
from `dataplatform-prd.master_contact.final_tam`
left join (select distinct tam_id, reference_date, from `dataplatform-prd.master_contact.final_tam_acquirer` where acquirer = 'Stone') b using(reference_date, tam_id)
where 1=1
--and merchant_market_hierarchy_id is not null
)




select
reference_date,
count(*),
count(distinct tam_id) qtd_id,
count(distinct id_empresa) qtd_empresa,
count(distinct merchant_market_hierarchy_id) qtd_mmhid,

countif(is_stone) qtd_id_stone,
count(distinct if(is_stone, tam_id, null)) qtd_id_stone_distinct,
count(distinct if(is_stone, id_empresa, null)) qtd_empresa_stone,
count(distinct if(is_stone, merchant_market_hierarchy_id, null)) qtd_mmhid_stone,


countif(is_stone or b.merchant_market_hierarchy_id is not null) qtd_id_new,
count(distinct if(is_stone or b.merchant_market_hierarchy_id is not null, tam_id, null)) qtd_id_new_distinct,
count(distinct if(is_stone or b.merchant_market_hierarchy_id is not null, id_empresa, null)) qtd_empresa_new,
count(distinct if(is_stone or b.merchant_market_hierarchy_id is not null, merchant_market_hierarchy_id, null)) qtd_mmhid_new
from aux_tam a
left join activity_aug_based b using(reference_date, merchant_market_hierarchy_id)
group by 1