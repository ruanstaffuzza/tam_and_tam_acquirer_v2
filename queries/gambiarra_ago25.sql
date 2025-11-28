

CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.temp_ruan_gambiarra_stone_tam_202508` AS 

with aux_aug as (
select 
distinct 
reference_month reference_date,
merchant_market_hierarchy_id, 
de42_merchant_id,
from `dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city` a
where 1=1
and subs_asterisk in ('Outros', 'Stone') 
and RIGHT(de42_merchant_id, 6) = '000000'
--and reference_month = '2025-08-31'
and reference_month >= '2025-05-01'
)



, active_base_stone as (
  SELECT distinct  
  RPAD(affiliation_id, 15, '0') AS de42_merchant_id,
  LAST_DAY(ft.reference_date, MONTH) reference_date,
  FROM `dataplatform-prd.master_contact.v2_temp_cross_company` AS ft
  WHERE 1=1
    AND ft.company = 'STONE'
    and ft.reference_date >= '2025-05-01'
)




, activity_based as (
select distinct
reference_date,
merchant_market_hierarchy_id,
de42_merchant_id,
from aux_aug a
inner join active_base_stone b using(de42_merchant_id, reference_date)
)



, aux_by_mmhid as (
select
merchant_market_hierarchy_id,
min(reference_date) min_ref,
from activity_based
inner join (select distinct merchant_market_hierarchy_id from activity_based where reference_date='2025-08-31'
qualify count(*) over(partition by de42_merchant_id) >1
) using(merchant_market_hierarchy_id)
group by 1
)

, aux_final as (
select distinct
merchant_market_hierarchy_id
from aux_by_mmhid
where min_ref = '2025-08-31'
)

select distinct tam_id,
from `dataplatform-prd.master_contact.vw_final_tam`
inner join (select tam_id, reference_date from `dataplatform-prd.economic_research.v2_tam_acquirer_completa` where acquirer = 'Stone') using(reference_date, tam_id)
inner join aux_final using(merchant_market_hierarchy_id)
where 1=1 
and reference_date = '2025-08-31'
