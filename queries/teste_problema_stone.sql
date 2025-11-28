
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
and reference_month >= '2025-07-01'
)



, active_base_stone as (
  SELECT distinct  
  RPAD(affiliation_id, 15, '0') AS de42_merchant_id,
  LAST_DAY(ft.reference_date, MONTH) reference_date,
  FROM `dataplatform-prd.master_contact.v2_temp_cross_company` AS ft
  WHERE 1=1
    AND ft.company = 'STONE'
    and ft.reference_date >= '2025-07-01'
)




, activity_based as (
select distinct
reference_date,
merchant_market_hierarchy_id,
from aux_aug a
inner join active_base_stone b using(de42_merchant_id, reference_date)
)



, aux_by_mmhid as (
select
merchant_market_hierarchy_id,
max(reference_date) max_ref,
min(reference_date) min_ref,
from activity_based
group by 1
)



select
case when max_ref = min_ref then CAST(max_ref as string)
     when max_ref <> min_ref then 'both'
     else 'unknown'
end as status,
count(*) qtd_mmhid
from aux_by_mmhid
group by 1