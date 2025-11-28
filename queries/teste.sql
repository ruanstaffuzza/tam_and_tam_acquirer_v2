
with aux as (
select distinct 
reference_date,
subs_asterisk,
nome_master,
mmhid_merge, 
from `dataplatform-prd.master_contact.v3_aux_tam_pos_python`
where 1=1
and reference_date in ('2025-07-31', '2025-08-31') 
)


, aux2 as (
select
reference_date,
subs_asterisk,
nome_master,
count(distinct mmhid_merge) qtd_mmhid
from aux
group by 1, 2, 3
)

, aux3 as (
select
subs_asterisk,
qtd_mmhid,
countif(reference_date='2025-07-31') qtd_jul,
countif(reference_date='2025-08-31') qtd_aug,
from aux2
group by 1, 2
)


select
subs_asterisk,
sum(qtd_jul) qtd_jul,
sum(qtd_aug) qtd_aug,
round(SAFE_DIVIDE(sum(IF(qtd_mmhid>0, qtd_jul, 0))*100, sum(qtd_jul)), 2) pct_jul_with_mmhid,
round(SAFE_DIVIDE(sum(IF(qtd_mmhid>0, qtd_aug, 0))*100, sum(qtd_aug)), 2) pct_aug_with_mmhid,
round(SAFE_DIVIDE(sum(IF(qtd_mmhid>1, qtd_jul, 0))*100, sum(qtd_jul)), 2) pct_jul_with_1m_mmhid,
round(SAFE_DIVIDE(sum(IF(qtd_mmhid>1, qtd_aug, 0))*100, sum(qtd_aug)), 2) pct_aug_with_1m_mmhid,
round(SAFE_DIVIDE(sum(IF(qtd_mmhid>2, qtd_jul, 0))*100, sum(qtd_jul)), 2) pct_jul_with_2m_mmhid,
round(SAFE_DIVIDE(sum(IF(qtd_mmhid>2, qtd_aug, 0))*100, sum(qtd_aug)), 2) pct_aug_with_2m_mmhid
from aux3
group by 1