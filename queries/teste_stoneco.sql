
with aux_tam_v1 as (
select *,
1 as version
 from `dataplatform-prd.master_contact.aux_tam_final_tam` t
where 1=1
--and reference_date in ({{list_ref_months}})
)

, aux_tam_v2 as (
select *, 
2 as version
 from `dataplatform-prd.master_contact.v2_aux_tam_final_tam` t
where 1=1
--and reference_date in ({{list_ref_months}}) 
)

, aux_tam_final as (
select * from aux_tam_v1
union all
select * from aux_tam_v2
)

, aux_acquirer as (
select *, 1 as version
from `dataplatform-prd.master_contact.aux_tam_final_tam_acquirer`
union all
select *, 2 as version
from `dataplatform-prd.master_contact.v2_aux_tam_final_tam_acquirer`
)




, tam_table as (
select 
reference_date,
cpf_cnpj document,
version,
coalesce(document_type, 'Desconhecido') as document_type,
max(merchant_market_hierarchy_id is not null) has_mmhid,
max(acquirer = 'Stone') has_stone,
max(acquirer = 'Ton') has_ton,
max(acquirer = 'Pagarme') has_pagarme,
from aux_tam_final a
inner join (select distinct tam_id, reference_date, acquirer, version from aux_acquirer where 1=1 and acquirer in ('Stone', 'Ton', 'Pagarme') )
using(version, reference_date, tam_id)
group by 1,2,3,4
)


, aux_tpv as (
select
reference_date,
document,
max(company = 'STONE') has_stone_tpv,
max(company = 'TON') has_ton_tpv,
max(company = 'PAGARME') has_pagarme_tpv
from `dataplatform-prd.master_contact.temp_ruan_tpv`
where 1=1
--and reference_date in ({{list_ref_months}})
group by 1,2
)


select 
coalesce(tt.reference_date, att.reference_date) reference_date,
1 as version,
has_stone, 
has_ton, 
has_pagarme,
has_stone_tpv, 
has_ton_tpv, 
has_pagarme_tpv,
count(*) qtd_documents
from (select * from tam_table where version = 1) tt
full outer join aux_tpv att using(reference_date, document)
group by 1,2,3,4,5,6,7,8

union all

select 
coalesce(tt.reference_date, att.reference_date) reference_date,
2 as version,
has_stone, 
has_ton, 
has_pagarme,
has_stone_tpv, 
has_ton_tpv, 
has_pagarme_tpv,
count(*) qtd_documents
from (select * from tam_table where version = 2) tt
full outer join aux_tpv att using(reference_date, document)
group by 1,2,3,4,5,6,7,8
