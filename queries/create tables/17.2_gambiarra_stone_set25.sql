
CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.temp_ruan_gambiarra_eliminar_stone_set25` AS



with aux_mmhid_problema as (

select distinct a.merchant_market_hierarchy_id, document, reference_month reference_date,
  from (select * from `dataplatform-prd.master_contact.tmp_ruan_test_stone_tam` where reference_month in ('2025-09-30', '2025-08-31')) a
  left join (select distinct de42_merchant_id, merchant_market_hierarchy_id,  from `dataplatform-prd.master_contact.tmp_ruan_test_stone_tam` where reference_month in ('2025-07-31', '2025-06-30') ) b using(merchant_market_hierarchy_id, de42_merchant_id)
  left join (select distinct reference_month, merchant_market_hierarchy_id from `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places`
  where first_seen_week >= DATE_ADD(reference_month, interval -45 day) ) c
  using(reference_month, merchant_market_hierarchy_id)
  where b.merchant_market_hierarchy_id is null and c.merchant_market_hierarchy_id is not null --#relação nova & EC novo
)


, ecs_stone as (
select distinct tam_id, cpf_cnpj document, merchant_market_hierarchy_id, acquirer, reference_date
from `dataplatform-prd.economic_research.v2_pre_tam_acquirer_completa` a
inner join `dataplatform-prd.master_contact.v2_aux_tam_final_tam` using(reference_date, tam_id)
where 1=1
and a.reference_date in ('2025-09-30', '2025-08-31')
and a.acquirer = 'Stone'
)

select distinct 
tam_id, acquirer, reference_date
from aux_mmhid_problema
inner join ecs_stone using(document, merchant_market_hierarchy_id, reference_date)