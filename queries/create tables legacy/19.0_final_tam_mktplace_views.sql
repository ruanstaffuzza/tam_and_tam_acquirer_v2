
CREATE OR REPLACE VIEW `dataplatform-prd.master_contact.vw_final_tam` AS


with final_tam as (

select * from `dataplatform-prd.master_contact.v2_aux_tam_final_tam` where reference_date >= '2025-06-01'
union all 
select * from `dataplatform-prd.master_contact.aux_tam_final_tam` where reference_date < '2025-06-01'

)


, aux_tam_acquirer as (
select distinct
reference_date,
tam_id,
acquirer in ('Mercado Livre', 'MercadoPago') as mp_or_ml,
market_place,
from `dataplatform-prd.economic_research.v2_tam_acquirer_completa`
)




, ml_osasco_only_table AS (
select 
reference_date,
tam_id,
min(COALESCE(mp_or_ml, False)) only_mp_or_ml, 
from aux_tam_acquirer
inner join (
select 
reference_date,
tam_id, 
from final_tam
where 1=1
and cod_muni = 3534401 -- Osasco
) using(reference_date, tam_id)
group by 1, 2
)



, mktplace_table AS (
select 
reference_date,
tam_id,
min(COALESCE(market_place, False)) only_market_place, 
from aux_tam_acquirer
group by 1, 2
)


select a.*, b.only_market_place, 
from final_tam a
inner join (select distinct reference_date, tam_id, only_market_place from mktplace_table where not only_market_place
) b using(reference_date, tam_id)
left join ml_osasco_only_table using(reference_date, tam_id)
where 1=1
and not coalesce(only_mp_or_ml, False)
;
