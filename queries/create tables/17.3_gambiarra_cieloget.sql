DECLARE ref_month  DATE DEFAULT '{{ref_month}}';

--CREATE TABLE `dataplatform-prd.master_contact.temp_ruan_gambiarra_eliminar_cieloget` AS


INSERT INTO `dataplatform-prd.master_contact.temp_ruan_gambiarra_eliminar_cieloget`

with final_tam as (
    select * from (
select * from `dataplatform-prd.master_contact.v2_aux_tam_final_tam` where reference_date >= '2025-06-01'
union all 
select * from `dataplatform-prd.master_contact.aux_tam_final_tam` where reference_date < '2025-06-01'
)
where 1=1
and reference_date = ref_month
)

, pre_acquirer_table as (
select distinct
a.tam_id,
acquirer,
reference_date,
from `dataplatform-prd.economic_research.v2_pre_tam_acquirer_completa` a
where 1=1
and acquirer in ('Cielo', 'GetNet')
and reference_date = ref_month
)


, aux_mmhid_rep as (
select 
distinct
reference_date,
tam_id,
cod_muni,
merchant_market_hierarchy_id,
from final_tam
inner join pre_acquirer_table using(tam_id, reference_date)
where 1=1
--and merchant_market_hierarchy_id is not null
qualify count(distinct cod_muni) over(partition by 
                                      reference_date, 
                                      merchant_market_hierarchy_id
                                      ,cpf_cnpj
                                      ) > 1
)

, city_places as (
select distinct
reference_month reference_date,
merchant_market_hierarchy_id,
CAST(cod_muni_cidade as INT) cod_muni, 
from `dataplatform-prd.economic_research.geo_places`
where 1=1
and reference_month = ref_month
)


--# delete from final acquirer 
select distinct tam_id, reference_date
from aux_mmhid_rep a
left join city_places b using( reference_date, merchant_market_hierarchy_id)
qualify row_number() over(partition by reference_date, merchant_market_hierarchy_id 
order by a.cod_muni = b.cod_muni desc --# em primeiro os que batem o município
) > 1 --# elimina o primeiro (com mais chances do município bater)
