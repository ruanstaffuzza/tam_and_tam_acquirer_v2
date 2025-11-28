
DECLARE ref_month  DATE DEFAULT '{{ref_month}}';

--CREATE OR REPLACE TABLE `dataplatform-prd.economic_research.v2_tam_acquirer_completa` PARTITION BY reference_date AS

INSERT INTO `dataplatform-prd.economic_research.v2_tam_acquirer_completa`

with gambiarra_stone as (
  select 
  concat(
     CAST(tam_id as STRING), 
     LEFT(REPLACE(CAST(DATE(reference_date) as STRING), '-', ''),6)
     ) as tam_id,
    reference_date,
     from `dataplatform-prd.master_contact.temp_ruan_gambiarra_eliminar_stone`
)


select distinct
a.reference_date,
a.tam_id,
acquirer,
market_place,
from `dataplatform-prd.economic_research.v2_pre_tam_acquirer_completa` a
left join gambiarra_stone b
   on a.reference_date = '2025-08-31'
   and a.tam_id = b.tam_id
   and a.acquirer = 'Stone'
left join `dataplatform-prd.master_contact.temp_ruan_gambiarra_eliminar_cieloget` c
   on a.reference_date = c.reference_date
   and a.tam_id = c.tam_id
   and a.acquirer in ('Cielo', 'GetNet')
where 1=1
and b.tam_id is null
and c.tam_id is null
and a.reference_date = ref_month

