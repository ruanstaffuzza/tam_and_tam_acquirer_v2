DECLARE ref_month  DATE DEFAULT '{{ref_month}}';


--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.v2_aux_tam_final_tam_acquirer` PARTITION BY reference_date AS
INSERT INTO `dataplatform-prd.master_contact.v2_aux_tam_final_tam_acquirer`


with aux as (
select distinct * except(adquirente, modelo_previsao),
case when adquirente = 'CloudWalk' then 'InfinitePay'
     when issue_get>100 then null
     else adquirente end acquirer,
from `dataplatform-prd.master_contact.v2_aux_tam_acquirers`
where reference_date = ref_month
)

, 
select distinct reference_date, tam_id, acquirer, 
from `dataplatform-prd.master_contact.v2_aux_tam_final_tam`
left join aux using(tam_id, reference_date)
where 1=1 
  and acquirer not in ('possivel_erro', 'Outras', 'Desconhecido')
  and acquirer is not null
  and reference_date = ref_month
