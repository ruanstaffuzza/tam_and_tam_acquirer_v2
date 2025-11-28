
DECLARE ref_month  DATE DEFAULT '{{ref_month}}';
--CREATE OR REPLACE TABLE `dataplatform-prd.economic_research.v2_pre_tam_acquirer_completa` PARTITION BY reference_date AS

INSERT INTO `dataplatform-prd.economic_research.v2_pre_tam_acquirer_completa`


with final_tam as (
select * from `dataplatform-prd.master_contact.v2_aux_tam_final_tam` where reference_date >= '2025-06-01'
and reference_date = ref_month
--union all select * from `dataplatform-prd.master_contact.aux_tam_final_tam` where reference_date < '2025-06-01'
)

/*
, aux_deleted as (
select distinct
reference_month reference_date,
subs_asterisk2,
merchant_market_hierarchy_id,
from `dataplatform-prd.master_contact.aux_tam_subs_asterisk_delete_analysis`
where 1=1
and reference_month < '2025-06-01'
)

, aux_tam as (
select 
distinct
reference_date,
tam_id, 
merchant_market_hierarchy_id,
from `dataplatform-prd.master_contact.aux_tam_final_tam`
where 1=1
and merchant_market_hierarchy_id is not null
)


, final_new_acquirer_deleted as (
select distinct
reference_date, 
subs_asterisk2 adquirente,
tam_id, 
from aux_deleted a
inner join aux_tam b using(reference_date, merchant_market_hierarchy_id )
where 1=1
and subs_asterisk2 not in (
  'Ton', 'Outros', 'delete_asterisk', 'Pagarme'
)
)


, old_tam_acquirer as (
select distinct tam_id, reference_date, adquirente, issue_get from `dataplatform-prd.master_contact.aux_tam_acquirers_v2` where reference_date < '2025-06-01'
union distinct
select distinct tam_id, reference_date, adquirente, 0 as issue_get
from final_new_acquirer_deleted

)

*/

, final_adq as (
select tam_id, reference_date, adquirente, issue_get from`dataplatform-prd.master_contact.v2_aux_tam_acquirers` where reference_date >= '2025-06-01'
and reference_date = ref_month
--union all select * from old_tam_acquirer
)


, final as (
select distinct 
reference_date, tam_id,
case when adquirente = 'CloudWalk' then 'InfinitePay'
     when issue_get>100 then null
     when adquirente  in ('possivel_erro', 'Outras', 'Desconhecido', 'Previsão Incerta') then  'Não identificado'
     when adquirente = 'EC' then 'MercadoPago'
     when adquirente in ('MercadoLivre1', 'MercadoLivre2', 'MercadoPago_mmhid_problema', 'MP_mmhid_problema', 'MercadoLivre1_mmhid_problema',
     'MercadoLivre3') then 'Mercado Livre'
     when adquirente in ('Magalu1', 'Magalu2') then 'Magalu'
     when adquirente = 'Adiqplus' then 'Adiq'
     else adquirente end acquirer,
COALESCE(adquirente in (
     '99Food', 'Aliexpress', 'Amazon', 'Americanas', 'Hotmart', 'Magalu1', 'Magalu2', 'MercadoLivre1',
     'MercadoLivre3',
     'MercadoLivre2', 
                'MercadoPago_mmhid_problema', 
                'Shein', 'Shopee',
                'MP_mmhid_problema', 
                'MercadoLivre1_mmhid_problema',
                'Ifood',
                'Hotmart',
                'Kiwify',
                'Rappi'
                ), FALSE) market_place, 

from final_tam
left join final_adq using(tam_id, reference_date)
where 1=1
and not COALESCE(issue_get, 0)>100 
and not adquirente = 'Desconhecido - XXXX' --# Retirando os que tem de42 mascarado XXXXXXXX que aparece apartir de 2025-07

)


select distinct
a.reference_date,
a.tam_id,
acquirer,
market_place,
from final a