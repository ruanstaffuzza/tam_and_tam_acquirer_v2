DECLARE ref_month  DATE DEFAULT '{{ref_month}}';


CREATE TEMP TABLE mkt_place_only AS (
select 
reference_date,
tam_id,
min(COALESCE(market_place, False)) only_market_place, 
min(acquirer in ('Mercado Livre', 'MercadoPago')) as only_mp_or_ml,
from dataplatform-prd.economic_research.v2_tam_acquirer_completa
where reference_date = ref_month
group by 1, 2
);


CREATE TEMP TABLE final_tam as (
select a.*,
coalesce(b.only_market_place, False) as only_market_place,
coalesce(b.only_mp_or_ml, False) and cod_muni = 3534401 as only_mp_or_ml_osasco,
from (
select * from `dataplatform-prd.master_contact.v2_aux_tam_final_tam` where reference_date >= '2025-06-01'
union all 
select * from `dataplatform-prd.master_contact.aux_tam_final_tam` where reference_date < '2025-06-01'
) a
left join mkt_place_only b using(reference_date, tam_id)
where 1=1
and reference_date = ref_month
);


CREATE TEMP TABLE final_tam_acquirer as (
select a.*,
only_market_place,
only_mp_or_ml_osasco,
from dataplatform-prd.economic_research.v2_tam_acquirer_completa a
left join final_tam using(reference_date, tam_id)
where 1=1
and reference_date = ref_month
);





--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.final_tam` PARTITION BY reference_date AS
INSERT INTO `dataplatform-prd.master_contact.final_tam`
select 
reference_date,
tam_id,
id_empresa,
cpf_cnpj,
cpf_cnpj_raiz,
document_type,
merchant_name,
merchant_market_hierarchy_id,
mcc,
cnae,
porte_rfb,
first_seen_week,
is_online,
cod_muni,
only_market_place,
only_mp_or_ml_osasco,
from final_tam a
where 1=1
and not only_market_place
and not only_mp_or_ml_osasco
;


--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.final_tam_marketplace` PARTITION BY reference_date AS
INSERT INTO `dataplatform-prd.master_contact.final_tam_marketplace`
select 
reference_date,
tam_id,
id_empresa,
cpf_cnpj,
cpf_cnpj_raiz,
document_type,
merchant_name,
merchant_market_hierarchy_id,
mcc,
cnae,
porte_rfb,
first_seen_week,
is_online,
cod_muni,
only_market_place,
only_mp_or_ml_osasco,
from final_tam
where only_market_place or only_mp_or_ml_osasco
;


--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.final_tam_acquirer` PARTITION BY reference_date AS
INSERT INTO `dataplatform-prd.master_contact.final_tam_acquirer`
select 
tam_id,
reference_date,
acquirer,
CASE
    WHEN COALESCE(market_place, FALSE) THEN 'marketplace'
    WHEN acquirer IN ('Pague Veloz','Listo','Iugu','Evoluservices','Asaas','PinPag','Vindi','Zoop','Cappta') THEN 'subadquirente'
    WHEN acquirer IN ('Pagarme','GetNet','Tribanco','Adyen','Bolt','Fiserv','MercadoPago', 'EC', 'InfinitePay',
                      'BMG/Granito','Cielo','Ton','SumUp','BRB','Sipag/Sicredi','Safra','Adiq','Rede',
                      'C6 Pay','Stone','PagSeguro','Sipag','Vero') THEN 'adquirente'
    WHEN acquirer = 'Não identificado' THEN 'Não identificado'
    ELSE null
  END AS acquirer_type
from final_tam_acquirer
where 1=1
and not only_market_place
and not only_mp_or_ml_osasco
;

--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.final_tam_acquirer_marketplace` PARTITION BY reference_date AS
INSERT INTO `dataplatform-prd.master_contact.final_tam_acquirer_marketplace`
select 

reference_date,
tam_id,
acquirer,
market_place, 
only_market_place,
only_mp_or_ml_osasco,
from final_tam_acquirer
where only_market_place or only_mp_or_ml_osasco 
;