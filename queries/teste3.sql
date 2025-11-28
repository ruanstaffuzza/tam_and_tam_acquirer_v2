DECLARE ref_month  DATE DEFAULT '2025-04-30';

/*
select
reference_date,
acquirer,
count(*),
count(distinct tam_id) qtd_id, 
count(distinct id_empresa) qtd_empresa,
count(distinct merchant_market_hierarchy_id) qtd_mmhid
from `dataplatform-prd.master_contact.vw_final_tam` t
inner join (select distinct tam_id, reference_date, acquirer from `dataplatform-prd.economic_research.v2_tam_acquirer_completa` where acquirer in ('Cielo', 'GetNet')) using(reference_date, tam_id)
where reference_date >= ref_month
group by 1, 2
order by 2, 1
*/

select
reference_month reference_date,
adquirente_padroes acquirer,
count(*),
count(distinct de42) qtd_de42,
sum(qtd_nomes_x_muni) qtd_nomes_x_muni,
--count(distinct id_empresa) qtd_empresa,
count(distinct merchant_market_hierarchy_id) qtd_mmhid
from `dataplatform-prd.master_contact.v2_aux_tam_pre_acquirer` t
where reference_month >= ref_month
and adquirente_padroes in ('Cielo', 'GetNet')
group by 1, 2
order by 2, 1
;