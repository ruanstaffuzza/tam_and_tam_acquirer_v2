
CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.temp_explorar_get_cielo`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
) AS





with aux as (
select distinct
reference_date,
acquirer,
tam_id,
id_empresa,
merchant_market_hierarchy_id,
cod_muni,
cpf_cnpj,
from `dataplatform-prd.master_contact.vw_final_tam` t
left join (select distinct tam_id, reference_date, acquirer from `dataplatform-prd.economic_research.v2_tam_acquirer_completa` where acquirer in (
  'Cielo', 'GetNet', 'Stone', 

    'Stone'
    

)) using(reference_date, tam_id)
where reference_date >= '2025-06-01'
)



select *,
IF(reference_date='2025-08-31', 1, 0) qtd_ago,
IF(reference_date='2025-07-31', 1, 0) qtd_jul,
count(distinct cod_muni) over(partition by merchant_market_hierarchy_id, reference_date) qtd_dis_muni,
count(distinct tam_id) over(partition by merchant_market_hierarchy_id, reference_date) qtd_dis_id,
count(distinct cod_muni) over(partition by 
                                      reference_date, 
                                      merchant_market_hierarchy_id
                                      ,cpf_cnpj
                                      ) qtd_dis_muni_mmhid_cnpj,

min(reference_date) over(partition by merchant_market_hierarchy_id, acquirer) = reference_date as first_month,
max(acquirer='Stone') over(partition by tam_id, reference_date) id_has_stone,
max(acquirer='Stone') over(partition by merchant_market_hierarchy_id, reference_date) mmhid_has_stone,
from aux

