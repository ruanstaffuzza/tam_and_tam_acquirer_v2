with mcc_aux as (
select distinct
mcc, 
segmento_smart,
from `dataplatform-prd.dados_abertos_receita.cnae_mcc2`
)


, pre_tam_table as (
select distinct
reference_date,
tam_id,
id_empresa,
mcc_cluster,
tier,
document_type,
tg.nome_regiao,
tg.uf,
segmento_smart,
porte_rfb,
merchant_market_hierarchy_id is not null has_mmhid,
from `dataplatform-prd.addressable_market.tam` t
left join `dataplatform-prd.addressable_market.tam_geocoded` tg using(tam_id, reference_date)
left join `dataplatform-prd.addressable_market.tam_tier` tt using(id_empresa, reference_date)
left join `dataplatform-treated-prod.acquirer_core.dim_mcc` dim_mcc on t.mcc = dim_mcc.mcc_key
left join mcc_aux using(mcc)
)


, aux_final as (
select 
reference_date,
mcc_cluster,
segmento_smart,
tier,
uf,
mcc,
document_type,
nome_regiao,
porte_rfb,
count(distinct tam_id) as qtd_ec,
count(distinct id_empresa) as qtd_empresa,
from tam_table
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)
