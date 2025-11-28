

with mcc_aux as (
select distinct
mcc, 
segmento_smart,
macro_classificacao,
from `dataplatform-prd.dados_abertos_receita.cnae_mcc2`
)


, aux_tam_v1 as (
select *,
1 as version
 from `dataplatform-prd.master_contact.aux_tam_final_tam` t
--left join `dataplatform-prd.master_contact.tam_geocoded_new` tg using(tam_id, reference_date)
--left join `dataplatform-prd.master_contact.tam_tier_review` tt using(id_empresa, reference_date)
--left join `dataplatform-treated-prod.acquirer_core.dim_mcc` dim_mcc on t.mcc = dim_mcc.mcc_key
where 1=1
and reference_date in ({{list_ref_months}})
)

, aux_tam_v2 as (
select *, 
2 as version
 from `dataplatform-prd.master_contact.final_tam` t
where 1=1
and reference_date in ({{list_ref_months}}) 
)

, aux_tam_final as (
select * from aux_tam_v1
union all
select * from aux_tam_v2
)


, tam_table as (
select distinct
reference_date,
tam_id,
id_empresa,
dim_mcc.mcc_cluster,
--tier,
a.mcc,
document_type,
--tg.nome_regiao_espacial nome_regiao,
--tg.uf_espacial uf,
segmento_smart,
porte_rfb,
--tg.cod_muni_espacial cod_muni,
cod_muni,
--is_online,
merchant_market_hierarchy_id is not null has_mmhid,
--macro_classificacao,
case when CAST(cod_muni as STRING) in (
  '1100205'  , -- Porto Velho'
  '1302603'   , -- Manaus'
  '1200401'  , -- Rio Branco'
    '5002704'  , -- Campo Grande'
    '1600303'  , -- Macapá'
    '5300108'  , -- Brasília'
    '1400100'  , -- Boa Vista'
    '5103403'  , -- Cuiabá'
    '1721000'  , -- Palmas'
    '3550308'  , -- São Paulo'
    '2211001'  , -- Teresina'
    '3304557'  , -- Rio de Janeiro'
    '1501402'  , -- Belém'
    '5208707'  , -- Goiânia'
    '2927408'  , -- Salvador'
    '4205407'  , -- Florianópolis'
    '2111300'  , -- São Luís'
    '2704302'  , -- Maceió'
    '4314902'  , -- Porto Alegre'
    '4106902'  , -- Curitiba'
    '3106200'  , -- Belo Horizonte'
    '2304400'  , -- Fortaleza'
    '2611606'  , -- Recife'
    '2507507'  , -- João Pessoa'
    '2800308'  , -- Aracaju'
    '2408102'  , -- Natal'
    '3205309'  , -- Vitória'
    '3534401'   -- Osasco'
) then CAST(cod_muni as STRING) else 'Outros' end as cod_muni_grouped,
version,
from aux_tam_final a
left join `dataplatform-treated-prod.acquirer_core.dim_mcc` dim_mcc on a.mcc = dim_mcc.mcc_key
left join mcc_aux using(mcc)
)



, acquirer_table as (

select 
version,
tam_id,
reference_date,
ARRAY_TO_STRING(ARRAY_AGG(DISTINCT acquirer), ', ') as acquirer_array
FROM (
select distinct
tam_id,
acquirer,
reference_date,
version from
(
select tam_id,
acquirer,
reference_date, 1 as version
from `dataplatform-prd.master_contact.aux_tam_final_tam_acquirer`
union all
select tam_id,
acquirer,
reference_date, 2 as version
from `dataplatform-prd.master_contact.final_tam_acquirer`
)

where 1=1
and acquirer is not null
and acquirer not in ('Adiq', 'Adyen', 'Ifood')
and reference_date in ({{list_ref_months}})
order by version, reference_date, tam_id, acquirer
) group by 1, 2, 3
)


, city_cluster_table as (
select distinct
CAST(str_city_code as INT) as cod_muni,
--str_city_code as cod_muni,
str_city_cluster as city_cluster,
from `ctra-comercial-1554819299431.sandbox_hubs_planning.tb_city_clusters`
)


select 
reference_date,
--coalesce(mcc_cluster, 'Desconhecido') as mcc_cluster,
--coalesce(segmento_smart, 'Desconhecido') as segmento_smart,
--tier,
--uf,
--mcc,
coalesce(document_type, 'Desconhecido') as document_type,
--nome_regiao,
acquirer_array,
coalesce(porte_rfb, 'Desconhecido') as porte_rfb,
--cod_muni,
--is_online,
has_mmhid,
--macro_classificacao,
city_cluster,
--cod_muni_grouped,
cod_muni = 3534401 as is_osasco,
version,
count(distinct tam_id) as qtd_ec,
count(distinct id_empresa) as qtd_empresa,
from tam_table
left join acquirer_table using(version, tam_id, reference_date)
left join city_cluster_table using(cod_muni)
group by 1, 2, 3, 4, 5, 6, 7, 8
