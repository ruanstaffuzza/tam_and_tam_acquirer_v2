with mcc_aux as (
select distinct
mcc, 
segmento_smart,
macro_classificacao,
from `dataplatform-prd.dados_abertos_receita.cnae_mcc2`
)


, aux_tam_final as (
select *, 
 from `dataplatform-prd.master_contact.final_tam` t
where 1=1
and reference_date >= '{{min_ref_month}}'
)




, aux_mmhid_rep as (
select 
distinct
reference_date,
tam_id,
cod_muni,
merchant_market_hierarchy_id,
from aux_tam_final
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
where reference_month  >= '{{min_ref_month}}'
)


, aux_delete_problematic as (
select distinct tam_id, reference_date
from aux_mmhid_rep a
left join city_places b using( reference_date, merchant_market_hierarchy_id)
qualify row_number() over(partition by reference_date, merchant_market_hierarchy_id 
order by a.cod_muni = b.cod_muni desc --# em primeiro os que batem o município
) > 1 --# elimina o primeiro (com mais chances do município bater)
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
b.tam_id is not null is_problematic,
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
from aux_tam_final a
left join `dataplatform-treated-prod.acquirer_core.dim_mcc` dim_mcc on a.mcc = dim_mcc.mcc_key
left join mcc_aux using(mcc)
left join aux_delete_problematic b using(reference_date, tam_id)
where 1=1
)


, pre_acquirer_table as (
select distinct
a.tam_id,
acquirer,
reference_date,
from `dataplatform-prd.master_contact.final_tam_acquirer` a
where 1=1
and reference_date >= '{{min_ref_month}}'
--and acquirer not in ('Adiq', 'Adyen', 'Ifood')
and acquirer is not null
)



, acquirer_table_id as (
select 
tam_id,
reference_date,
ARRAY_TO_STRING(ARRAY_AGG(DISTINCT acquirer), ', ') as acquirer_array
FROM  pre_acquirer_table
group by 1, 2
)


, acquirer_table_empresa as (
select 
id_empresa,
reference_date,
ARRAY_TO_STRING(ARRAY_AGG(DISTINCT acquirer), ', ') as acquirer_array
FROM  pre_acquirer_table
left join (select distinct id_empresa, tam_id, reference_date from aux_tam_final) using(tam_id, reference_date)
group by 1, 2
)



, city_cluster_table as (
select distinct
CAST(str_city_code as INT) as cod_muni,
--str_city_code as cod_muni,
str_city_cluster as city_cluster,
from `ctra-comercial-1554819299431.sandbox_hubs_planning.tb_city_clusters`
)


, choose_chr_empresa as (
select
reference_date,
id_empresa,
max(coalesce(document_type, 'Desconhecido')) document_type,
max(cod_muni) cod_muni,
min(is_problematic) is_problematic,
min(city_cluster) city_cluster,
min(cod_muni = 3534401) is_osasco
from tam_table
left join city_cluster_table using(cod_muni)
group by 1, 2
)




, aux_final as (

select * except(qtd_ec),
0 qtd_empresa, qtd_ec
from (
select 
reference_date,
coalesce(document_type, 'Desconhecido') as document_type,
acquirer_array,
has_mmhid,
city_cluster,

cod_muni = 3534401 as is_osasco,
is_problematic,
count(distinct tam_id) as qtd_ec,
from tam_table
left join acquirer_table_id using(tam_id, reference_date)
left join city_cluster_table using(cod_muni)
group by 1, 2, 3, 4, 5, 6, 7
)


union all

select * except(qtd_empresa),
qtd_empresa, 0 qtd_ec,
from (
select 
reference_date,
document_type,
acquirer_array,
False has_mmhid,
city_cluster,
is_osasco,
is_problematic,
count(distinct id_empresa) as qtd_empresa,
from choose_chr_empresa
left join acquirer_table_empresa using(id_empresa, reference_date)
group by 1, 2, 3, 4, 5, 6, 7
)
)

select 
reference_date,
document_type,
acquirer_array,
has_mmhid,
city_cluster,
is_osasco,
is_problematic,
sum(qtd_ec) qtd_ec,
sum(qtd_empresa) qtd_empresa,
from aux_final
group by 1, 2, 3, 4, 5, 6, 7
