/*
Version: 1
Author: Ruan

Description:
Essa query cria a tabela auxiliar subsequente ao aux_tam_python_agrupados (tratamento feito em python), que é a aux_tam_pos_python.
Os objetivos dessa query são:
- 0. adicionar o reference_month ao final do tam_id
- 1. Escolher o documento preferido para cada tam_id e reference_month
- 2. Escolher o nome preferido para cada tam_id e reference_month
- 3. Incluir a flag in_bus_flag_30d
- 4. Excluir os registros que possuem subs_asterisk = 'MercadoPago_subPagarme' e len_resultado = 1
- 5. Incluir os de42 que são documentos da CloudWalk
*/

DECLARE ref_month  DATE DEFAULT '{{ref_month}}';
--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.v3_aux_tam_pos_python` PARTITION BY reference_date AS 



INSERT INTO `dataplatform-prd.master_contact.v3_aux_tam_pos_python` 

with atividade_places as (
  SELECT DISTINCT 
  reference_month,
  merchant_market_hierarchy_id,
  merchant_name_cleansed,
  in_bus_flag_30d,
  first_seen_week,
  merchant_category_code mcc_places,
  FROM 
    `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places`
     WHERE reference_month=ref_month
)

, names_documents as (
  select * from (
  select distinct 
  nome, coalesce(cnpj, cpf) document
  from `dataplatform-prd.master_contact.names_documents`
  where situacao_cadastral_ativo
  )
  qualify row_number() over(partition by document)=1
)

, docs_raiz as (
select distinct
left(document,8) as numero_inicio,
document document_rfb
from names_documents
where LENGTH(document) = 14
qualify row_number() over(partition by numero_inicio)=1
)


/*

, aux_final_nomes as (
select * except(mmhid_merge, pre_doc_unmerge),
  coalesce(pre_doc_unmerge, '') pre_doc_unmerge,
  coalesce(mmhid_merge, 99) new_mmhid_merge,
from `dataplatform-prd.master_contact.v2_aux_tam_final_nomes`
where reference_month = ref_month
)


, aux_python_agrupados as (
select * except(pre_doc_unmerge),
coalesce(mmhid_merge, 99) new_mmhid_merge,
coalesce(pre_doc_unmerge, '') pre_doc_unmerge,
from `dataplatform-prd.master_contact.v2_aux_tam_python_agrupados`
where reference_month = ref_month

)



, validade_numero_inicio as (
select *,
COALESCE(document_rfb, IF(LENGTH(numero_inicio)=8, null, numero_inicio)) as numero_inicio_doc_valido,
from aux_python_agrupados 
left join aux_final_nomes using(reference_month, cod_muni, inicio, nome_master, is_mp, pre_doc_unmerge, new_mmhid_merge)
left join docs_raiz using(numero_inicio)
)
*/


,  aux_final_nomes as (
select distinct * except(mmhid_merge, pre_doc_unmerge, id_merge_places, is_mp, id_ton),
  coalesce(pre_doc_unmerge, '') doc_unmerge,
  coalesce(mmhid_merge, 99) new_mmhid_merge,
  coalesce(id_merge_places, '') id_merge_places
from `dataplatform-prd.master_contact.v3_aux_tam_final_nomes`
where reference_month = ref_month
)


, aux_python_agrupados as (
select * except(id_merge_places, grouped_names, pre_doc_unmerge),
coalesce(mmhid_merge, 99) new_mmhid_merge,
coalesce(pre_doc_unmerge, '') doc_unmerge,
coalesce(id_merge_places, '') id_merge_places,
from `dataplatform-prd.master_contact.v3_aux_tam_python_agrupados`
where reference_month = ref_month
{{ingestion_filter}}
)


, validade_numero_inicio as (
select * except(merchant_market_hierarchy_id, mmhid_merge),
COALESCE(document_rfb, IF(LENGTH(numero_inicio)=8, null, numero_inicio)) as numero_inicio_doc_valido,
mmhid_merge as merchant_market_hierarchy_id,
merchant_market_hierarchy_id as mmhid_original,
from aux_python_agrupados
left join aux_final_nomes using(
reference_month,
inicio,
new_mmhid_merge,
nome_master, 
cod_muni,
id_merge_places,
doc_unmerge
)
left join docs_raiz using(numero_inicio)
)





, choose_prefered as (
select 
* except(tam_id, numero_inicio_doc_valido),
concat(
     CAST(tam_id as STRING), 
     LEFT(REPLACE(CAST(DATE(reference_month) as STRING), '-', ''),6)
     ) as tam_id,
from (
SELECT
* except(nome_master_com_espaco, merchant_name_cleansed, in_bus_flag_30d),
coalesce(
IF(subs_asterisk in ('Ton', 'Outros_Stone', 'Pagarme'), coalesce(cnpj, cpf), null)
,merchant_tax_id, numero_inicio_doc_valido, cnpj, cpf, cpf_brasil) document,
count(*) over(partition by tam_id, reference_month) len_resultado,
CASE when subs_asterisk in ('Ton', 'MercadoPago_subPagarme', 'Outros_Stone', 'Pagarme') then subs_asterisk
     when merchant_tax_id is not null then 'Places'
     else 'Busca' end as source_document,
MAX(COALESCE(
     IF(subs_asterisk in ('Ton', 'Outros_Stone', 'Pagarme'), TRUE, null),
     in_bus_flag_30d, TRUE)) over(partition by tam_id, reference_month) in_bus_flag_30d,
coalesce(IF(subs_asterisk in ('Ton', 'Outros_Stone', 'Pagarme'), nome_master_com_espaco, null), merchant_name_cleansed, nome_master_com_espaco) as merchant_name,
from validade_numero_inicio
left join atividade_places using(merchant_market_hierarchy_id, reference_month)
)
where in_bus_flag_30d
)


, df_choice_document as (
select * EXCEPT(order_docu)
from (
select 
reference_month,
tam_id,
document,
source_document,
CASE WHEN source_document = 'Outros_Stone' THEN 1
     WHEN source_document = 'Ton' THEN 2
     WHEN source_document = 'Pagarme' THEN 3
     WHEN source_document = 'Places' THEN 4
     WHEN source_document = 'MercadoPago_subPagarme' THEN 5
     ELSE 5 END as order_docu,
from choose_prefered
where 1=1
and document is not null
and len_resultado > 1
)
qualify row_number() over(partition by tam_id, reference_month order by order_docu)=1
)


, choose_prefered_document as (
select a.* EXCEPT(source_document, document),
coalesce(b.source_document, a.source_document) as source_document,
coalesce(b.document, a.document) as document
from choose_prefered a
left join df_choice_document b using(tam_id, reference_month)
)

, df_choice_name as (
select * EXCEPT(order_name, order_name_tipo_documento)
from (
select
reference_month,
tam_id,
merchant_name,
source_name,
source_document,
document,
CASE WHEN source_name = 'Cadastro Interno' then 1
     WHEN source_name = 'Places' THEN 2
     WHEN source_name = 'Documento' THEN 3
     ELSE 4 END as order_name,
case when cnpj is not null then 1
     when cpf is not null then 2
     when cpf_brasil is not null then 3
     else 4 end as order_name_tipo_documento,
FROM (select *,
CASE when subs_asterisk in ('Ton', 'Outros_Stone', 'Pagarme') then 'Cadastro Interno'
     when merchant_market_hierarchy_id is not null then 'Places'
     when document is not null then 'Documento' 
     else 'Original' end as source_name
from choose_prefered_document)
)
qualify row_number() over(partition by tam_id, reference_month order by order_name, order_name_tipo_documento, LENGTH(merchant_name) desc)=1
)



, df_choice_name2 as (
  select a.* EXCEPT(merchant_name),
  coalesce(b.nome, a.merchant_name) as merchant_name
  from df_choice_name a
  left join names_documents b on a.source_document='Documento' and a.document=b.document
  )

, escolha_final as (
select a.* EXCEPT(merchant_name, numero_inicio),
coalesce(b.merchant_name, a.merchant_name) as merchant_name,
source_name,
coalesce(numero_inicio, '') numero_inicio
from choose_prefered_document a
left join df_choice_name2 b using(tam_id, reference_month)
where NOT (subs_asterisk = 'MercadoPago_subPagarme' AND len_resultado=1)
)


, docs_cloud_walk as (
select * from (
select distinct
DATE(reference_month) reference_month,
subs_asterisk,
numero_inicio,
REGEXP_REPLACE(TRIM(REGEXP_REPLACE(nome_limpo, r'^SHOPEE|IFOOD', '')), r'[^A-Z]', '') nome_master,
cod_muni,
TRIM(de42_merchant_id) AS de42_document,
--original_name, 
from `dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city`
where 1=1
and subs_asterisk ='CloudWalk'
and LENGTH(TRIM(de42_merchant_id)) in (11, 14)
)
qualify row_number() over(partition by reference_month, subs_asterisk, numero_inicio, nome_master, cod_muni)=1
)

, aux_final1 as (
select * except(document, source_document),
document, source_document,
from escolha_final
where not (subs_asterisk = 'CloudWalk' and document is null)

union all

select a.* except(document, source_document),
de42_document as document,
'CloudWalk' as source_document,
from escolha_final a
left join docs_cloud_walk b using(reference_month, subs_asterisk, numero_inicio, nome_master, cod_muni)
where (subs_asterisk = 'CloudWalk' and document is null)
)

, aux_final2 as (
select * except(document, source_document, merchant_name),
max(document) over(partition by tam_id, reference_month) as document,
max(source_document) over(partition by tam_id, reference_month) as source_document,
max(merchant_name) over(partition by tam_id, reference_month) as merchant_name,
from aux_final1
)


select  
DATE(reference_month) as reference_date,
tam_id, merchant_market_hierarchy_id, merchant_name, subs_asterisk, 
        document, 
        --coalesce(source_document, 'Sem Documento') as source_document,
        IF(document is null, 'Sem Documento', source_document) as source_document,
        merchant_tax_id, numero_inicio, cnpj, cpf, cpf_brasil,
       --# grouped_names,
        -- Include any other columns that are not in the order_cols list
        * EXCEPT(reference_month, tam_id, merchant_market_hierarchy_id, merchant_name, subs_asterisk, 
                 document, source_document, merchant_tax_id, numero_inicio, cnpj, cpf, cpf_brasil
                 --,grouped_names
                 )
from aux_final2







/*

, aux_2 as (
select 
reference_month,
group_id,
tam_id,
merchant_market_hierarchy_id,
nome_master,
subs_asterisk,
document, 
source_document,
merchant_tax_id,
numero_inicio,
cnpj,
cpf,
cpf_brasil,
grouped_names,	
cod_muni,
--nome_ton, #lidar
--agrupamento_nome_1,
--mmhid_merge #lidar
--len_resultado
document, #lidar
--agrupamento_inspecao,
--qtd_distinct_agrupamento_nome_1,
--len_inspecao,
has_mp,
has_mp_subs,
has_outros,
has_pagseguro,
has_sumup,
has_ton,
has_not_ton,
--has_stone, 
--qtd_mmhid_tax_id,
--grouped_tax_id,
qtd_nomes,
from aux_1
)


, mcc_interno as (
  select 
  document,
  mcc_id mcc,      
  from  `dataplatform-treated-prod.cross_company.payments__client_affiliation`
  qualify row_number() over(partition by document order by created_at desc)=1
)

, docs_receita as (
select distinct 
left(cpf_cnpj,8) as cpf_cnpj_raiz,
cpf_cnpj
from `dataplatform-prd.dados_abertos_receita.vw_empresas`
)

select distinct 
DATE(reference_month) as reference_date,
tam_id, 
coalesce(e.cpf_cnpj, documento_final, '') cpf_cnpj,
max(merchant_market_hierarchy_id) over(partition by tam_id, reference_month) merchant_market_hierarchy_id,
cnae_fiscal_principal,
coalesce(c.mcc, d.mcc) mcc, 
coalesce(source_document, '') fonte_cpf_cnpj,
cod_muni,
nome_muni,
aux.uf,
group_id,


has_mp,
has_mp_subs,
has_outros,
has_pagseguro,
has_sumup,
has_ton,
--has_stone, 


from aux_2
left join `dataplatform-prd.dados_abertos_receita.vw_estabelecimentos` b on aux.documento_final=b.cpf_cnpj
left join  `dataplatform-prd.dados_abertos_receita.cnae_mcc` c on b.cnae_fiscal_principal=CAST(c.codigo_cnae as INT)
left join mcc_interno d on aux.documento_final=d.document
left join docs_receita e on aux.documento_final=e.cpf_cnpj_raiz
where in_bus_flag_30d

*/