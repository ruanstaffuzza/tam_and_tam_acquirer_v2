DECLARE ref_month  DATE DEFAULT '{{ref_month}}';

--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.v2_aux_tam_final_tam` PARTITION BY reference_date AS 


INSERT INTO `dataplatform-prd.master_contact.v2_aux_tam_final_tam`


with aux_init_table as (
select *
from `dataplatform-prd.master_contact.v3_aux_tam_pos_python` 
where 1=1
and reference_date=ref_month
)

, aux_issue_get as (
select 
reference_date,
tam_id, 
min(coalesce(issue_get,0)>100) issue_get,
from aux_init_table
left join (select * from  `dataplatform-prd.master_contact.v2_aux_tam_acquirers`) using(reference_date, tam_id)
where 1=1 
and reference_date=ref_month
group by 1, 2
)

, init_table as (
select a.*
from aux_init_table a
left join aux_issue_get b using(reference_date, tam_id)
where not b.issue_get  --# Retirando os que tem problema Getnet com problema (de42 com multiplos ECs)
)


, mcc_interno as (
  select 
  document,
  mcc_id mcc,      
  from  `dataplatform-treated-prod.cross_company.payments__client_affiliation`
  where mcc_id is not null
  qualify row_number() over(partition by document order by created_at desc)=1
)

, docs_receita as (
select distinct 
left(cpf_cnpj,8) as cpf_cnpj_raiz,
cpf_cnpj
from `dataplatform-prd.dados_abertos_receita.vw_empresas`
)

, mei as (
  select 
      left(cpf_cnpj,8) as cpf_cnpj_raiz,
      max(opcao_mei) as opcao_mei,
      max(opcao_simples) as opcao_simples
  from `dataplatform-prd.dados_abertos_receita.vw_mei`
  group by 1
)

, empresas as (
  select 
      left(cpf_cnpj,8) as cpf_cnpj_raiz,
      max(capital_social) as capital_social,
      max(porte_da_empresa) as porte_da_empresa,
      max(natureza_juridica) as natureza_juridica
  from `dataplatform-prd.dados_abertos_receita.vw_empresas`
  group by 1
)

, portes as (
  select
    cpf_cnpj_raiz,
    case when mm.opcao_mei is null then 'N' else mm.opcao_mei end as mei,
    porte_da_empresa,
    natureza_juridica
  from empresas
  left join mei mm using(cpf_cnpj_raiz)
  qualify row_number() over (partition by cpf_cnpj_raiz) = 1
)

, porte_receita as (
  select 
    cpf_cnpj_raiz,
    case
      when porte = 'Demais' and natureza_juridica not in (2046,2054,2062,2070,2089,2135,2232,2240,2259,2267)
      then 'Demais: Exceções' 
    else porte end as porte
  from (
    select
      cpf_cnpj_raiz,
      case 
        when porte_da_empresa = 1 and mei = 'N' then 'ME'
        when porte_da_empresa = 3 and mei = 'N' then 'EPP'
        when porte_da_empresa = 5 and mei = 'N' then 'Demais'
      else 'MEI' end as porte,
      natureza_juridica
    from portes rec
  )
)


, aux_final as (
select distinct 
reference_date,
tam_id, 
trim(coalesce(e.cpf_cnpj, aux.document)) cpf_cnpj, --# corrigir document=cpf_cnpj_raiz
max(merchant_market_hierarchy_id) over(partition by tam_id, reference_date) merchant_market_hierarchy_id,
cnae_fiscal_principal,
max(coalesce(c.mcc, d.mcc, mcc_places)) over(partition by tam_id, reference_date) mcc, 
source_document source_cpf_cnpj,
cod_muni,
merchant_name,
min(first_seen_week) over(partition by tam_id, reference_date) first_seen_week,

from init_table aux
left join `dataplatform-prd.dados_abertos_receita.vw_estabelecimentos` b on aux.document=b.cpf_cnpj
left join  `dataplatform-prd.dados_abertos_receita.cnae_mcc` c on b.cnae_fiscal_principal=CAST(c.codigo_cnae as INT)
left join mcc_interno d on aux.document=d.document
left join docs_receita e on aux.document=e.cpf_cnpj_raiz
)

, final as (
select distinct
reference_date,
tam_id,

coalesce(cpf_cnpj_raiz, concat('tam_id_', CAST(tam_id as STRING))) id_empresa,
cpf_cnpj,
cpf_cnpj_raiz,
case when LENGTH(cpf_cnpj)=14 then 'CNPJ'
     when LENGTH(cpf_cnpj)=11 then 'CPF'
     else null end document_type,
merchant_name, 
merchant_market_hierarchy_id,
a.mcc,
coalesce(CAST(cnae_fiscal_principal as STRING), codigo_cnae) cnae, 
porte porte_rfb,
first_seen_week,
e.tam_id is not null is_online,
cod_muni,
--source_cpf_cnpj,
from (
select *,
IF(length(cpf_cnpj) = 14, left(cpf_cnpj,8), cpf_cnpj) as cpf_cnpj_raiz,
from aux_final 
) a
left join (select distinct cpf_cnpj_raiz, porte from porte_receita
qualify row_number() over(partition by cpf_cnpj_raiz)=1
) b using(cpf_cnpj_raiz)
left join (
  select distinct codigo_cnae, mcc from `dataplatform-prd.dados_abertos_receita.cnae_mcc2`
  qualify row_number() over(partition by mcc) = 1
  ) c using(mcc)
left join (select distinct tam_id, reference_date from `dataplatform-prd.master_contact.v2_aux_tam_presenca_online`) e using(tam_id, reference_date)
)



select * from final