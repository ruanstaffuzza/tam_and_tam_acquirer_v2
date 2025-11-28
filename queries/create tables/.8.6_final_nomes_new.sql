
DECLARE ref_month  DATE DEFAULT '{{ref_month}}';


--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.v3_aux_tam_final_nomes` PARTITION BY reference_month AS 

INSERT INTO `dataplatform-prd.master_contact.v3_aux_tam_final_nomes`

with tax_id_places as (
  SELECT DISTINCT 
  reference_month,
    merchant_market_hierarchy_id,
    merchant_tax_id,
  FROM `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places`
  where 1=1
  {{add_filter}}
  and reference_month = ref_month
)


, aux1 as (
select *
from (
select 
distinct
reference_month,
merchant_market_hierarchy_id,
subs_asterisk,
nome_master nome_master_com_espaco,
REGEXP_REPLACE(nome_master, r'[^A-Z]', '') nome_master,
--nome_muni,
--uf,
CASE WHEN merchant_tax_id in (
 '', 
 '10573521000191',-- # MP
 '16668076000120', -- # sumup
 '08561701000101', -- # PagSeguro
 '14380200000121', -- # Ifood
 '18727053000174', -- # Pagarme
 '03816413000137', -- # Pagueveloz
 '06308851000182' -- # MOOZ SOLUCOES FINANCEIRAS LTDA
 '01109184000438', -- # UOL Pags
 '21689483000153' -- # Bilheteria Digital
) 
OR b.cnpj is not null --# participantes homologados (subs e credenciadoras)
 then null else merchant_tax_id end merchant_tax_id,

cpf,
cpf_brasil,
--qtd_cpfs,
--qtd_cnpjs,
a.cnpj,
numero_inicio,
cod_muni,
from  `dataplatform-prd.master_contact.v2_aux_tam_get_document_master` a 

--left join muni using(cod_muni)
left join tax_id_places using(merchant_market_hierarchy_id, reference_month)

left join `dataplatform-prd.master_contact.participantes_homologados` b on merchant_tax_id=b.cnpj
)
where 1=1
and not starts_with(nome_master, 'PICPAY')
and not starts_with(nome_master, 'GOOGLE')
and not starts_with(nome_master_com_espaco, 'ZUL ')
and not starts_with(nome_master, 'GOLLIN')
and not starts_with(nome_master, 'DEEZER')
and not starts_with(nome_master, 'RECARGA')
and not starts_with(nome_master, 'OLXBRA')
and not starts_with(nome_master, 'GRUPOOLX')
and not starts_with(nome_master, 'AVIANCA')
and not starts_with(nome_master, 'ETSAMERICAN')
and not starts_with(nome_master, 'EMIRATES')
and not starts_with(nome_master, 'AZULLINHA')
and not starts_with(nome_master, 'SYMPLA')
{{add_filter}}
and reference_month = ref_month
)


, big_mmhid as (
  select * from `dataplatform-prd.master_contact.v2_aux_tam_big_mmhid`
)


, final_sem_ton as (
select distinct * except(merchant_tax_id),
IF(big_mmhid.merchant_market_hierarchy_id is null, merchant_tax_id, null) merchant_tax_id,
IF(big_mmhid.merchant_market_hierarchy_id is null, merchant_market_hierarchy_id, null) mmhid_merge,
from aux1
left join big_mmhid using(merchant_market_hierarchy_id, reference_month)
qualify row_number() over(partition by subs_asterisk, nome_master, numero_inicio, cnpj, cpf, merchant_market_hierarchy_id, reference_month, cod_muni) =1
)



, active_base_ton as (
  SELECT distinct 
  affiliation_id, 
  document,
  reference_month,
  company, 
  FROM  `dataplatform-prd.master_contact.v2_temp_cross_company` AS ft
  CROSS JOIN (select distinct reference_month from final_sem_ton)
  WHERE 1=1
    and ft.reference_date between DATE_ADD(LAST_DAY(reference_month, MONTH), interval -29 day) and reference_month
    --AND LAST_DAY(ft.reference_date, MONTH) in (select distinct reference_month from final_sem_ton)
    AND ft.company in ('TON', 'PAGARME')
)


, nomes_cpf as (
SELECT distinct 
cpf document,
TRIM(REGEXP_REPLACE(sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(upper(name)),  r'[0-9.,\-+]', '')) name
from (select distinct document cpf from active_base_ton where length(document)=11) a
inner join `dataplatform-prd.ton.vw_smuser_users` using(cpf)
where 1=1
and document_type='cpf'
and name is not null
)

, clientes_ton as (
  select distinct
  reference_month,
  company, 
        dcl.document,
        COALESCE(TRIM(REGEXP_REPLACE(
          IF(legal_name is null, null, 
          sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(upper(
          legal_name
          ))),  r'[0-9.,\-+]', '')),         
        name) legal_name,
        TRIM(REGEXP_REPLACE(
          IF(trade_name is null, null,
          sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(upper(trade_name))),  r'[0-9.,\-+]', '')) trade_name,
  from active_base_ton
  inner join `dataplatform-treated-prod.cross_company.payments__client_affiliation` dcl using(affiliation_id, company)
  left join nomes_cpf on dcl.document=nomes_cpf.document
  where dcl.company in ('TON', 'PAGARME')
)


, munis as (
select 
documento document, 
CAST(cod_muni_espacial AS INT) cod_muni,
from `dataplatform-prd.economic_research.geo_stoneco`
)

, id_ton_df as (
select * except(document),
document document_ton,
from clientes_ton
left join munis using(document)
)

, nome_master_df AS (
    SELECT 
        document_ton,
        legal_name AS nome_master_com_espaco,
        REGEXP_REPLACE(legal_name, r'[^A-Z]', '') AS nome_master,
        cod_muni,  reference_month,
        company, 
    FROM id_ton_df
    where LENGTH(REGEXP_REPLACE(legal_name, r'[^A-Z]', ''))>=5 
    UNION ALL
    SELECT 
        document_ton,
        trade_name AS nome_master_com_espaco,
        REGEXP_REPLACE(trade_name, r'[^A-Z]', '') AS nome_master,
        cod_muni, reference_month,
        company,
    FROM id_ton_df
    where LENGTH(REGEXP_REPLACE(trade_name, r'[^A-Z]', ''))>=5 
)

, deduped_df AS (
    SELECT DISTINCT 
           document_ton,
           nome_master,
           nome_master_com_espaco,
           cod_muni,
           reference_month,
           company, 
    FROM nome_master_df
    qualify row_number() over(partition by document_ton, nome_master, cod_muni, reference_month)=1
)


, almost_final_ton as (
select * except(nome_master),
case when COUNT(distinct document_ton) over(partition by nome_master, cod_muni, reference_month) > 1 then concat(nome_master, ' ', document_ton)
    else nome_master end nome_master,
case when length(document_ton)=11 then document_ton
    else null end cpf,
case when length(document_ton)=14 then document_ton
    else null end cnpj,
IF(company='TON', 'Ton', 'Pagarme') subs_asterisk,
from deduped_df
)



, aux_mmhid_ton as (
select distinct
document_auth document_ton, 
merchant_market_hierarchy_id,
reference_date reference_month,
from `dataplatform-prd.master_contact.v2_aux_tam_ton_to_mmhid`
where reference_date in (select distinct reference_month from final_sem_ton)
)

, final_ton as (
select distinct * except(merchant_tax_id),
IF(big_mmhid.merchant_market_hierarchy_id is null, merchant_tax_id, null) merchant_tax_id,
IF(big_mmhid.merchant_market_hierarchy_id is null, merchant_market_hierarchy_id, null) mmhid_merge,
from almost_final_ton
left join aux_mmhid_ton using(document_ton, reference_month)
left join big_mmhid using(merchant_market_hierarchy_id, reference_month)
left join tax_id_places using(merchant_market_hierarchy_id, reference_month)
)



, aux_final as (
select
reference_month,
merchant_market_hierarchy_id,
mmhid_merge,
subs_asterisk,
nome_master_com_espaco,
nome_master,
merchant_tax_id,
cpf,
cpf_brasil,
cnpj,
numero_inicio,
cod_muni,
null id_ton,
from final_sem_ton
where subs_asterisk <> 'Pagarme'

UNION ALL

select
reference_month,
merchant_market_hierarchy_id,
mmhid_merge,
subs_asterisk,
nome_master_com_espaco,
nome_master,
merchant_tax_id,
cpf,
null cpf_brasil,
cnpj,
null numero_inicio,
cod_muni,
null id_ton,
from final_ton
)


, your_table as (
select
* except(cod_muni),
coalesce(cod_muni, 9999) cod_muni,
LEFT(nome_master, 6) inicio,
subs_asterisk in ('MercadoLivre2', 'MercadoPago', 'MercadoPago_mmhid_problema',
       'MercadoPago_subPagarme'
       , 'MercadoLivre1'
       ) as is_mp,
coalesce(IF(subs_asterisk in ('Outros_Stone', 'Ton', 'Pagarme'), coalesce(cnpj, cpf), NULL), numero_inicio) as pre_doc_unmerge,
from aux_final
where 1=1
and not (reference_month >= '2025-04-30' and coalesce(merchant_market_hierarchy_id, 9999) = 1005409745) -- # excluir MMHID problematico de estacionamento que os descriptors parecem ser no nível da transacão
)


, base AS (
select
    *,
    CAST(mmhid_merge AS STRING) AS id_merge_places,
  FROM your_table
  where 1=1
  and inicio<>'SYMPLA'
)



select * except(id_merge_places),
 CASE
      WHEN subs_asterisk = 'Ton' AND id_merge_places IS NULL
        THEN 'Ton' || COALESCE(CAST(cpf AS STRING), CAST(cnpj AS STRING))
      
      ELSE id_merge_places
    END AS id_merge_places
from base