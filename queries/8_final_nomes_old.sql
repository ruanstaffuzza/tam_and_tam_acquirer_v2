CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.aux_tam_final_nomes` PARTITION BY reference_month AS 

--INSERT INTO `dataplatform-prd.master_contact.aux_tam_final_nomes`

with tax_id_places as (
  SELECT DISTINCT 
  reference_month,
    merchant_market_hierarchy_id,
    merchant_tax_id,
  FROM `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places`
    
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
nome_muni,
uf,
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
from  `dataplatform-prd.master_contact.aux_tam_get_document_master` a 

left join muni using(cod_muni)
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
--{{add_filter}}
)


, big_mmhid as (
  select * from `dataplatform-prd.master_contact.aux_tam_big_mmhid`
)


, final_sem_ton as (
select distinct * except(merchant_tax_id, merchant_market_hierarchy_id),
IF(subs_asterisk not in ('CloudWalk', 'PagSeguro', 'SumUp') and big_mmhid.merchant_market_hierarchy_id is null, merchant_tax_id, null) merchant_tax_id,
IF(subs_asterisk in ('Outros', 'Outros_Pags', 'Outros_SumUp') and big_mmhid.merchant_market_hierarchy_id is null, merchant_market_hierarchy_id, null) mmhid_merge,
IF(subs_asterisk not in ('CloudWalk', 'PagSeguro', 'SumUp') and big_mmhid.merchant_market_hierarchy_id is null , merchant_market_hierarchy_id, null) merchant_market_hierarchy_id
from aux1
left join big_mmhid using(merchant_market_hierarchy_id, reference_month)
qualify row_number() over(partition by subs_asterisk, nome_master, numero_inicio, cnpj, cpf, merchant_market_hierarchy_id, reference_month, cod_muni) =1
)



, active_base_ton as (
  SELECT distinct affiliation_id, document,
  reference_month,
  FROM `dataplatform-treated-prod.cross_company.payments_cards__tpv` AS ft
  CROSS JOIN (select distinct reference_month from final_sem_ton)
  WHERE 1=1
    AND ft.company = 'TON'
    and ft.reference_date between DATE_ADD(LAST_DAY(reference_month, MONTH), interval -29 day) and reference_month
    --AND LAST_DAY(ft.reference_date, MONTH) in (select distinct reference_month from final_sem_ton)
    and tpv>0
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
  inner join `dataplatform-treated-prod..payments__client_affiliation` dcl using(affiliation_id)
  left join nomes_cpf on dcl.document=nomes_cpf.document
  where dcl.company = 'TON'
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
document id_ton,
from clientes_ton
left join munis using(document)
)

, nome_master_df AS (
    SELECT 
        id_ton,
        document_ton,
        legal_name AS nome_master_com_espaco,
        REGEXP_REPLACE(legal_name, r'[^A-Z]', '') AS nome_master,
        cod_muni,  reference_month,
    FROM id_ton_df
    where LENGTH(REGEXP_REPLACE(legal_name, r'[^A-Z]', ''))>=5 
    UNION ALL
    SELECT 
        id_ton,
        document_ton,
        trade_name AS nome_master_com_espaco,
        REGEXP_REPLACE(trade_name, r'[^A-Z]', '') AS nome_master,
        cod_muni, reference_month,
    FROM id_ton_df
    where LENGTH(REGEXP_REPLACE(trade_name, r'[^A-Z]', ''))>=5 
)

, deduped_df AS (
    SELECT DISTINCT 
           id_ton,
           document_ton,
           nome_master,
           nome_master_com_espaco,
           cod_muni,
           reference_month,
    FROM nome_master_df
    qualify row_number() over(partition by id_ton, document_ton, nome_master, cod_muni, reference_month)=1
)


, final_ton as (
select * except(document_ton, nome_master),
case when COUNT(distinct document_ton) over(partition by nome_master, cod_muni, reference_month) > 1 then concat(nome_master, ' ', document_ton)
    else nome_master end nome_master,
case when length(document_ton)=11 then document_ton
    else null end cpf,
case when length(document_ton)=14 then document_ton
    else null end cnpj,
'Ton' AS subs_asterisk
from deduped_df
)



, aux_final as (
select
reference_month,
merchant_market_hierarchy_id,
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

UNION ALL

select
reference_month,
null merchant_market_hierarchy_id,
subs_asterisk,
nome_master_com_espaco,
nome_master,
null merchant_tax_id,
cpf,
null cpf_brasil,
cnpj,
null numero_inicio,
cod_muni,
id_ton,
from final_ton
)

select
* except(cod_muni),
coalesce(cod_muni, 9999) cod_muni,
LEFT(nome_master, 6) inicio,
from aux_final


