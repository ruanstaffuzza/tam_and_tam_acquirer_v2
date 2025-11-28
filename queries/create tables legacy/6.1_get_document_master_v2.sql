--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.aux_tam_get_document_master` AS
DECLARE ref_month  DATE DEFAULT '2025-03-31';

CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.aux_tam_get_document_master_v2` AS


-- Cria tabela que determina o documento a partir do nome do estabelecimento e do 'cod_muni'
-- Tenta fazer o match pelo CPF e pelo CNPJ
-- No caso do CPF procura no municipio e nos municipios adjacentes
-- No caso do CNPJ procura no municipio
-- Lida com o fato de que os nomes podem vir truncados



with acquirer_get_issue as (
select distinct 
DATE(reference_month) reference_month,
de42 de42_merchant_id,
from `dataplatform-prd.master_contact.aux_tam_pre_acquirer`
where adquirente_padroes = 'GetNet'
and REGEXP_CONTAINS(TRIM(de42), r'^00000001[0-8]')
and qtd_nomes_x_muni > 100 --# Issue GetNet
)


, names_city_master as (
select 
distinct
* except(numero_inicio),
REPLACE(nome, ' ', '') nome_merge, -- # Nome sem espaços
LENGTH(REPLACE(nome, ' ', '')) len_nome_merge, -- # Comprimento do nome sem espaços
if(length(numero_inicio) in (8, 14), numero_inicio, null) numero_inicio -- # número de 8 ou 14 dígitos na frente do nome
 from (
select 
distinct 
a.reference_month,
merchant_market_hierarchy_id, 
subs_asterisk,
REPLACE(nome_limpo,'IFOOD', '') nome,
cod_muni,
REPLACE(REGEXP_extract(TRIM(REGEXP_REPLACE(original_name_split,  r'[.,\-+]', '')) , r'^[0-9 ]+'), ' ', '') numero_inicio,
from `dataplatform-prd.master_contact.aux_tam_subs_asterisk_city_v2` a
left join acquirer_get_issue b on TRIM(a.de42_merchant_id) = TRIM(b.de42_merchant_id) and a.reference_month = b.reference_month
where 1=1
and a.reference_month = ref_month
and subs_asterisk not in ('delete_asterisk')
and not starts_with(nome_limpo, 'LOTERIASONLINE')
and not starts_with(nome_limpo, 'FACEBOOK')
and not starts_with(nome_limpo, 'PIC PAY')
and not starts_with(nome_limpo, 'PICPAY')
and not (subs_asterisk in ('Outros', 'Ton') and RIGHT(a.de42_merchant_id, 6) = '000000') # Stone
and b.de42_merchant_id is null
)
where  LENGTH(nome) > 5
)


, names_city_drop_duplicates as (
select distinct 
nome_merge, len_nome_merge, cod_muni 
from names_city_master
)


, munis_interesse as (
select distinct 
cod_muni
from names_city_master
)



-- Lista única de nomes e documentos
, final_docs as (
select distinct 
cod_muni, nome, cpf, cnpj, nome_merge
from `dataplatform-prd.master_contact.names_documents` a
left join `dataplatform-prd.master_contact.participantes_homologados` b using(cnpj)
where cod_muni in (select cod_muni from munis_interesse)
and b.cnpj is null
and situacao_cadastral_ativo
)


-------- ################################### -------------------
-- # Processo de truncamento
-- Tamanhos possíveis dos nomes
, len_keys as (
select distinct len_nome_merge from names_city_master
)


-- Faz um cross join como todos os tamanhos possíveis de nomes
-- E faz todos os truncamentos possíveis
, aux_truncated AS (
  SELECT DISTINCT
  cod_muni,
  nome,
  cpf,
  cnpj,
  LENGTH(TRIM(LEFT(d.nome_merge, ls.len_nome_merge))) as len_nome_merge,
  TRIM(LEFT(d.nome_merge, ls.len_nome_merge)) AS nome_merge_truncated,
  FROM
    final_docs d
  CROSS JOIN
    len_keys ls
  WHERE LENGTH(d.nome_merge) >= ls.len_nome_merge
)


-- Separa pf de pj
, truncated_pf as (
select
cod_muni,
cpf,
len_nome_merge,
nome_merge_truncated nome_merge,
nome nome_cpf,
count(*) over(partition by cod_muni, nome_merge_truncated) qtd_cpfs, -- # Quantidade de CPFs com o mesmo nome truncado
row_number() over(partition by cod_muni, nome_merge_truncated order by rand()) as rn
from aux_truncated
where cpf is not null
and cod_muni is not null
)

, trucated_pj as (
select
cod_muni,
cnpj,
len_nome_merge,
nome_merge_truncated nome_merge,
nome nome_cnpj,
count(distinct cnpj) over(partition by cod_muni, nome_merge_truncated) qtd_cnpjs, -- # Quantidade de CNPJs com o mesmo nome truncado
row_number() over(partition by cod_muni, nome_merge_truncated order by rand()) as rn
from aux_truncated
where cnpj is not null
)


, truncated_pf_brasil as (
select
cpf,
len_nome_merge,
nome_merge_truncated nome_merge,
nome nome_cpf,
from (
select distinct
cpf,
len_nome_merge,
nome_merge_truncated,
nome,
from aux_truncated
where cpf is not null
)
qualify count(*) over(partition by nome_merge_truncated) = 1 -- # Apenas nomes únicos no Brasil
)


, aux_mp as (
select
ref_month reference_month,
null merchant_market_hierarchy_id,
'MercadoPago_subPagarme' subs_asterisk,
CAST(null as STRING) numero_inicio,
nome_limpo nome_master_com_espaco,
REGEXP_REPLACE(nome_limpo, r'[^A-Z]', '') nome_master,
cod_muni, 
REPLACE(nome_limpo, ' ', '') nome_merge,
IF(length(TaxId) = 11, TaxId, null) cpf,
IF(length(TaxId) = 14, TaxId, null) cnpj,
CAST(null as STRING) nome_cpf,
CAST(null as STRING) nome_cnpj,
--IF(length(TaxId) = 11, 1, 0) qtd_cpfs,
--IF(length(TaxId) = 14, 1, 0) qtd_cnpjs,
CAST(null as STRING) nome_cpf_brasil,
CAST(null as STRING) cpf_brasil,
null qtd_nome_cpf_brasil,
null qtd_muni_cpf_brasil,
from `dataplatform-prd.master_contact.clients_mercado_pago`
where 1=1
and (reference_month = ref_month or reference_month = DATE_ADD(DATE_TRUNC(ref_month, MONTH), interval -1 day)) --# + 1 month: in mar/24 we also look for feb/24  mar - 1 month = feb
)



, names_city_master_stone as (
select 
distinct 
reference_month, 
merchant_market_hierarchy_id, 
de42_merchant_id,
cod_muni,
IF(subs_asterisk = 'Outros', 'Outros_Stone', subs_asterisk) subs_asterisk,
IF(subs_asterisk = 'Outros', 'STONE', 'TON') company,
from `dataplatform-prd.master_contact.aux_tam_subs_asterisk_city_v2` a
where 1=1
and subs_asterisk in ('Outros', 'Ton') 
and RIGHT(de42_merchant_id, 6) = '000000'
and reference_month = ref_month
)

, active_base_stone as (
  SELECT distinct  
  affiliation_id,
  ref_month reference_month,
  company,
  document,
  FROM `dataplatform-treated-prod.cross_company.payments_cards__tpv` AS ft
  WHERE 1=1
    AND ft.company in ('STONE', 'TON')
    --AND LAST_DAY(ft.reference_date, MONTH) in (select distinct reference_month from names_city_master)
    and ft.reference_date between DATE_ADD(LAST_DAY(ref_month, MONTH), interval -29 day) and ref_month
    and tpv>0
)



, nomes_cpf as (
SELECT distinct 
cpf document,
TRIM(REGEXP_REPLACE(sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(upper(name)),  r'[0-9.,\-+]', '')) name
from (select distinct document cpf from active_base_stone where length(document)=11 and company='TON') a
inner join `dataplatform-prd.ton.vw_smuser_users` using(cpf)
where 1=1
and document_type='cpf'
and name is not null
)

, clientes_stone as (
  select distinct
        reference_month,
        dcl.document,
        company, 
        RPAD(affiliation_id, 15, '0') AS de42_merchant_id,
        TRIM(sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(REGEXP_REPLACE(TRIM(UPPER(trade_name)),  r'[0-9.,\-+]', ''))) nome,
  from active_base_stone a
  inner join `dataplatform-treated-prod.cross_company.payments__client_affiliation` dcl using(affiliation_id, company)
  where a.company='STONE'
)



, clientes_ton as (
  select distinct
  reference_month,
        dcl.document,
        a.company,
        RPAD(affiliation_id, 15, '0') AS de42_merchant_id,
        COALESCE(TRIM(REGEXP_REPLACE(
          IF(legal_name is null, null, 
          sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(upper(
          legal_name
          ))),  r'[0-9.,\-+]', '')),         
        name) legal_name,
        TRIM(REGEXP_REPLACE(
          IF(trade_name is null, null,
          sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(upper(trade_name))),  r'[0-9.,\-+]', '')) trade_name,
  from active_base_stone a
  inner join `dataplatform-treated-prod.cross_company.payments__client_affiliation` dcl using(affiliation_id)
  left join nomes_cpf on dcl.document=nomes_cpf.document
  where a.company = 'TON'
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
    company,
        id_ton, de42_merchant_id,
        document_ton,
        legal_name AS nome_master_com_espaco,
        REGEXP_REPLACE(legal_name, r'[^A-Z]', '') AS nome_master,
        cod_muni,  reference_month,
    FROM id_ton_df
    where LENGTH(REGEXP_REPLACE(legal_name, r'[^A-Z]', ''))>=5 
    UNION ALL
    SELECT 
    company,
        id_ton, de42_merchant_id,
        document_ton,
        trade_name AS nome_master_com_espaco,
        REGEXP_REPLACE(trade_name, r'[^A-Z]', '') AS nome_master,
        cod_muni, reference_month,
    FROM id_ton_df
    where LENGTH(REGEXP_REPLACE(trade_name, r'[^A-Z]', ''))>=5 
)


, deduped_df AS (
    SELECT DISTINCT 
    company,
           id_ton, 
           de42_merchant_id,
           document_ton,
           nome_master,
           nome_master_com_espaco,
           cod_muni,
           reference_month,
           document_ton document,
    FROM nome_master_df
    qualify row_number() over(partition by id_ton,  de42_merchant_id, document_ton, nome_master, cod_muni, reference_month)=1
)


, aux_final as (
select
reference_month,
merchant_market_hierarchy_id,
subs_asterisk,
numero_inicio,
a.nome nome_master_com_espaco,
REGEXP_REPLACE(a.nome, r'[^A-Z]', '') nome_master,
cod_muni,
nome_merge,
cpf,
cnpj, 
nome_cpf,
nome_cnpj,
nome_cpf_brasil,
cpf_brasil,
count(distinct concat(nome, cast(cod_muni as STRING))) over(partition by cpf) qtd_nome_cpf_brasil,
count(distinct cod_muni) over(partition by cpf_brasil) qtd_muni_cpf_brasil,

 -- # Bom resultado é quando temos um CPF ou um CNPJ (e não temos mais de um CPF ou CNPJ)
from (
select * except(cpf, nome_cpf),
d.nome_cpf nome_cpf_brasil,
d.cpf cpf_brasil,
b.cpf,
b.nome_cpf,
from names_city_drop_duplicates a
left join (select * from truncated_pf where qtd_cpfs=1) b using(cod_muni, len_nome_merge, nome_merge) -- # Pega um CPF aleatório quando há mais de um (só a título de conferir)
left join (select * from trucated_pj where qtd_cnpjs=1) c using(cod_muni, len_nome_merge, nome_merge) -- # Pega um CNPJ aleatório quando há mais de um (só a título de conferir)
left join truncated_pf_brasil d using(len_nome_merge, nome_merge)
)
left join names_city_master a using(cod_muni, nome_merge, len_nome_merge)
union all

select * from aux_mp
where 1=1
and cod_muni in (select cod_muni from munis_interesse)
)




select distinct 
reference_month,
merchant_market_hierarchy_id,
subs_asterisk,
CAST(null as STRING) numero_inicio,
nome_master_com_espaco,
nome_master,
a.cod_muni,
REPLACE(nome_master_com_espaco, ' ', '') nome_merge,
IF(length(document) = 11, document, null) cpf,
IF(length(document) = 14, document, null) cnpj,
CAST(null as STRING) nome_cpf,
CAST(null as STRING) nome_cnpj,
CAST(null as STRING) nome_cpf_brasil,
CAST(null as STRING) cpf_brasil,
True bom_resultado,
True bom_resultado_brasil,
document,
from deduped_df a
left join names_city_master_stone using(de42_merchant_id, reference_month, company)


union all

select distinct 
reference_month,
merchant_market_hierarchy_id,
subs_asterisk,
CAST(null as STRING) numero_inicio,
a.nome nome_master_com_espaco,
REGEXP_REPLACE(a.nome, r'[^A-Z]', '') nome_master,
cod_muni,
REPLACE(nome, ' ', '') nome_merge,
IF(length(document) = 11, document, null) cpf,
IF(length(document) = 14, document, null) cnpj,
CAST(null as STRING) nome_cpf,
CAST(null as STRING) nome_cnpj,
CAST(null as STRING) nome_cpf_brasil,
CAST(null as STRING) cpf_brasil,
True bom_resultado,
True bom_resultado_brasil,
document,
from clientes_stone a
left join names_city_master_stone using(de42_merchant_id, reference_month, company)

union all



select *, 
IF((cpf is not null or cnpj is not null), TRUE, FALSE) bom_resultado,
IF(((cpf is not null or cnpj is not null)  or ( cpf_brasil is not null)) , TRUE, FALSE) bom_resultado_brasil,
null document, 
from (
select * except(nome_cpf_brasil, cpf_brasil, qtd_nome_cpf_brasil, qtd_muni_cpf_brasil), 
IF(qtd_nome_cpf_brasil<=5 and qtd_muni_cpf_brasil=1, nome_cpf_brasil, null) nome_cpf_brasil,
IF(qtd_nome_cpf_brasil<=5 and qtd_muni_cpf_brasil=1, cpf_brasil, null) cpf_brasil,
from aux_final
)



CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.aux_tam_final_nomes_v2` PARTITION BY reference_month AS 

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
nome_master_com_espaco,
nome_master,
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
coalesce(cpf,a.cnpj) document, 
cpf,
cpf_brasil,
--qtd_cpfs,
--qtd_cnpjs,
a.cnpj,
numero_inicio,
cod_muni,
from  `dataplatform-prd.master_contact.aux_tam_get_document_master_v2` a 
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





, aux_final as (
select
reference_month,
merchant_market_hierarchy_id,
subs_asterisk,
nome_master_com_espaco,
case when COUNT(distinct document) over(partition by nome_master, cod_muni, reference_month, subs_asterisk) > 1 and subs_asterisk = 'Ton' then concat(nome_master, ' ', document)
else nome_master end nome_master,
merchant_tax_id,
cpf,
cpf_brasil,
cnpj,
numero_inicio,
cod_muni,
null id_ton,
from final_sem_ton
)

select
* except(cod_muni),
coalesce(cod_muni, 9999) cod_muni,
LEFT(nome_master, 6) inicio,
from aux_final


