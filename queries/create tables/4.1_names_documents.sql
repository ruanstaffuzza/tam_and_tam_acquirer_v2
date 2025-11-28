CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.names_documents` 
AS

with munis_interesse as (
select distinct 
cod_muni
from `dataplatform-prd.economic_research.geo_muni_2021`
)


-------- ################################### ------------------- 
-- Lida com muncipipios e vizinhança
, vizinhanca_muni_aux as (
SELECT 
CAST(id_municipio_1 as INT) cod_muni,
CAST(id_municipio_2 as INT) cod_muni_vizinho,
FROM `basedosdados.br_bd_vizinhanca.municipio`
where ano=2020
)


-- # Cria vizinhança de municípios (incluindo o próprio município como vizinho)
, vizinhanca_muni as (
select * from vizinhanca_muni_aux
union distinct
select distinct cod_muni, cod_muni cod_muni_vizinho from vizinhanca_muni_aux
)


-- Lista todos os municipios de interesse
, munis_interesse_cpf as (
select distinct b.cod_muni_vizinho, b.cod_muni,
from (select distinct cod_muni from munis_interesse) a
inner join vizinhanca_muni b on a.cod_muni=b.cod_muni
)


--# Cria lista CPFs por municipio, várias fontes distintas
, nomes_cpf_pix as (
select distinct 
source_document cpf,
sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(UPPER(source_name)) nome, 
cod_muni,
from `dataplatform-prd.master_contact.aux_tam_cpfs_by_city` 
where source_name is not null
)


, nomes_cpf_parentes as (
SELECT  
cpf_cnpj_parente as cpf, 
sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(UPPER(h.nome_parente)) nome,
null cod_muni,
FROM `dataplatform-prd.master_contact.hubble_parentes` h 
left join (select distinct cpf from nomes_cpf_pix) p on h.cpf_cnpj_parente = p.cpf
WHERE 1=1
and obito_parente = 'NAO' 
and p.cpf is null 
and cpf_cnpj_parente is not null 
and nome_parente is not null
qualify row_number() over(partition by cpf_cnpj_parente order by length(nome_parente) desc)=1
)





-------- ################################### ------------------- 
--# Interage com tabela de municipios vizinhos
, nomes_cpf_pix_vizinho as (
select distinct 
cpf,
nome, 
a.cod_muni, -- # Todas as opções de municípios (vizinhos)
from munis_interesse_cpf a
left join nomes_cpf_pix b on a.cod_muni_vizinho=b.cod_muni
)



, nome_cnpj as (

with aux_razao_social as (
select distinct
emp.cpf_cnpj cnpj,
TRIM(REGEXP_REPLACE(sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(UPPER(razao_social)), r'[^A-Z ]', '')) nome,
municipio cod_muni,
estab.situacao_cadastral,
from (select distinct cod_muni municipio from munis_interesse)
inner join `dataplatform-prd.dados_abertos_receita.vw_estabelecimentos` estab using(municipio)
left join `dataplatform-prd.dados_abertos_receita.vw_empresas` emp on left(estab.cpf_cnpj,8) = left(emp.cpf_cnpj,8)
where razao_social is not null
--and situacao_cadastral = 2
)

, aux_nome_fantasia as (
select distinct
--emp.cpf_cnpj cnpj,
cpf_cnpj cnpj, 
TRIM(REGEXP_REPLACE(sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(UPPER(nome_fantasia)), r'[^A-Z ]', '')) nome, 
municipio cod_muni,
estab.situacao_cadastral,
from (select distinct cod_muni municipio from munis_interesse)
inner join  `dataplatform-prd.dados_abertos_receita.vw_estabelecimentos` estab using(municipio)
--left join `dataplatform-prd.dados_abertos_receita.vw_empresas` emp on left(estab.cpf_cnpj,8) = left(emp.cpf_cnpj,8)
where nome_fantasia is not null
--and situacao_cadastral = 2
)


select *, 'razão social' tipo_nome from aux_razao_social
union distinct
select *, 'nome fantasia' tipo_nome from aux_nome_fantasia
)


-- Lista única de nomes e documentos

select distinct *,
replace(nome, ' ', '') nome_merge,
 from (
select cod_muni, nome, cpf, null cnpj, 'pf' tipo_nome, TRUE situacao_cadastral_ativo from nomes_cpf_pix_vizinho
union all
select cod_muni, nome, cpf, null cnpj, 'pf' tipo_nome, TRUE situacao_cadastral_ativo from nomes_cpf_parentes
union all
select cod_muni, nome, null cpf, cnpj, tipo_nome, situacao_cadastral=2 situacao_cadastral_ativo from nome_cnpj
)
