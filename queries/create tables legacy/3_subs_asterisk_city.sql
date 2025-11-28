--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.aux_tam_subs_asterisk_city` PARTITION BY reference_month AS

INSERT INTO `dataplatform-prd.master_contact.aux_tam_subs_asterisk_city`

-- Cria tabela que determina um 'cod_muni' para cada entrada na tabela de 'mid_de42' 
-- Usa basicamente 5 técnicas:
-- 1. Municipio direto da Places
-- 2. Cidade = Municipio do IBGE
-- 3. Cidade = Distrito/Municipio truncado do IBGE
-- 4. Dicionário de cidades (a partir dos dados da 'places')
-- 5. Cidade = Municipio do IBGE (cortanto letras com acentos) e truncado

with city_places as (
select distinct
reference_month,
merchant_market_hierarchy_id,
CAST(cod_muni_cidade as INT) cod_muni, 
from `dataplatform-prd.economic_research.geo_places`
where reference_month='{{ref_month}}'
)


, aux as (
SELECT 
reference_month,
merchant_market_hierarchy_id,
a.de42_merchant_id,
a.merchant_descriptor,
a.original_name,
a.city_limpo, -- original_city limpa
a.state_limpo, -- original_state limpo
a.subs_asterisk, -- classificação tipo cliente
a.qtd_cidades_distintas, 
b.cod_muni,
IF(REGEXP_CONTAINS(original_name, r'[*]'), SPLIT(original_name, '*')[OFFSET(1)], original_name) original_name_split
from  `dataplatform-prd.master_contact.aux_tam_subs_asterisk` a 
left join city_places b using(merchant_market_hierarchy_id, reference_month)
where 1=1
and reference_month='{{ref_month}}'
)

----------- ########################################### -------------
-- 2. Cidade = Municipio do IBGE
--# Tabela auxiliar com o nomes de municípios possíveis
, nomes_muni as (
select *,
count(*) over(partition by nome_muni) qtd_muni_mesmo_nome
from (
select 
nome nome_muni,
uf,
cod_muni
from `dataplatform-prd.economic_research.nomes_todos_distritos`
where muni
))


-- # Tabela auxiliar com municipios que não tem homônimos
, nome_muni_not_repeated as (
select 
nome_muni,
uf,
cod_muni,
from nomes_muni
where qtd_muni_mesmo_nome=1
)



----------- ########################################### -------------
-- 4. Dicionário de cidades (a partir dos dados da 'places')
-- Cria um dicionário de cidades usando informações da places
-- Consiste na identificação do munícipio com maior ocorrência para cada cidade (com e sem UF)
, aux_city1 as (
SELECT * 
from aux
where 1=1
and cod_muni is not null
qualify count(distinct city_limpo) over(partition by merchant_market_hierarchy_id, reference_month)=1 -- # usa apenas mmhid que só tem uma cidade (na mid_de42)
)


-- Vê qual é o munícipio mais comum para cada cidade
, aux_city as (
select 
  reference_month,
  city_limpo, 
  state_limpo,
  a.cod_muni,
  qtd_muni_mesmo_nome,
  count(*) qtd_aparicoes, 
from aux_city1 a
left join (select distinct cod_muni, qtd_muni_mesmo_nome from nomes_muni) b on a.cod_muni=b.cod_muni
group by 1, 2, 3, 4, 5
)

-- Municipio mais comum quando há UF
, dict_city as (
    select * from aux_city 
    where state_limpo is not null
    qualify row_number() over(partition by city_limpo, state_limpo, reference_month order by qtd_aparicoes desc)=1 
           )

-- Municipio mais comum quando não há UF
, dict_city_sem_uf as (
    select * from aux_city 
    where state_limpo is null
    qualify row_number() over(partition by city_limpo, reference_month order by qtd_aparicoes desc)=1 
           )


----------- ########################################### -------------
-- 3. Cidade = Distrito truncado do IBGE
, nomes_distrito as (
SELECT distinct 
nome nome_distrito, 
uf, 
cod_muni, 
count(distinct cod_muni) over(partition by nome) qtd_mesmo_nome_brasil,
count(distinct cod_muni) over(partition by nome, uf) qtd_mesmo_nome_uf,
from `dataplatform-prd.economic_research.nomes_todos_distritos`
)

, LengthSeries AS (
  SELECT num
  FROM UNNEST(GENERATE_ARRAY(5, 35)) AS num
)

, truncated_distrito AS (
  SELECT DISTINCT
    TRIM(LEFT(d.nome_distrito, ls.num)) AS nome_distrito_truncated,
    uf,
    cod_muni
  FROM
    nomes_distrito d
  CROSS JOIN
    LengthSeries ls
)

, final_distrito as (
SELECT *,
count(cod_muni) over(partition by nome_distrito_truncated) n_unique --# se=1, somente uma ocorrência no Brasil
FROM truncated_distrito
qualify count(cod_muni) over(partition by nome_distrito_truncated, uf) = 1 --# somente uma ocorrência por uf
)




----------- ########################################### -------------
-- 5. Cidade = Municipio do IBGE (cortanto letras com acentos) e truncado
-- Tabela auxiliar com nome de municipios sem os acentos. Ao invés de trocar 'SÃO' por 'SAO', troca por 'SO' ou por 'S O'
-- Isso ajuda com algumas identificações
, nomes_muni_missing_accents as (
SELECT distinct
REGEXP_REPLACE(UPPER(nome), r'[^A-Z ]', '') nome_muni,
CAST(id_municipio as INT) cod_muni,
sigla_uf uf,
from `basedosdados.br_bd_diretorios_brasil.municipio`

union distinct 

select distinct
TRIM(REGEXP_REPLACE(UPPER(nome), r'[^A-Z ]', ' ')) nome_muni,
CAST(id_municipio as INT) cod_muni,
sigla_uf uf,
from `basedosdados.br_bd_diretorios_brasil.municipio`
where REGEXP_CONTAINS(UPPER(nome), r'[^A-Z ]')
)


, trucated_muni_missing_accents as (
select *,
count(cod_muni) over(partition by nome_muni_truncated) n_unique
from (
select distinct 
trim(left(nome_muni, num)) nome_muni_truncated,
uf,
cod_muni
from nomes_muni_missing_accents cross join LengthSeries
)
qualify count(cod_muni) over(partition by nome_muni_truncated, uf) = 1
)



----------- ########################################### -------------
-- End
-- # Classificação dos casos e implementação da ordem desejada
, asterisk as (
select a.* except(cod_muni),
CASE  

     WHEN (qtd_cidades_distintas=1 or qtd_cidades_distintas is null) and not a.cod_muni is null then 'Pega municipio direto da Places - Só uma original_city por mmhid' 
     
     when muni.city_limpo is not null then 'Município IBGE - com UF'
     when muni_nr.city_limpo is not null then 'Município IBGE - sem UF - muni sem homônimo'
     when distrito.city_limpo is not null then 'Distrito ou muni truncado - com UF - distrito/muni sem homonimo na UF'
     when distrito_nr.city_limpo is not null then 'Distrito ou muni truncado - sem UF - distrito sem homonimo no Brasil'
     WHEN places.city_limpo is not null then 'Dicionário Places - com UF'
     

     WHEN muni_missing.city_limpo is not null then 'Município IBGE - com UF - removendo acentos'
     WHEN muni_missing_nr.city_limpo is not null then 'Município IBGE - sem UF - removendo acentos'
     when places2.city_limpo is not null then 'Dicionário Places - sem UF - municipio sem homônimo no Brasil'  --## Talvez seja melhor desconsiderar
     ELSE 'others' END as geo_case,
     coalesce(
      IF(a.qtd_cidades_distintas=1 or a.qtd_cidades_distintas is null, a.cod_muni, null), -- Pega municipio direto da Places - Só uma original_city por mmhid
      muni.cod_muni, -- Município IBGE - com UF
      muni_nr.cod_muni, -- Município IBGE - sem UF - muni sem homônimo
      distrito.cod_muni, -- Distrito ou muni truncado - com UF - distrito/muni sem homonimo na UF
      distrito_nr.cod_muni, -- Distrito ou muni truncado - sem UF - distrito/muni sem homonimo no Brasil
      places.cod_muni, -- Dicionário Places - com UF

      muni_missing.cod_muni, -- Município IBGE - com UF - removendo acentos
      muni_missing_nr.cod_muni, -- Município IBGE - sem UF - removendo acentos
      places2.cod_muni -- Dicionário Places - sem UF - municipio sem homônimo no Brasil   ## Talvez seja melhor desconsiderar
      ) cod_muni
FROM aux a
left join (select distinct nome_muni city_limpo, uf state_limpo, cod_muni from nomes_muni) muni using(city_limpo, state_limpo)
left join (select nome_muni city_limpo, cod_muni from nome_muni_not_repeated) muni_nr using(city_limpo)
left join (select distinct nome_distrito_truncated city_limpo, uf state_limpo, cod_muni from final_distrito) distrito using(city_limpo, state_limpo)
left join (select distinct nome_distrito_truncated city_limpo,                 cod_muni from final_distrito where n_unique=1) distrito_nr using(city_limpo)
left join (select distinct city_limpo, state_limpo, cod_muni, reference_month from dict_city ) places using(city_limpo, state_limpo, reference_month)
left join (select distinct city_limpo,              cod_muni, reference_month from dict_city_sem_uf where qtd_muni_mesmo_nome=1) places2 using(city_limpo, reference_month)
left join (select distinct nome_muni_truncated city_limpo, uf state_limpo, cod_muni from trucated_muni_missing_accents) muni_missing using(city_limpo, state_limpo)
left join (select distinct nome_muni_truncated city_limpo,                 cod_muni from trucated_muni_missing_accents where n_unique=1) muni_missing_nr using(city_limpo)
)

, aux_final as (
select * from asterisk
)

select distinct *,
TRIM(sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(REGEXP_REPLACE(TRIM(UPPER(original_name_split)),  r'[0-9.,\-+]', ''))) nome_limpo,
REPLACE(REGEXP_extract(TRIM(REGEXP_REPLACE(original_name_split,  r'[.,\-+]', '')) , r'^[0-9 ]+'), ' ', '') numero_inicio,
from aux_final