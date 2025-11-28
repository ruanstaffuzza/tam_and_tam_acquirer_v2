CREATE OR REPLACE TABLE `dataplatform-prd.economic_research.nomes_todos_distritos` AS
-- Create or replace table with all possible distrito/municipality names


-- Combine unique municipality names from two sources
-- The reason to do this is that municipality names can be written in different ways in different sources: eg. 'Itapajé and 'Itapagé'
WITH nomes_muni AS (
  -- Select distinct municipality names from the first source
  -- Normalize accents and convert names to uppercase, replace '-' with ' '
  SELECT DISTINCT
    REPLACE(sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(UPPER(nome)), '-', ' ') AS nome_muni,
    CAST(id_municipio AS INT) AS cod_muni,
    sigla_uf AS uf
  FROM `basedosdados.br_bd_diretorios_brasil.municipio`

  UNION DISTINCT

  -- Select distinct municipality names from the second source; Names are already normalized
  SELECT DISTINCT 
    nome_muni,
    cod_muni,
    uf
  FROM `dataplatform-prd.economic_research.geo_muni_2021`
)


-- Combine unique district names from two sources
, aux_distrito1 AS (
  SELECT DISTINCT 
    sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(UPPER(nome_distrito)) AS nome_distrito,
    uf,
    cod_muni
  FROM `dataplatform-prd.economic_research.geo_subdistrito_2021`

  UNION DISTINCT

  SELECT DISTINCT 
    sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(UPPER(nome)) AS nome_distrito,
    sigla_uf AS uf,
    CAST(id_municipio AS INT) AS cod_muni 
  FROM `basedosdados.br_bd_diretorios_brasil.distrito`
  WHERE ano >= 2020
) 

-- Combine municipality and district names
, aux_final AS (
  SELECT DISTINCT  
    REPLACE(nome, '-', ' ') AS nome, -- Remove hyphens from names
    uf,
    cod_muni,
    muni
  FROM (
    SELECT 
        nome_muni AS nome, uf, cod_muni, 
        TRUE AS muni -- Flag to identify municipality names
    FROM nomes_muni
    UNION ALL
    SELECT nome_distrito AS nome, uf, cod_muni, FALSE AS muni FROM aux_distrito1
  )
)



-- Add names without apostrophes
SELECT DISTINCT nome, uf, cod_muni, muni
FROM aux_final

UNION DISTINCT

SELECT DISTINCT REPLACE(nome, "'", '') AS nome, uf, cod_muni, muni
FROM aux_final


/*
-- # Exemplos de queries --
-- nomes todos distritos e municipios
SELECT distinct nome, uf, cod_muni, 
count(distinct cod_muni) over(partition by nome) qtd_mesmo_nome_brasil,
count(distinct cod_muni) over(partition by nome, uf) qtd_mesmo_nome_uf,
from `dataplatform-prd.economic_research.nomes_todos_distritos`


-- nomes todos municipios
SELECT distinct nome, uf, cod_muni, 
count(*) over(partition by nome) qtd_mesmo_nome_brasil,
count(*) over(partition by nome, uf) qtd_mesmo_nome_uf
from `dataplatform-prd.economic_research.nomes_todos_distritos` where muni

*/
