
DECLARE ref_month  DATE DEFAULT '{{ref_month}}';


CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.v2_aux_tam_get_document_master_simple` PARTITION BY reference_month AS
--INSERT INTO `dataplatform-prd.master_contact.v2_aux_tam_get_document_master`


-- Cria tabela que determina o documento a partir do nome do estabelecimento e do 'cod_muni'
-- Tenta fazer o match pelo CPF e pelo CNPJ
-- No caso do CPF procura no municipio e nos municipios adjacentes
-- No caso do CNPJ procura no municipio
-- Lida com o fato de que os nomes podem vir truncados



with names_city_master as (
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
TRIM(REGEXP_REPLACE(nome_limpo, r'^SHOPEE|IFOOD', '')) AS nome,
cod_muni,
REPLACE(REGEXP_extract(TRIM(REGEXP_REPLACE(original_name_split,  r'[.,\-+]', '')) , r'^[0-9 ]+'), ' ', '') numero_inicio,
from `dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city` a
left join acquirer_get_issue b on TRIM(a.de42_merchant_id) = TRIM(b.de42_merchant_id) and a.reference_month = b.reference_month
where 1=1
and a.reference_month = ref_month
--and cod_muni in ( 2607901, 2406908,3129806,3305208,4214151,4202057,3302502,4110607,1302603,3549409,2602407,2807204,3555406,3502101,4128104, 4305355,3200169,1303304,3152006,2704302) 
and subs_asterisk not in ('Ton')
and not starts_with(nome_limpo, 'LOTERIASONLINE')
and not starts_with(nome_limpo, 'FACEBOOK')
and not starts_with(nome_limpo, 'PIC PAY')
and not starts_with(nome_limpo, 'PICPAY')
and not (subs_asterisk='Outros' and RIGHT(a.de42_merchant_id, 6) = '000000') # Stone e TON
and b.de42_merchant_id is null ---# Gambiarra para não pegar os mmhids Mercado Pago que estão com problemas (crescimento muito grande)
)
where  LENGTH(nome) > 5
)


, names_city_drop_duplicates as (

select distinct 
nome_merge, len_nome_merge, cod_muni, reference_month,
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



, aux_final as (
select
reference_month,
a.nome nome_master,
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
)



, final as (
select *, 
IF((cpf is not null or cnpj is not null), TRUE, FALSE) bom_resultado,
IF(((cpf is not null or cnpj is not null)  or ( cpf_brasil is not null)) , TRUE, FALSE) bom_resultado_brasil,
from (
select * except(nome_cpf_brasil, cpf_brasil, qtd_nome_cpf_brasil, qtd_muni_cpf_brasil), 
IF(qtd_nome_cpf_brasil<=5 and qtd_muni_cpf_brasil=1, nome_cpf_brasil, null) nome_cpf_brasil,
IF(qtd_nome_cpf_brasil<=5 and qtd_muni_cpf_brasil=1, cpf_brasil, null) cpf_brasil,
from aux_final
)
)

select * 
from final
where reference_month = ref_month


