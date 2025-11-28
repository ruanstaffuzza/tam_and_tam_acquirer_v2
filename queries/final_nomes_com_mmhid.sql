CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.tmp_ruan_final_nomes_com_mmhid` 
PARTITION BY reference_month
OPTIONS(
  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
)

 AS 

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
from  `dataplatform-prd.master_contact.aux_tam_get_document_master` a 

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
and subs_asterisk in ('MercadoPago')
)


, big_mmhid as (
  select * from `dataplatform-prd.master_contact.aux_tam_big_mmhid`
)


, final_sem_ton as (
select distinct * except(merchant_tax_id, merchant_market_hierarchy_id),
IF(subs_asterisk not in ('CloudWalk', 'PagSeguro', 'SumUp') and big_mmhid.merchant_market_hierarchy_id is null, merchant_tax_id, null) merchant_tax_id,
IF(subs_asterisk not in ('CloudWalk', 'PagSeguro', 'SumUp') and big_mmhid.merchant_market_hierarchy_id is null , merchant_market_hierarchy_id, null) merchant_market_hierarchy_id,
merchant_market_hierarchy_id mmhid_orig, 
from aux1
left join big_mmhid using(merchant_market_hierarchy_id, reference_month)
qualify row_number() over(partition by subs_asterisk, nome_master, numero_inicio, cnpj, cpf, merchant_market_hierarchy_id, reference_month, cod_muni) =1
)






, aux_final as (
select
reference_month,
merchant_market_hierarchy_id,
mmhid_orig,
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

)

select
* except(cod_muni),
coalesce(cod_muni, 9999) cod_muni,
LEFT(nome_master, 6) inicio,
from aux_final


