--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.aux_tam_subs_asterisk` AS

INSERT INTO `dataplatform-prd.master_contact.aux_tam_subs_asterisk`


-- Cria tabela que: i) limpa o nome de cidades e estados; ii) classifica os estabelecimentos para ver se é subacquirer ou cliente normal


WITH valid_ufs AS (
  SELECT valid_uf
  FROM UNNEST(['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 
    'TO']) AS valid_uf
)


, aux as (
SELECT 
a.*,
REGEXP_REPLACE(TRIM(UPPER(SPLIT(original_city, '  ')[0])),  r'[0-9.,\-+*]', '') city_limpo, -- # limpa cidade
TRIM(COALESCE(
  IF(original_state='', null, original_state), 
  SPLIT(REGEXP_REPLACE(TRIM(original_city), r'\s{3,40}', '  '), '  ')[SAFE_OFFSET(1)],
  ''))   state_limpo, -- # Pega a melhor info de UF, do original_state ou da cidade
p.merchant_name,
merchant_tax_id,
from `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.mid_de42` a
left join `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places` p using(merchant_market_hierarchy_id, reference_month)
where 1=1
and reference_month = "{{ref_month}}"
-- # AND reference_month = '2024-04-30'
)


SELECT distinct
DATE(reference_month) as reference_month,
merchant_market_hierarchy_id,
de42_merchant_id,
merchant_descriptor,
original_name,
original_city,
original_state,
-- Identificação do tipo de descriptor
CASE WHEN STARTS_WITH(merchant_descriptor, 'EC*') then 'delete_asterisk' -- Não é de interesse
     WHEN STARTS_WITH(merchant_descriptor, 'PAG*') and merchant_name='PAGSEGURO' and count(distinct city_limpo) over(partition by merchant_market_hierarchy_id) >=1000     then 'PagSeguro' 
     WHEN STARTS_WITH(merchant_descriptor, 'PAG*') then 'Outros_Pags'
     WHEN STARTS_WITH(merchant_descriptor, 'PG*TON') then 'Ton' -- Será incluído de outra forma depois
     WHEN STARTS_WITH(merchant_descriptor, 'SUMUP*') and merchant_tax_id='16668076000120' and merchant_name='SUMUP.COM.BR' then 'SumUp' 
     WHEN STARTS_WITH(merchant_descriptor, 'SUMUP*') then 'Outros_SumUp'
     WHEN STARTS_WITH(merchant_name, 'CLOUDWALK') then 'CloudWalk'
     WHEN REGEXP_CONTAINS(merchant_descriptor, r'^(IFOOD|IFD|IF|CRV*IFD)\*') then 'Ifood'
     WHEN ( 
        STARTS_WITH(TRIM(merchant_descriptor), 'MP*') OR STARTS_WITH(TRIM(merchant_descriptor), 'MERCADOPAGO*')
        OR (STARTS_WITH(TRIM(de42_merchant_id), 'M') AND LENGTH(TRIM(de42_merchant_id)) = 15) 
        OR TRIM(de42_merchant_id) IN ('34', '31', '103', '29')
        OR TRIM(de42_merchant_id) IN ('148', '147', '000000007187449', '7187449', '138006')
     ) then 'MercadoPago'
     WHEN  REGEXP_CONTAINS(merchant_descriptor, r'\*') 
           OR REGEXP_CONTAINS(merchant_name, r'^APOIA-?SE')
           OR REGEXP_CONTAINS(merchant_name, r'^FACEBOOK')
           OR REGEXP_CONTAINS(merchant_name, r'^LOTERIASONLINE')
           OR REGEXP_CONTAINS(merchant_name, r'^PIC PAY')
                
     then 'delete_asterisk' -- Não é de interesse
     ELSE 'Outros' END as subs_asterisk, -- Tipo de cliente padrão, que está na places, normal. 
city_limpo,
IF(state_limpo in (select valid_uf from valid_ufs), state_limpo, null) state_limpo, -- Verifica se o estado é válido
count(distinct city_limpo) over(partition by merchant_market_hierarchy_id) qtd_cidades_distintas,
FROM aux a





