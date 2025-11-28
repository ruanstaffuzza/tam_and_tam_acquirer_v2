--DECLARE ref_month  DATE DEFAULT '2025-04-30';

CREATE OR REPLACE TABLE  `dataplatform-prd.master_contact.aux_tam_subs_asterisk_delete_analysis` PARTITION BY reference_month AS


--INSERT INTO `dataplatform-prd.master_contact.aux_tam_subs_asterisk_vMP`

-- Cria tabela que determina um 'cod_muni' para cada entrada na tabela de 'mid_de42' 
-- Usa basicamente 5 técnicas:
-- 1. Municipio direto da Places
-- 2. Cidade = Municipio do IBGE
-- 3. Cidade = Distrito/Municipio truncado do IBGE
-- 4. Dicionário de cidades (a partir dos dados da 'places')
-- 5. Cidade = Municipio do IBGE (cortanto letras com acentos) e truncado


WITH valid_ufs AS (
  SELECT valid_uf
  FROM UNNEST(['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 
    'TO']) AS valid_uf
)


, aux001 as (
SELECT *
from `dataplatform-prd.master_contact.aux_tam_subs_asterisk_city_v2`  
where 1=1
--and reference_month = ref_month
and subs_asterisk = 'delete_asterisk'
)

SELECT distinct
DATE(reference_month) as reference_month,
merchant_market_hierarchy_id,
nome_limpo,
numero_inicio,
subs_asterisk,
-- Identificação do tipo de descriptor
CASE 
     WHEN (STARTS_WITH(merchant_descriptor, 'PAG*') or (LENGTH(de42_merchant_id)=11 and de42_merchant_id LIKE '0%' )) then 'PagSeguro'
     WHEN STARTS_WITH(merchant_descriptor, 'EC*') then 'EC' 
     WHEN STARTS_WITH(merchant_descriptor, 'PG*TON') then 'Ton' -- Será incluído de outra forma depois
     WHEN STARTS_WITH(merchant_descriptor, 'PG*') then 'Pagarme' -- Será incluído de outra forma depois
     WHEN STARTS_WITH(merchant_descriptor, 'SUMUP*') then 'SumUp'
     WHEN REGEXP_CONTAINS(merchant_descriptor, r'^(IFOOD|IFD|IF|CRV*IFD)\*') then 'Ifood'
     --WHEN reference_month >= '2025-05-31' and merchant_market_hierarchy_id in (1053988581, 785135901) then 'MercadoPago_mmhid_problema' -- Gambiarra para não pegar os mmhids Mercado Pago que estão com problemas (crescimento muito grande)
     


    WHEN reference_month >= '2025-05-31' and merchant_market_hierarchy_id in (1053988581, 785135901) and STARTS_WITH(merchant_descriptor, 'MERCADOLIVR') then 'MercadoLivre1_mmhid_problema' 
    WHEN reference_month >= '2025-05-31' and merchant_market_hierarchy_id in (1053988581, 785135901) and STARTS_WITH(TRIM(merchant_descriptor), 'MERCADOPAGO*') then 'MercadoLivre2_mmhid_problema' 
    WHEN reference_month >= '2025-05-31' and merchant_market_hierarchy_id in (1053988581, 785135901) and ( 
        STARTS_WITH(merchant_descriptor, 'MP*')
        OR (STARTS_WITH(TRIM(de42_merchant_id), 'M') AND LENGTH(TRIM(de42_merchant_id)) = 15) 
        OR TRIM(de42_merchant_id) IN ('34', '31', '103', '29')
        OR TRIM(de42_merchant_id) IN ('148', '147', '000000007187449', '7187449', '138006')
     ) then 'MP_mmhid_problema' 

    WHEN STARTS_WITH(merchant_descriptor, 'MERCADOLIVR') then 'MercadoLivre1'
    WHEN STARTS_WITH(TRIM(merchant_descriptor), 'MERCADOPAGO*') then 'MercadoLivre2'

     --WHEN RIGHT(TRIM(de42_merchant_id), 6) = '000000' then 'Stone'  -- Stone
     --WHEN (LENGTH(TRIM(de42_merchant_id)) = 9 and TRIM(de42_merchant_id) LIKE '0%' ) THEN 'Rede'  -- Rede
     WHEN ( 
        STARTS_WITH(merchant_descriptor, 'MP*')
        OR (STARTS_WITH(TRIM(de42_merchant_id), 'M') AND LENGTH(TRIM(de42_merchant_id)) = 15) 
        OR TRIM(de42_merchant_id) IN ('34', '31', '103', '29')
        OR TRIM(de42_merchant_id) IN ('148', '147', '000000007187449', '7187449', '138006')
     ) then 'MercadoPago'


    WHEN reference_month >= '2025-05-31' and merchant_market_hierarchy_id in (1053988581, 785135901) then 'MercadoPago_mmhid_problema' 
    
           
    WHEN (STARTS_WITH(merchant_descriptor, 'SHOPEE*') OR CONTAINS_SUBSTR(merchant_descriptor, 'SHOPEE') 
        OR STARTS_WITH(merchant_descriptor, 'SHO*')) then 'Shopee'
      
    WHEN STARTS_WITH(merchant_descriptor, 'MLP*') then 'Magalu1'
    WHEN (STARTS_WITH(merchant_descriptor, 'MAGALU') OR STARTS_WITH(merchant_descriptor, 'MAGAZINELU')) then 'Magalu2'

    WHEN STARTS_WITH(merchant_descriptor, 'AMAZON.COM') OR STARTS_WITH(merchant_descriptor, 'AMAZONMARKETPLAC') OR
     STARTS_WITH(merchant_descriptor, 'AMAZONMKTPLC*') then 'Amazon'

    WHEN STARTS_WITH(merchant_descriptor, 'SHEIN*') OR REGEXP_CONTAINS(merchant_descriptor, r'SHEIN(COM|\.CO)')  then 'Shein'

    WHEN STARTS_WITH(merchant_descriptor, 'ALIEXPRESS') OR STARTS_WITH(merchant_descriptor, 'ALIPAY') then 'Aliexpress'

    WHEN STARTS_WITH(merchant_descriptor, 'AMERICANAS.COM') OR STARTS_WITH(merchant_descriptor, 'AMERICANASMARKETP') 
         OR merchant_descriptor = 'AMERICANAS' OR STARTS_WITH(merchant_descriptor, 'AMERICANAS*') then 'Americanas'

    WHEN STARTS_WITH(merchant_descriptor, 'RAPPI') then 'Rappi'

    WHEN STARTS_WITH(merchant_descriptor, '99FOOD') then '99Food'

    WHEN STARTS_WITH(merchant_descriptor, 'HTM*') OR STARTS_WITH(merchant_descriptor, 'HOTMART*') then 'Hotmart'

   WHEN STARTS_WITH(merchant_descriptor, 'ZP*') then 'Zoop'
   WHEN STARTS_WITH(merchant_descriptor, 'KIWIFY*') then 'Kiwify'
   WHEN STARTS_WITH(merchant_descriptor, 'MERCADO*') then 'MercadoLivre3'
   WHEN STARTS_WITH(merchant_descriptor, 'PAYGO*') OR STARTS_WITH(merchant_descriptor, 'C6PAY*') then 'C6 Pay'
   WHEN STARTS_WITH(merchant_descriptor, 'ASA*') or STARTS_WITH(merchant_descriptor, 'ASAASIP*') 
    OR STARTS_WITH(merchant_descriptor, 'ASAAS*') OR STARTS_WITH(merchant_descriptor, 'ASAASGESTAO*') then 'Asaas'
   WHEN STARTS_WITH(merchant_descriptor, 'IUGU*') then 'Iugu'
   WHEN STARTS_WITH(merchant_descriptor, 'LISTO*') then 'Listo'
   WHEN STARTS_WITH(merchant_descriptor, 'VINDI*') then 'Vindi'
   WHEN STARTS_WITH(merchant_descriptor, 'EVO*') then 'Evoluservices'
   WHEN STARTS_WITH(merchant_descriptor, 'ADIQPLU*') then 'Adiqplus'
   WHEN STARTS_WITH(merchant_descriptor, 'CAPPTA*') then 'Cappta'
   WHEN STARTS_WITH(merchant_descriptor, 'PINPAG*') then 'PinPag'
   WHEN STARTS_WITH(merchant_descriptor, 'PAGUEVELOZ*') or STARTS_WITH(merchant_descriptor, 'PGZ*') then 'Pague Veloz'

    WHEN  CONTAINS_SUBSTR(merchant_descriptor, '*') 
           OR CONTAINS_SUBSTR(merchant_descriptor, "LINK-")   
           then 'delete_asterisk' -- Não é de interesse
     ELSE 'Outros' END as subs_asterisk2, -- Tipo de cliente padrão, que está na places, normal. 

FROM aux001 a