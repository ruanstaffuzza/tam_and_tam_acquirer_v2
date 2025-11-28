
DECLARE ref_month DATE DEFAULT '2025-03-31';

CREATE TABLE `dataplatform-prd.master_contact.temp_ruan_clients_ton`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 240 HOUR) -- 10 days
) AS


-- Cria uma lista distinta de códigos postais e códigos de município,
with aux_cep AS (
select
cep,
cod_muni,
from `dataplatform-prd.economic_research.geo_cep`
)


, aux_mp as (
SELECT 
    ref_month reference_month,
    Transaction.TransactionMerchant.TaxId,
    `Transaction`.TransactionMerchant.Name as original_name,
    Transaction.TransactionMerchant.StreetAddress,
    Transaction.TransactionMerchant.PostalCode cep,  
    Transaction.TransactionMerchant.City,
    Transaction.TransactionMerchant.CountrySubdivisionCode,  
    COUNT(DISTINCT `Transaction`.TransactionKey) as qtd_trx,
    SUM(`Transaction`.Amount.Merchant.Authorized.Amount) as total_tpv,
FROM `dataplatform-prd.transactional_aut_open.transaction_notifier_v2` trx 
WHERE OperationType = "Authorization" -- Tipo de operação Authorization
  AND Transaction.SchemeBusinessModel = "FullAcquirer" -- Modelo Full Acquirer
  AND Operation.ResponseCode = "0000" -- Confirmado pelo arranjo de pagamento
  AND Transaction.IsCaptured = True -- Confirmado pelo estabelecimento comercial
  AND Transaction.Amount.AuthorizedState = 'Full' -- Totalmente autorizado
  AND ( `Transaction`.TransactionMerchant.Name LIKE 'PG *TON %')-- Apenas clientes TON
AND DATE(Timestamp) between DATE_ADD(DATE_TRUNC(ref_month, month), interval -1 month) and ref_month
GROUP BY 1,2,3,4,5,6,7
)

, aux_final as (
select
*,
TRIM(sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(REGEXP_REPLACE(
UPPER(
  IF(REGEXP_CONTAINS(original_name, r'[*]'), SPLIT(original_name, '*')[OFFSET(1)], original_name)
)
,  r'[0-9.,\-+]', ''))) nome_limpo,
from aux_mp
left join aux_cep using(cep)
)


SELECT distinct * from aux_final