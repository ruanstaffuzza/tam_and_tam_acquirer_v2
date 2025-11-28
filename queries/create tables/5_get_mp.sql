--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.clients_mercado_pago` AS
DECLARE ref_month DATE DEFAULT '{{ref_month}}';
INSERT INTO `dataplatform-prd.master_contact.clients_mercado_pago`

-- Cria uma lista distinta de códigos postais e códigos de município,
with aux_cep AS (
select
cep,
cod_muni,
from `dataplatform-prd.economic_research.geo_cep`
)


, aux_mp as (
SELECT  distinct
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
  AND ( `Transaction`.TransactionMerchant.Name LIKE 'MP *%' AND Transaction.TransactionMerchant.TaxId <> '10573521000191' )-- Apenas clientes MP
 -- AND `Transaction`.Features.AccountDataEntryMode IN ('IccContactless', 'IccContact') -- Apenas transações com chip/presenciais
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


SELECT distinct reference_month, * except(reference_month) from aux_final
--UNION DISTINCT
--SELECT distinct 
--LAST_DAY(DATE_ADD(DATE_TRUNC(reference_month, MONTH), interval 1 month), MONTH) reference_month, --# + 1 month: in mar/24 we also look for feb/24  (feb + 1 month = mar)
--* except(reference_month) from aux_final
