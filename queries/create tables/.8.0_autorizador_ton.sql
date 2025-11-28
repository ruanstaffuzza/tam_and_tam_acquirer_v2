
DECLARE ref_month  DATE DEFAULT '{{ref_month}}';


--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.v2_temp_ruan_autorizador` PARTITION BY reference_month AS

INSERT INTO `dataplatform-prd.master_contact.v2_temp_ruan_autorizador`

SELECT distinct
Transaction.TransactionMerchant.TaxId as tax_id, 
`Transaction`.TransactionMerchant.Name, 
`Transaction`.TransactionMerchant.PostalCode,
Transaction.TransactionMerchant.City,
Transaction.TransactionMerchant.SubMerchantId smid,
JSON_EXTRACT_SCALAR(Operation.SchemeRequestMessage, '$.42') AS de42,
JSON_EXTRACT_SCALAR(Operation.SchemeRequestMessage, '$.43') AS de43,
`Transaction`.Installment.NumberOfInstallments>1 parc,
LAST_DAY(DATE(Timestamp)) as reference_month,
FROM `dataplatform-prd.transactional_aut_open.transaction_notifier_v2` trx 
WHERE OperationType = "Authorization" -- Tipo de operação Authorization
  AND Transaction.SchemeBusinessModel = "FullAcquirer" -- Modelo Full Acquirer
  AND Operation.ResponseCode = "0000" -- Confirmado pelo arranjo de pagamento
  AND Transaction.IsCaptured = True -- Confirmado pelo estabelecimento comercial
  AND Transaction.Amount.AuthorizedState = 'Full' -- Totalmente autorizado
  --AND NOT STARTS_WITH(UPPER(JSON_EXTRACT_SCALAR(Operation.SchemeRequestMessage, '$.43')),  'PARC=')
AND `Transaction`.Installment.NumberOfInstallments is null

--AND LAST_DAY(DATE(Timestamp), month) = ref_month -- Filtro antigo 18TB
and DATE(Timestamp) between DATE_TRUNC(ref_month, month) and ref_month -- Filtro novo 600GB



