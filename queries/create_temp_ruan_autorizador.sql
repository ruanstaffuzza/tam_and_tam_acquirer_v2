
DECLARE ref_month  DATE DEFAULT '2025-05-01';


INSERT `dataplatform-prd.master_contact.temp_ruan_autorizador` 


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
  AND DATE_TRUNC(DATE(Timestamp), month) = ref_month -- Data de referência


