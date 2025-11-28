
CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.temp_ruan_tpv`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
) AS


SELECT distinct 
document, company, 
reference_date,
affiliation_id, 
FROM `dataplatform-treated-prod.cross_company.payments_cards__tpv` AS ft
WHERE 1=1
  AND ft.company in ('STONE', 'TON')
  and ft.reference_date between '2024-10-01' and '2025-04-30'
  and tpv>0



CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.temp_ruan_tpv`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
) AS


SELECT distinct 
document, company, 
LAST_DAY(reference_date, month) reference_date,
FROM `dataplatform-treated-prod.cross_company.payments_cards__tpv` AS ft
WHERE 1=1
  and ft.reference_date between '2025-01-01' and '2025-08-31'
  and tpv>0