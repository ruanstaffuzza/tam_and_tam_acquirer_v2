
DECLARE ref_month  DATE DEFAULT '{{ref_month}}';

--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.v2_temp_cross_company`  PARTITION BY reference_date  CLUSTER BY company AS

delete from `dataplatform-prd.master_contact.v2_temp_cross_company` 
where reference_date between DATE_TRUNC(ref_month, MONTH) and ref_month
; 



INSERT INTO `dataplatform-prd.master_contact.v2_temp_cross_company`

select 
distinct reference_date, 
affiliation_id, 
document, 
company 
from `dataplatform-treated-prod.cross_company.payments_cards__tpv`
WHERE 1=1
and tpv>0
and reference_date between DATE_TRUNC(ref_month, MONTH) and ref_month
    