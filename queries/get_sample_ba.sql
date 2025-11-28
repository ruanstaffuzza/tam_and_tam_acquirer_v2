
SELECT distinct 
document, company, 
LAST_DAY(reference_date, month) as reference_date,
affiliation_id, 
from (select * from `dataplatform-prd.master_contact.temp_ruan_tpv_sample` limit 1000) 
inner join `dataplatform-prd.master_contact.temp_ruan_tpv` using (document)
