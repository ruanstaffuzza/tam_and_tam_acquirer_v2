
CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.temp_ruan_tpv_sample` AS

select distinct document
from `dataplatform-prd.master_contact.temp_ruan_tpv`
order by rand()
limit 100000
