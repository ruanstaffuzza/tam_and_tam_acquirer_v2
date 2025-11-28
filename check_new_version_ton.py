#%%
import pandas as pd
import numpy as np
import os
from utils import read_data, save_query, download_data

query_str2 = """
select * from `dataplatform-prd.master_contact.aux_tam_final_nomes_v2`
where reference_month = '2025-03-31'
and subs_asterisk = 'Ton'
"""

query_str1 = """
select * from `dataplatform-prd.master_contact.aux_tam_final_nomes`
where reference_month = '2025-03-31'
and subs_asterisk = 'Ton'
"""

query_name2 = 'final_nomes_ton_v2'
query_name1 = 'final_nomes_ton_v1'

save_query(query_name1, query_str1)
save_query(query_name2, query_str2)

download_data(query_name1)
download_data(query_name2)

#%%

df1 = read_data(query_name1)
df2 = read_data(query_name2)

from IPython.display import display
display(df1)

display(df2)

#%%

query_str3 = """
DECLARE ref_month  DATE DEFAULT '2025-03-31';

with aux as (
select distinct
reference_month, 
merchant_market_hierarchy_id, 
de42_merchant_id,
CAST(cod_muni as STRING) cod_muni,
original_name,
from `dataplatform-prd.master_contact.aux_tam_subs_asterisk_city_v2` a
where 1=1
and subs_asterisk in ('Ton') 
and RIGHT(de42_merchant_id, 6) = '000000'
and reference_month = ref_month
)

, big_mmhid as (
  select * from `dataplatform-prd.master_contact.aux_tam_big_mmhid`
)

select 
reference_month,
--merchant_market_hierarchy_id,
count(*),
count(distinct de42_merchant_id),
count(distinct original_name),
count(distinct concat(original_name, cod_muni)),
count(distinct concat(original_name, cod_muni, de42_merchant_id)),
from aux
left join big_mmhid b using(merchant_market_hierarchy_id, reference_month)
where b.reference_month is null


group by 1
--order by 3 desc
    
    """