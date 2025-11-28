#%%
from utils import download_data, read_data, read_text, read_gbq_from_template



query_template = """
select
reference_date,
document_type,
acquirers,
porte_rfb,
merchant_market_hierarchy_id is not null as mmhid_not_null,
count(*) qtd_linhas,
count(distinct cpf_cnpj) qtd_docs,
from `dataplatform-prd.master_contact.aux_tam_final_tam`
inner join (select distinct reference_date, tam_id from `dataplatform-prd.master_contact.aux_tam_final_tam_acquirer` where acquirer='Ton') using (reference_date, tam_id)
left join (select reference_date, tam_id, ARRAY_AGG(DISTINCT acquirer ORDER BY acquirer) as acquirers,
from `dataplatform-prd.master_contact.aux_tam_final_tam_acquirer` where not acquirer in ('Fiserv', 'Adiq', 'Adyen', 'Ifood', 'Ton', 'Stone')
group by reference_date, tam_id
) c using (reference_date, tam_id)


where 1=1
and reference_date >= '2024-11-01'

group by 1, 2, 3, 4, 5

"""

df_orig = read_gbq_from_template(query_template)

#%%

df = df_orig.copy()
df = df.groupby(['reference_date', 
                 'mmhid_not_null',
                 #'document_type', 
                 #'mmhid_not_null', 'porte_rfb'
                 ], as_index=False).agg({'qtd_linhas': 'sum', 'qtd_docs': 'sum'})

metric = 'qtd_linhas'

df = df.pivot(index='reference_date', columns='mmhid_not_null', values=metric).reset_index()

df['Total'] = df[True] + df[False]
df['share_true'] = df[True] / df['Total'] * 100
df

#%%


df = df_orig.copy()
df['divide_com_pags'] = df['acquirers'].apply(lambda x: 'PagSeguro' in x)

bool_var = 'divide_com_pags'

df = df.groupby(['reference_date', 
                 bool_var,
                 #'document_type', 
                 #'mmhid_not_null', 'porte_rfb'
                 ], as_index=False).agg({'qtd_linhas': 'sum', 'qtd_docs': 'sum'})

metric = 'qtd_linhas'

df = df.pivot(index='reference_date', columns=bool_var, values=metric).reset_index()

df['Total'] = df[True] + df[False]
df['share_true'] = df[True] / df['Total'] * 100
df