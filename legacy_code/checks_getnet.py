
#%%


import pandas as pd
from utils import download_data, read_data, read_text, read_gbq_from_template
import os

import seaborn as sns
from matplotlib.ticker import EngFormatter
from matplotlib import pyplot as plt



def qtd_linhas_by(df, col, metric='qtd_linhas'):
    res = (
        df
        .groupby(['reference_date', col])[metric]
        .sum()
        .reset_index()
    )
    res['var'] = col
    res['var2'] = res[col]
    return res

def plot_point(df, col, metric, obs_title=''):
    dfg = qtd_linhas_by(df, col, metric=metric)
    dfg['str_ref_date'] = pd.to_datetime(dfg['reference_date']).dt.strftime('%m/%y')
    dfg = dfg.sort_values('reference_date')
    g = sns.catplot(data=dfg,
                    x='str_ref_date',
                    y=metric,
                    kind='point',
                    hue='var2',
                    aspect=1.7)
    g.set(xlabel='')
    # legend_title = col
    g._legend.set_title('')
    for ax in g.axes.flat:
        ax.yaxis.set_major_formatter(EngFormatter())
    plt.suptitle(col + ' ' + obs_title)
    plt.show()
#%%

query_template = """

with mid_de42 as (
select 
distinct 
reference_month,
merchant_market_hierarchy_id,
RPAD(TRIM(de42_merchant_id), 15, ' ') AS de42,
REGEXP_REPLACE(REPLACE(nome_limpo,'IFOOD', ''), r'[^A-Z]', '') nome_master,
 from `dataplatform-prd.master_contact.aux_tam_subs_asterisk_city`
where 1=1
and REGEXP_CONTAINS(TRIM(de42_merchant_id),  r'^00000001[0-8]')
)




, aux as (
select
de42,
reference_month,
count(*) qtd_linhas,
--count(distinct concat(merchant_market_hierarchy_id)) qtd_mmhid,
count(distinct nome_master) qtd_mmhid,
from mid_de42
group by 1, 2
)


select 
reference_month,
case when qtd_mmhid=1 then '1'
     when qtd_mmhid=2 then '2'
     when qtd_mmhid<5 then '3-4'
     when qtd_mmhid<10 then '5-9'
     when qtd_mmhid<20 then '10-19'
     when qtd_mmhid<50 then '20-49'
     else '50+' end qtd_mmhid,
count(*) qtd_linhas,
from aux
group by 1, 2
"""

df_orig_getnet = read_gbq_from_template(query_template)
#%%

df = df_orig_getnet.copy()
df['qtd_nomes'] = df['qtd_mmhid'].replace({
    '1': 'a. 1',
    '2': 'b. 2',
    '3-4': 'c. 3-4',
    '5-9': 'd. 5-9',
    '10-19': 'e. 10-19',
    '20-49': 'f. 20-49',
    '50+': 'g. 50+'
})

df = pd.concat([df, df.groupby('reference_month')['qtd_linhas'].sum().reset_index().assign(qtd_nomes='h. Total')])


df = df.pivot(index='reference_month', columns='qtd_nomes', values='qtd_linhas').fillna(0)
df
#%%

df = df_orig_getnet.copy()
df['count'] = 1
df['count'] = df.groupby(['de42', 'reference_month'])['count'].transform('sum')
df = df[df['count']>20]
df
#%%
import numpy as np
df = df_orig_getnet.copy()
df = df.sort_values('reference_month')
df['qtd'] = 1
df = df.groupby(['de42', 'reference_month']).agg({
    'merchant_market_hierarchy_id': lambda x: ','.join([str(x) for x in np.unique(sorted(x))]),
    'nome_master': lambda x: ','.join(sorted(x)),
    'qtd': 'sum'
}).reset_index()

#%%

df.groupby(['reference_month'])['qtd'].max().plot(kind='bar')




#%%
s = df['de42'].drop_duplicates().sample(1).iloc[0]
df[df['de42']==s]

#%%

df = df_orig_getnet.copy()
df['ref']=1
df.groupby('reference_month')['ref'].sum().plot(kind='bar')

#%%
df = df_orig_getnet.copy().rename(columns={'max_ref_month': 'min_ref_month'})

df['inicio'] = df['de42'].str[:9]
#df['min_ref_month'].value_counts().sort_index().plot(kind='bar')
#df.groupby('min_ref_month')['qtd_mmhid'].sum().plot(kind='bar')

df = df.groupby('inicio')['qtd_ref_month'].mean().sort_values(ascending=False)
df
#%%


df = df_orig_getnet.copy().rename(columns={'max_ref_month': 'min_ref_month'})

df['inicio'] = df['de42'].str[:11]

df = df.groupby('inicio').agg({
    'qtd_ref_month': 'max',
    'qtd_linhas': 'sum',
    'qtd_mmhid': 'sum',
    'min_ref_month': 'min',
    'de42': 'first'
}).reset_index()

df = df.sort_values('min_ref_month', ascending=False)

df[df['qtd_ref_month']==1]

#%%
df[df['qtd_ref_month']>4]

#%%

df = df_orig_getnet.copy()
df['reference_date'] = pd.to_datetime(df['reference_month'])
df = df[df['adquirente_padroes']=='GetNet']

df['getnet_padroes'] = df['getnet_padroes'].replace({
    'GetNet-1': '^00000001[0-8]',
    'GetNet-2': '^00000000[4-9]',
    'GetNet-3': '^000000001[4-9]',
})



plot_point(df, 'getnet_padroes', metric='qtd_mmhid', obs_title='- Qtd. Docs')

