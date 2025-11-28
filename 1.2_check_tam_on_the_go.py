#%%
import pandas as pd
from utils import download_data, read_data, read_text, read_gbq_from_template
import os

import seaborn as sns
from matplotlib.ticker import EngFormatter
from matplotlib import pyplot as plt

import pandas_gbq


from utils import high_contrast_color_palette

# function to get current date and time
def get_current_time():
    from datetime import datetime
    return "[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]"

def read_gbq_(query):
    project_id='sfwthr2a4shdyuogrt3jjtygj160rs'
    print(f'{get_current_time()} Getting dataset from BQ...')

    df = pandas_gbq.read_gbq(query, progress_bar_type='tqdm',
        project_id=project_id)
    

    if df is None:
        raise ValueError('No data returned from BQ query')
    
    print(f'{get_current_time()} Dataset successfully retrieved from BQ')
    print('Dataframe shape:', df.shape)
    return df

def read_gbq_from_template(template_query, dict_query=None):
    query = template_query
    if dict_query:
        from jinja2 import Template
        # Reads a query from a template and returns the query with the variables replaced
        # template_query: query as string, may use jinja2 templating
        # dict_query: dictionary of query parameters, to render from the template with jinja2
        query = Template(template_query).render(dict_query)
        print('Using dict_query')
        return read_gbq_(query)
    return read_gbq_(query)

def qtd_linhas_by(df, col, metric='qtd_linhas'):
    res = (
        df
        .groupby(['reference_date', 'version', col])[metric]
        .sum()
        .reset_index()
    )
    res['var'] = col
    res['var2'] = res[col]
    return res


def plot_point(df, col, metric, obs_title='',
               dodge=False,
               limit_0=False
               ):
    dfg = qtd_linhas_by(df, col, metric=metric)
    dfg['str_ref_date'] = pd.to_datetime(dfg['reference_date']).dt.strftime('%m/%y')
    dfg = dfg.sort_values('reference_date')
    g = sns.catplot(data=dfg,
                    x='str_ref_date',
                    y=metric,
                    kind='point',
                    hue='var2',
                    dodge=dodge,
                    palette=high_contrast_color_palette()[:len(dfg['var2'].unique())],
                    aspect=1.7)
    g.set(xlabel='')

    if limit_0:
        g.set(ylim=(0, None))



    # legend_title = col
    if g.legend is not None:
        g.legend.set_title('')
    for ax in g.axes.flat:
        ax.yaxis.set_major_formatter(EngFormatter())
    plt.suptitle(col + ' ' + obs_title, va='bottom', y=1)
    plt.show()
    return g
#%%
query_template = """
select
reference_month reference_date,
subs_asterisk,
geo_case, 
cod_muni=3534401 is_osasco,
merchant_market_hierarchy_id in (1053988581, 785135901) is_novo_mmhid,
count(*) qtd_linhas,
count(distinct concat(de42_merchant_id, merchant_descriptor)) qtd_des_de42,
count(distinct concat(de42_merchant_id, nome_limpo)) qtd_nome_de42,
count(distinct concat(nome_limpo, cod_muni)) qtd_nome_muni,
count(distinct merchant_descriptor) qtd_des,
count(distinct merchant_market_hierarchy_id) qtd_mmhid,
count(distinct nome_limpo) qtd_nome,
count(distinct LEFT(nome_limpo, 6)) qtd_nome_trunc,
count(distinct merchant_tax_id) qtd_tax_id,
from  `dataplatform-prd.master_contact.{{table_name}}`
left join `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places` p using(merchant_market_hierarchy_id, reference_month)
--where merchant_tax_id =''
where 1=1
and reference_month >= '2025-01-01'
group by 1, 2, 3, 4, 5
"""



#df_orig_subs_v1 = read_gbq_from_template(query_template, dict_query={'table_name': 'aux_tam_subs_asterisk_city_v2'})
df_orig_subs = read_gbq_from_template(query_template, dict_query={'table_name': 'v2_aux_tam_subs_asterisk_city'})



#%%

mp_asterisk = ['MP - Osasco', 'MercadoPago', 'MercadoLivre1', 'MercadoLivre2', 'MercadoLivre3']

df = df_orig_subs.assign(version='v2').copy()


df.loc[df['is_novo_mmhid'] 
       #& (df['subs_asterisk'] == 'MercadoPago')
       , 'subs_asterisk'] = 'MP - MMHID excluído'


df['reference_date'] = pd.to_datetime(df['reference_date'])
df = df[df['reference_date'] >= '2025-01-01']

df = df[df['subs_asterisk']!='delete_asterisk']
df = df[df['subs_asterisk']!='Outros']
#df = df[~df['is_osasco']]
#df = df[~df['is_novo_mmhid']]
#df = df[df['subs_asterisk']!='Outros_Pags']
df.loc[df['subs_asterisk'] == 'Outros_Pags', 'subs_asterisk'] = 'PagSeguro'
#df = df[df['subs_asterisk']!='MercadoPago']
#df['subs_asterisk'] = df['subs_asterisk'].replace('Outros_Pags', 'PagSeguro')
#df['subs_asterisk'] = df['subs_asterisk'].replace('Outros_SumUp', 'SumUp')


df = df[df['subs_asterisk']!='Stone']


df = df[df['subs_asterisk']!='Rede']
df = df[df['subs_asterisk']!='MP - MMHID excluído']

df.loc[df['is_osasco'] & (df['subs_asterisk'].isin(['MercadoPago'])), 'subs_asterisk'] = 'MP - Osasco'



df = df[df['subs_asterisk'].isin(mp_asterisk)]

df = pd.concat([
    df,
    df.assign(subs_asterisk='MercadoPago - Total')
])

print('--------------------------------------')
print('Plotting for subs_asterisk in:', mp_asterisk + ['MercadoPago - Total'])

df_sub = df

metric = 'qtd_linhas'
plot_point(df_sub, 'subs_asterisk', metric, obs_title='- Quantidade de linhas')


metric = 'qtd_mmhid'
plot_point(df_sub, 'subs_asterisk', metric, obs_title='- Quantidade de MMHID')

metric = 'qtd_des_de42'
plot_point(df_sub, 'subs_asterisk', metric, obs_title='- Quantidade de descriptors x de42')

metric = 'qtd_nome_muni'
plot_point(df_sub, 'subs_asterisk', metric, obs_title='- Quantidade de nomes x município')

metric = 'qtd_tax_id'
plot_point(df_sub, 'subs_asterisk', metric, obs_title='- Quantidade de Tax ID')




#%%



df = df_orig_subs.assign(version='v2').copy()

df.loc[df['is_novo_mmhid'] 
       #& (df['subs_asterisk'] == 'MercadoPago')
       , 'subs_asterisk'] = 'MP - MMHID excluído'


df['reference_date'] = pd.to_datetime(df['reference_date'])
df = df[df['reference_date'] >= '2025-01-01']

df = df[df['subs_asterisk']!='delete_asterisk']
df = df[df['subs_asterisk']!='Outros']
#df = df[~df['is_osasco']]
#df = df[~df['is_novo_mmhid']]
#df = df[df['subs_asterisk']!='Outros_Pags']
df.loc[df['subs_asterisk'] == 'Outros_Pags', 'subs_asterisk'] = 'PagSeguro'
#df = df[df['subs_asterisk']!='MercadoPago']
#df['subs_asterisk'] = df['subs_asterisk'].replace('Outros_Pags', 'PagSeguro')
#df['subs_asterisk'] = df['subs_asterisk'].replace('Outros_SumUp', 'SumUp')


df = df[df['subs_asterisk']!='Stone']


df = df[df['subs_asterisk']!='Rede']
df = df[df['subs_asterisk']!='MP - MMHID excluído']

df.loc[df['is_osasco'] & (df['subs_asterisk'].isin(['MercadoPago'])), 'subs_asterisk'] = 'MP - Osasco'

df = df[df['subs_asterisk'].isin(mp_asterisk)==False]

subs_asterisks = df.groupby(['subs_asterisk'])['qtd_linhas'].sum().sort_values().index.tolist()

import numpy as np

sublist_asteriks = np.array_split(subs_asterisks, 3)
sublist_asteriks = [lst.tolist() for lst in sublist_asteriks]





for sub in sublist_asteriks:
    print('--------------------------------------')
    print('Plotting for subs_asterisk in:', sub)

    df_sub = df[df['subs_asterisk'].isin(sub)]

    metric = 'qtd_linhas'
    plot_point(df_sub, 'subs_asterisk', metric, obs_title='- Quantidade de linhas')


    metric = 'qtd_mmhid'
    plot_point(df_sub, 'subs_asterisk', metric, obs_title='- Quantidade de MMHID')

    metric = 'qtd_des_de42'
    plot_point(df_sub, 'subs_asterisk', metric, obs_title='- Quantidade de descriptors x de42')

    metric = 'qtd_nome_muni'
    plot_point(df_sub, 'subs_asterisk', metric, obs_title='- Quantidade de nomes x município')

    metric = 'qtd_tax_id'
    plot_point(df_sub, 'subs_asterisk', metric, obs_title='- Quantidade de Tax ID')


#%%

query_1 = """
select 
reference_date,
coalesce(acquirer, 'sem acquirer') acquirer,
count(*) qtd_linhas,
count(distinct concat(merchant_market_hierarchy_id, SAFE_CAST(cod_muni as STRING))) qtd_mmhid_muni,
count(distinct merchant_market_hierarchy_id) qtd_mmhid,
count(distinct cpf_cnpj) qtd_cpf_cnpj,
count(distinct id_empresa) qtd_empresa,
count(distinct tam_id) qtd_tam_id,
from `dataplatform-prd.master_contact.final_tam`
left join `dataplatform-prd.master_contact.final_tam_acquirer` using(reference_date, tam_id)

where 1=1
and reference_date >='2025-01-01'
group by 1, 2
order by 2, 1
"""


df_orig1 = read_gbq_from_template(query_1)

#%%

df = df_orig1.copy()
df['version'] = 'v2'
adq = df.groupby(['acquirer'])['qtd_linhas'].sum().sort_values().index.tolist()


import numpy as np

sublist_adqs = np.array_split(adq, 3)
sublist_adqs = [lst.tolist() for lst in sublist_adqs]

for adqs in sublist_adqs:
    print('--------------------------------------')
    print('Plotting for acquirer in:', adqs)

    df_sub = df[df['acquirer'].isin(adqs)]

    metric = 'qtd_linhas'
    plot_point(df_sub, 'acquirer', metric, obs_title='- Quantidade de linhas', dodge=True, limit_0=True)


    metric = 'qtd_mmhid'
    plot_point(df_sub, 'acquirer', metric, obs_title='- Quantidade de MMHID', dodge=True, limit_0=True)

    metric = 'qtd_cpf_cnpj'
    plot_point(df_sub, 'acquirer', metric, obs_title='- Quantidade de CPF/CNPJ', dodge=True, limit_0=True)

    metric = 'qtd_empresa'
    plot_point(df_sub, 'acquirer', metric, obs_title='- Quantidade de empresas', dodge=True, limit_0=True)

    metric = 'qtd_tam_id'
    plot_point(df_sub, 'acquirer', metric, obs_title='- Quantidade de TAM ID', dodge=True, limit_0=True)

#%%

for adqs in [['Ton', 'Stone']]:
    print('--------------------------------------')
    print('Plotting for acquirer in:', adqs)

    df_sub = df[df['acquirer'].isin(adqs)]

    metric = 'qtd_linhas'
    plot_point(df_sub, 'acquirer', metric, obs_title='- Quantidade de linhas', dodge=True, limit_0=True)


    metric = 'qtd_mmhid'
    plot_point(df_sub, 'acquirer', metric, obs_title='- Quantidade de MMHID', dodge=True, limit_0=True)

    metric = 'qtd_cpf_cnpj'
    plot_point(df_sub, 'acquirer', metric, obs_title='- Quantidade de CPF/CNPJ', dodge=True, limit_0=True)

    metric = 'qtd_empresa'
    plot_point(df_sub, 'acquirer', metric, obs_title='- Quantidade de empresas', dodge=True, limit_0=True)

    metric = 'qtd_tam_id'
    plot_point(df_sub, 'acquirer', metric, obs_title='- Quantidade de TAM ID', dodge=True, limit_0=True)


#%%


query_2 = """
with aux as (
select 
cpf_cnpj,
a.tam_id,
id_empresa,
tier,
reference_date,
--gambiarra.tam_id is not null gambiarra_stone,
ARRAY_AGG(DISTINCT coalesce(acquirer, 'sem acquirer')) acquirers,
from `dataplatform-prd.master_contact.final_tam` a
left join `dataplatform-prd.master_contact.final_tam_acquirer` using(reference_date, tam_id)
left join `dataplatform-prd.master_contact.final_tam_tier` using(reference_date, id_empresa)
inner join (select distinct tam_id, reference_date from `dataplatform-prd.master_contact.final_tam_acquirer` where acquirer = 'Stone') using(reference_date, tam_id)
--left join `dataplatform-prd.master_contact.temp_ruan_gambiarra_eliminar_stone_set25` gambiarra on a.reference_date = gambiarra.reference_month and a.tam_id = concat(SAFE_CAST(gambiarra.tam_id as STRING), '202509')
where 1=1
and reference_date >='2025-01-01'
and tier = '3 - Medium. 100K-2Mi'
group by 1,2,3,4,5--, 6
)

, aux2 as (
select
reference_date,
tier,
tam_id value, 
'tam_id' var,
--gambiarra_stone,
acquirers,
count(*) qtd_linhas,
from aux
group by 1,2,3,4, 5--, 6

union all


select
reference_date,
tier,
id_empresa value,
'id_empresa' var,
--gambiarra_stone,
acquirers2 acquirers,
count(*) qtd_linhas,
from (
    select
    reference_date,
    tier,
    id_empresa,
--gambiarra_stone,
    ARRAY_AGG(DISTINCT acquirer) acquirers2
    from aux, unnest(acquirers) as acquirer
    group by 1,2,3--,4
)
group by 1,2,3,4,5--,6
)


select 
reference_date,
tier,
var,
acquirers,
--gambiarra_stone,
sum(qtd_linhas) qtd_linhas,
from aux2
group by 1,2,3,4--,5


"""


df_orig2 = read_gbq_from_template(query_2)

#%%
import numpy as np
df = df_orig2.copy()
df = df[df['tier']=='3 - Medium. 100K-2Mi']
df = df[df['var']=='tam_id']

#df = df[~df['gambiarra_stone']]
#df = df[df['acquirer'].str.contains('Stone')]
df['version'] = 'v2'

df['apenas_stone'] = df['acquirers'].apply(lambda x: ','.join(x) == 'Stone')
df['acquirer'] = np.where(df['apenas_stone'], 'Apenas Stone', 'Stone e outros')


df = pd.concat([
    df,
    df.assign(acquirer='Stone - Total'),
    #df[~df['gambiarra_stone']].assign(acquirer='Stone - Sem Gambiarra')
])



#df = df.explode('acquirers').rename(columns={'acquirers':'acquirer'})

#df = df[df['acquirer']!='Stone']

adq = df.groupby(['acquirer'])['qtd_linhas'].sum().sort_values().index.tolist()


import numpy as np

#sublist_adqs = np.array_split(adq, 1)
#sublist_adqs = [lst.tolist() for lst in sublist_adqs]

#for adqs in sublist_adqs:
for adqs in [adq]:
    print('--------------------------------------')
    print('Plotting for acquirer in:', adqs)

    df_sub = df[df['acquirer'].isin(adqs)]

    metric = 'qtd_linhas'
    plot_point(df_sub, 'acquirer', metric, obs_title='- Quantidade de linhas', dodge=True, limit_0=True)


#%%

for adqs in [['Ton', 'Stone']]:
    print('--------------------------------------')
    print('Plotting for acquirer in:', adqs)

    df_sub = df[df['acquirer'].isin(adqs)]

    metric = 'qtd_linhas'
    plot_point(df_sub, 'acquirer', metric, obs_title='- Quantidade de linhas', dodge=True, limit_0=True)


    metric = 'qtd_mmhid'
    plot_point(df_sub, 'acquirer', metric, obs_title='- Quantidade de MMHID', dodge=True, limit_0=True)

    metric = 'qtd_cpf_cnpj'
    plot_point(df_sub, 'acquirer', metric, obs_title='- Quantidade de CPF/CNPJ', dodge=True, limit_0=True)

    metric = 'qtd_empresa'
    plot_point(df_sub, 'acquirer', metric, obs_title='- Quantidade de empresas', dodge=True, limit_0=True)

    metric = 'qtd_tam_id'
    plot_point(df_sub, 'acquirer', metric, obs_title='- Quantidade de TAM ID', dodge=True, limit_0=True)