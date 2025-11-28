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
                    col='version',
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
where reference_month >= '2024-10-01'
group by 1, 2, 3, 4, 5
"""



df_orig_subs_v1 = read_gbq_from_template(query_template, dict_query={'table_name': 'aux_tam_subs_asterisk_city_v2'})
df_orig_subs_v2 = read_gbq_from_template(query_template, dict_query={'table_name': 'v2_aux_tam_subs_asterisk_city'})

#%%

df = pd.concat([
    df_orig_subs_v1.assign(version='v1'),
    df_orig_subs_v2.assign(version='v2')
])
df.loc[df['is_novo_mmhid'] 
       #& (df['subs_asterisk'] == 'MercadoPago')
       , 'subs_asterisk'] = 'MP - MMHID excluído'


df['reference_date'] = pd.to_datetime(df['reference_date'])
df = df[df['reference_date'] >= '2025-01-01']

df = df[~df['subs_asterisk'].isin(df_orig_subs_v1['subs_asterisk'].unique())]

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







metric = 'qtd_linhas'
plot_point(df, 'subs_asterisk', metric, obs_title=' - versao nova usando de42')


metric = 'qtd_mmhid'
plot_point(df, 'subs_asterisk', metric, obs_title=' - versao nova usando de42')

metric = 'qtd_des_de42'
plot_point(df, 'subs_asterisk', metric, obs_title=' - versao nova usando de42')

metric = 'qtd_nome_muni'
plot_point(df, 'subs_asterisk', metric, obs_title=' - versao nova usando de42')

metric = 'qtd_tax_id'
plot_point(df, 'subs_asterisk', metric, obs_title=' - versao nova usando de42')

#%%


query_template = """
select
reference_month reference_date,
adquirente_padroes,
count(*) qtd_linhas,
sum(qtd_nomes_x_muni) qtd_ecs,
from  `dataplatform-prd.master_contact.{{a}}aux_tam_pre_acquirer`
group by 1, 2
"""

df_orig_adq_v1 = read_gbq_from_template(query_template, dict_query={'a': ''})
df_orig_adq_v2 = read_gbq_from_template(query_template, dict_query={'a': 'v2_'})

#%%


acquirers = [
    'Stone', 
    'Rede', 
    'Desconhecido',
    'Cielo', 
    'unknown',
    'GetNet',
    'possivel_erro',
    'PagSeguro',
    ]
df = pd.concat([
    df_orig_adq_v1.assign(version='v1'),
    df_orig_adq_v2.assign(version='v2')
])
df = df[~df['adquirente_padroes'].isin(acquirers)]
df['reference_date'] = pd.to_datetime(df['reference_date'])
df = df[df['reference_date'] >= '2025-01-01']

metric = 'qtd_ecs'
plot_point(df, 'adquirente_padroes', metric, obs_title=' - versao original')


#%%
from google.cloud import bigquery

def get_cols_bq_table(table_id):
    client = bigquery.Client(project='sfwthr2a4shdyuogrt3jjtygj160rs')
    table = client.get_table(table_id)  # Make an API request.
    return [schema_field.name for schema_field in table.schema]

table_id = 'dataplatform-prd.master_contact.v2_aux_tam_get_document_master'
cols = get_cols_bq_table(table_id)
print(cols)


['reference_month', 'merchant_market_hierarchy_id', 'subs_asterisk', 'numero_inicio', 'nome_master', 'cod_muni', 'nome_merge', 'cpf', 'cnpj', 'nome_cpf', 'nome_cnpj', 'nome_cpf_brasil', 'cpf_brasil', 'bom_resultado', 'bom_resultado_brasil']

#%%

query_template = """
select
reference_month reference_date,
subs_asterisk,
count(*) qtd_linhas,
count(distinct concat(nome_master, cod_muni)) qtd_nome_muni,
count(distinct merchant_market_hierarchy_id) qtd_mmhid,
count(distinct nome_master) qtd_nome,
count(distinct LEFT(nome_master, 6)) qtd_nome_trunc,
count(distinct coalesce(cnpj, cpf)) qtd_tax_id,
from `dataplatform-prd.master_contact.{{table_name}}`
where reference_month >= '2025-01-01'
group by 1, 2
"""

df_a_v1 = read_gbq_from_template(query_template, dict_query={'table_name': 'aux_tam_get_document_master'})
df_a_v2 = read_gbq_from_template(query_template, dict_query={'table_name': 'v2_aux_tam_get_document_master'})



#%%

dfg = df_a_v1.copy()

df = pd.concat([
    dfg.assign(version='v1'),
    df_a_v2.assign(version='v2')
])

normal_subs = [
    #'CloudWalk', 'Ifood', 'Outros', 
    #'MercadoPago', 'MercadoPago_subPagarme', 'Outros_SumUp', 'SumUp',
    'PagSeguro', 'Stone', 'Outros_Pags', 'Outros_Stone']

df['reference_date'] = pd.to_datetime(df['reference_date'])
df = df[df['reference_date'] >= '2025-01-01']
df = df[df['reference_date'] < '2025-08-01']
df = df[df['subs_asterisk'].isin(normal_subs)]





metric = 'qtd_linhas'
plot_point(df, 'subs_asterisk', metric, obs_title=' - versao nova usando de42')


metric = 'qtd_mmhid'
plot_point(df, 'subs_asterisk', metric, obs_title=' - versao nova usando de42')

metric = 'qtd_nome_muni'
plot_point(df, 'subs_asterisk', metric, obs_title=' - versao nova usando de42')

metric = 'qtd_tax_id'
plot_point(df, 'subs_asterisk', metric, obs_title=' - versao nova usando de42')


#%%

table_name = 'aux_tam_ton_to_mmhid'
table_id = f'dataplatform-prd.master_contact.v2_{table_name}'

cols = get_cols_bq_table(table_id)
print(cols)
['document_auth', 'merchant_market_hierarchy_id', 'inserted_at', 'reference_date']


#%%

query_template = """
select
reference_date,
count(*) qtd_linhas,
count(distinct document_auth) qtd_docs,
count(distinct merchant_market_hierarchy_id) qtd_mmhid,
count(distinct inserted_at) qtd_inserts,
from (select distinct * from `dataplatform-prd.master_contact.{{table_name}}`)
where reference_date >= '2025-01-01'
group by 1
"""


df_t_v1 = read_gbq_from_template(query_template, dict_query={'table_name': 'aux_tam_ton_to_mmhid'})
df_t_v2 = read_gbq_from_template(query_template, dict_query={'table_name': 'v2_aux_tam_ton_to_mmhid'})


#%%

df = pd.concat([
    df_t_v1.assign(version='v1'),
    df_t_v2.assign(version='v2')
])

df['reference_date'] = pd.to_datetime(df['reference_date'])
df = df[df['reference_date'] >= '2025-04-01']
df = df[df['reference_date'] < '2025-08-01']
df['blank'] = ''

for metric in ['qtd_linhas', 'qtd_docs', 'qtd_mmhid',]:
    plot_point(df, 'blank', metric, obs_title='')


#%%

table_name = 'aux_tam_ton_auth'
table_id = f'dataplatform-prd.master_contact.v2_{table_name}'
cols = get_cols_bq_table(table_id)
['document',
 'name',
 'de42',
 'reference_date',
 'cod_muni',
 'postal_code_7d',
 'postal_code_6d',
 'postal_code_5d',
 'postal_code_4d']

#%%


query_template = """
select
reference_date,
count(*) qtd_linhas,
count(distinct document) qtd_docs,
count(distinct name) qtd_names,
count(distinct de42) qtd_de42,
count(distinct concat(name, document)) qtd_name_doc,
from `dataplatform-prd.master_contact.{{table_name}}`
where reference_date >= '2025-01-01'
group by 1
"""

df_t_v1 = read_gbq_from_template(query_template, dict_query={'table_name': 'aux_tam_ton_auth'})
df_t_v2 = read_gbq_from_template(query_template, dict_query={'table_name': 'v2_aux_tam_ton_auth'})
#%%
df = pd.concat([
    df_t_v1.assign(version='v1'),
    df_t_v2.assign(version='v2')
])
df['reference_date'] = pd.to_datetime(df['reference_date'])
df = df[df['reference_date'] >= '2025-01-01']
df = df[df['reference_date'] < '2025-08-01']
df['blank'] = ''

for metric in ['qtd_linhas', 'qtd_docs', 'qtd_names', 'qtd_de42', 'qtd_name_doc']:
    plot_point(df, 'blank', metric, obs_title='', limit_0=True)

#%%

query_template = """
select
reference_month reference_date,
subs_asterisk,
count(*) qtd_linhas,
count(distinct concat(nome_master, cod_muni)) qtd_nome_muni,
count(distinct merchant_market_hierarchy_id) qtd_mmhid,
count(distinct nome_master) qtd_nome,
count(distinct LEFT(nome_master, 6)) qtd_nome_trunc,
count(distinct coalesce(cnpj, cpf)) qtd_tax_id,
from `dataplatform-prd.master_contact.{{table_name}}`
where reference_month >= '2025-01-01'
group by 1, 2
"""

df_fn_v1 = read_gbq_from_template(query_template, dict_query={'table_name': 'aux_tam_final_nomes'})
df_fn_v2 = read_gbq_from_template(query_template, dict_query={'table_name': 'v2_aux_tam_final_nomes'})

#%%

from utils import high_contrast_color_palette
df = pd.concat([
    df_fn_v1.assign(version='v1'),
    df_fn_v2.assign(version='v2')
])

normal_subs = [
    'CloudWalk', 'Ifood', 
    #'Outros', 
    'MercadoPago', 'MercadoPago_subPagarme', 'Outros_SumUp', 'SumUp',
    'PagSeguro', 'Stone', 'Outros_Pags', 'Outros_Stone']

df['reference_date'] = pd.to_datetime(df['reference_date'])
df = df[df['reference_date'] >= '2025-01-01']
df = df[df['reference_date'] < '2025-08-01']
df.loc[df['subs_asterisk'] == 'Outros_Pags', 'subs_asterisk'] = 'PagSeguro'
df = df[~df['subs_asterisk'].isin(normal_subs)]

subs_exclude = [
    'PagSeguro', 'Stone', 'Outros_Pags', 'Outros_Stone',
    'Ton', 'Rede', 'Outros', 'MercadoPago_mmhid_problema',
    ]

df = df[~df['subs_asterisk'].isin(subs_exclude)]
for metric in ['qtd_linhas', 'qtd_mmhid', 'qtd_nome_muni', 'qtd_tax_id']:
    plot_point(df, 'subs_asterisk', metric, obs_title='')
