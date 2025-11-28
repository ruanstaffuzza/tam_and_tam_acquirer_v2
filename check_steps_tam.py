#%%

import pandas as pd
from utils import download_data, read_data, read_text, read_gbq_from_template
import os

import seaborn as sns
from matplotlib.ticker import EngFormatter
from matplotlib import pyplot as plt

import pandas_gbq

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

def plot_point(df, col, metric, obs_title='',
               dodge=False,
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
                    aspect=1.7)
    g.set(xlabel='')
    # legend_title = col
    if g.legend is not None:
        g.legend.set_title('')
    for ax in g.axes.flat:
        ax.yaxis.set_major_formatter(EngFormatter())
    plt.suptitle(col + ' ' + obs_title)
    plt.show()
    return g

#%%

queries = [x for x in os.listdir(r'queries/create tables') if x.endswith('.sql')]


def read_first_line_starting_with(query_path, start='CREATE OR REPLACE TABLE `'):
    """
    Get first line of each query starting with start
    """
    start = start.upper().replace(' ', '')
    query_text = read_text(query_path)
    for line in query_text.split('\n'):
        if start in line.upper().replace(' ', ''):
            return line, query_path.split('/')[-1]
    return '', query_path.split('/')[-1]


line1 = [read_first_line_starting_with('queries/create tables/'+x) for x in sorted(queries)]
dict_line = {x[1]:x[0] for x in line1}

dict_table = {k: x.split('`')[1] if len(x.split('`')) > 1 else '' for k, x in dict_line.items()}
dict_table = {k: v for k, v in dict_table.items() if v}

query_template = """
select 
reference_{{month_or_date}} reference_date,
count(*) qtd_linhas,
from `{{table}}`
group by 1
order by 1
"""

exclude_tables = [
  #'dataplatform-prd.master_contact.aux_tam_presenca_online',
  #'dataplatform-prd.addressable_market.tam',
  'dataplatform-prd.economic_research.nomes_todos_distritos',
  'dataplatform-prd.master_contact.names_documents',
  '',
  'dataplatform-prd.master_contact.aux_tam_cpfs_by_city',
]

tables = [x for x in dict_table.values() if x not in exclude_tables]
dict_table = {k: v for k, v in dict_table.items() if v not in exclude_tables}

date_tables = [
    'aux_tam_pos_python',
    'temp_ruan_tam_acquirers',
    'addressable_market.tam',
    'aux_tam_acquirers',
    'aux_tam_presenca_online',
    'addressable_market.tam_acquirer',
    'economic_research.aux_tam_all_docs_names',
    'aux_tam_final_tam',
    'aux_tam_descriptors_total',
    'tam_acquirer_completa',
    'aux_tam_ton_auth',
    'aux_tam_ton_mastercard',
    'aux_tam_ton_match_part1',
    'aux_tam_ton_match_part2',
    'tam_acquirer_exclusivos',
    'v2_temp_cross_company',
    'v2_aux_tam_ton_to_mmhid',
    'temp_ruan_gambiarra_eliminar_stone',

]


def read_table(table, name):
    print('Reading table:', table)
    return read_gbq_from_template(query_template,  
                            dict_query={'month_or_date': 'date'
                                        if any([x in table for x in date_tables])
                                        else 'month', 'table': table}
    ).assign(table=table.replace('dataplatform-prd.', ''),
             name=name)


df = pd.concat([read_table(table, name) for name, table in dict_table.items()])

df_orig = df.copy()

#%%


df = df_orig.copy()



df['beatifull_name'] = df['table'].apply(lambda x: x.split('.')[-1])
del df['table']

df['number'] = df['name'].apply(lambda x: float(x.split('_')[0].replace('-', '.')))

df['new_name'] = df['beatifull_name'].astype(str)
order_name = df[['number', 'new_name']].drop_duplicates().sort_values('number')['new_name'].values
order_name2 = [str(i) + ' - ' + order_name[i] for i in range(len(order_name))]
df['new_name'] = df['new_name'].map({order_name[i]: order_name2[i] for i in range(len(order_name))})

df = df.reset_index(drop=True)

df['reference_date'] = pd.to_datetime(df['reference_date'])

df = df[df['reference_date'] >= '2023-10-01']
#df = df[df['reference_date'] <  '2024-07-01']


df['str_ref_date'] = pd.to_datetime(df['reference_date']).dt.strftime('%m/%y')
df = df.sort_values('reference_date')


g = sns.catplot(data=df, 
                x='str_ref_date', 
                y='qtd_linhas',
                kind='point',
                col='new_name',
                col_wrap=2,
                height=2,
                col_order=order_name2,
                sharey=False,
                aspect=3)
                 
g.set(xlabel='Data', ylabel='Quantidade de linhas')
for ax in g.axes.flat:
    ax.yaxis.set_major_formatter(EngFormatter())

#rename title

g.set_titles(col_template="{col_name}")





#%%


query_template = """
select
reference_date,
cpf_cnpj is not null document_not_null,
document_type,
merchant_market_hierarchy_id is not null mmhid_not_null,
porte_rfb,
count(*) qtd_linhas,
count(distinct cpf_cnpj) qtd_docs,
from `dataplatform-prd.master_contact.final_tam`
group by 1, 2, 3, 4, 5
"""
df_orig2 = read_gbq_from_template(query_template)

#%% 

df = df_orig2.copy()
df['document_type'] = df['document_type'].fillna('Sem documento')
df['porte_rfb'] = df['porte_rfb'].fillna('Sem porte')

dfs = [
    df.groupby(['reference_date', 'document_type'])['qtd_linhas'].sum().reset_index().assign(var = 'tipo documento',
                                                                                             var2 = lambda x: x['document_type']),
    df.groupby(['reference_date', 'porte_rfb'])['qtd_linhas'].sum().reset_index().assign(var = 'porte rfb',
                                                                                             var2 = lambda x: x['porte_rfb']),
    df.groupby(['reference_date'])['qtd_linhas'].sum().reset_index().assign(var = 'total',
                                                                                                var2 = 'total'),
    
    (df.groupby(['reference_date'])['qtd_docs'].sum().rename('qtd_linhas').reset_index().assign(var = 'Qtd. Docs',
                                                                                                var2 = 'total')
    ),

    (df.groupby(['reference_date', 'mmhid_not_null'])['qtd_linhas'].sum()
     .reset_index().assign(var = 'Mmhid',
                            var2 = lambda x: x['mmhid_not_null'].map({True: 'Tem mmhid', False: 'Não tem mmhid'}))
     )
]

for dataframe in dfs:
    g = sns.catplot(data=dataframe,
                x='reference_date',
                y='qtd_linhas',
                #hue='var',
                kind='point',
                hue='var2',
                aspect=1.7)
    g.set(xlabel='Data', ylabel='Quantidade de linhas')
    for ax in g.axes.flat:
        ax.yaxis.set_major_formatter(EngFormatter())
    plt.suptitle(dataframe['var'].iloc[0])

#%%
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
        print('using dict_query')
        return read_gbq_(query)
    return read_gbq_(query)
from jinja2 import Template
template_query = """ teste {{version}} """
dict_query = {'version': '_v1'}
query = Template(template_query).render(dict_query)
print(query)

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
from  `dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city`
left join `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places` p using(merchant_market_hierarchy_id, reference_month)
--where merchant_tax_id =''
group by 1, 2, 3, 4, 5
"""




df_orig_subs_v2 = read_gbq_from_template(query_template)

#%%

df = df_orig_subs_v2.copy()
df = df[df['subs_asterisk']!='delete_asterisk']
df = df[df['subs_asterisk']!='Outros']
#df = df[~df['is_osasco']]
#df = df[~df['is_novo_mmhid']]
df = df[df['subs_asterisk']!='Outros_Pags']
#df = df[df['subs_asterisk']!='MercadoPago']
#df['subs_asterisk'] = df['subs_asterisk'].replace('Outros_Pags', 'PagSeguro')
#df['subs_asterisk'] = df['subs_asterisk'].replace('Outros_SumUp', 'SumUp')


df.loc[df['is_osasco'] & (df['subs_asterisk'] == 'MercadoPago'), 'subs_asterisk'] = 'MP - Osasco'
df.loc[df['is_novo_mmhid'] 
       #& (df['subs_asterisk'] == 'MercadoPago')
       , 'subs_asterisk'] = 'MP - MMHID excluído'

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


df = df_orig_subs_v2.copy()
df = df[df['subs_asterisk']!='delete_asterisk']
df = df[df['subs_asterisk']!='Outros']
df = df[~df['is_osasco']]
#df = df[df['subs_asterisk']!='Outros_Pags']
#df = df[df['subs_asterisk']!='MercadoPago']
df['subs_asterisk'] = df['subs_asterisk'].replace('Outros_Pags', 'PagSeguro')
df['subs_asterisk'] = df['subs_asterisk'].replace('Outros_SumUp', 'SumUp')



title = 'subs_asterisk'

metric = 'qtd_linhas'
plot_point(df, title, metric, obs_title=' - Qtd. linhas')


metric = 'qtd_mmhid'
plot_point(df, title, metric, obs_title=' - Qtd. MMHID')

metric = 'qtd_des_de42'
plot_point(df, title, metric, obs_title=' - Qtd. Concat(de42_merchant_id, merchant_descriptor)')

metric = 'qtd_nome_de42'
plot_point(df, title, metric, obs_title=' - Qtd. Concat(de42_merchant_id, nome_limpo)')



metric = 'qtd_nome_muni'
plot_point(df, title, metric, obs_title=' - Qtd. Concat(municipio, nome_limpo)')


metric = 'qtd_tax_id'
plot_point(df, title, metric, obs_title=' - Qtd. Tax ID')


metric = 'qtd_nome'
plot_point(df, title, metric, obs_title=' - Qtd. Nome')

metric = 'qtd_nome_trunc'
plot_point(df, title, metric, obs_title=' - Qtd. Nome Truncado (6 primeiros caracteres)')



#%%

df = df_orig_subs_v2.copy()
#df = df[df['subs_asterisk']!='delete_asterisk']
#df = df[df['subs_asterisk']!='Outros']

df['subs_asterisk'] = df['subs_asterisk'].replace('Outros_Pags', 'PagSeguro')


cols_qtd = ['qtd_linhas', 'qtd_mmhid', 'qtd_des_de42', 'qtd_tax_id']
df = df.groupby(['reference_date', 'subs_asterisk', 'is_osasco'])[cols_qtd].sum().reset_index()

df = df.melt(id_vars=['reference_date', 'subs_asterisk', 'is_osasco'],
            value_vars=cols_qtd,
            var_name='metric',
            value_name='value').reset_index()

df = df.pivot(index=['reference_date', 'subs_asterisk', 'metric'],
            columns='is_osasco',
            values='value'
              ).reset_index()

df['share_osasco'] = (df[True] / (df[True] + df[False])) * 100

df = df.pivot(index=['reference_date', 'subs_asterisk'],
            columns='metric',
            values='share_osasco'
).reset_index()



metric = 'qtd_linhas'
plot_point(df, 'subs_asterisk', metric, obs_title=' - versao nova usando de42')


metric = 'qtd_mmhid'
plot_point(df, 'subs_asterisk', metric, obs_title=' - versao nova usando de42')

metric = 'qtd_des_de42'
plot_point(df, 'subs_asterisk', metric, obs_title=' - versao nova usando de42')

metric = 'qtd_tax_id'
plot_point(df, 'subs_asterisk', metric, obs_title=' - versao nova usando de42')





#%%


df =  df_orig_subs_v2.copy()
df['reference_date'] = pd.to_datetime(df['reference_date'])
#df = df[pd.to_datetime(df['reference_date'])!=pd.to_datetime('2024-08-31')]

df2 = df_orig_subs_v2.copy()
df2['reference_date'] = pd.to_datetime(df2['reference_date'])
#df2 = df2[pd.to_datetime(df2['reference_date'])==pd.to_datetime('2024-08-31')]

df = df.assign(versao='v1 usando apenas de43 (PAG*)')
df2 = df2.assign(versao='v2 usando apenas de42')

df3 = pd.concat([
    df.query('reference_date != "2024-08-31"'),
    df2.query('reference_date == "2024-08-31"')
    ]).assign(versao='mistura')
df = pd.concat([df, df2, df3])

df = df[df['subs_asterisk'].isin([
    'Outros_Pags', 
    'PagSeguro'
    ])]


metric = 'qtd_des_de42'
plot_point(df, 'versao', metric, obs_title='- qtd mmhid pags')
#%%



metric = 'qtd_des'

plot_point(df[~df['is_osasco']], 'subs_asterisk', metric, ' - Sem Osasco')
plot_point(df, 'subs_asterisk', metric)
plot_point(df[df['is_osasco']], 'subs_asterisk', metric, ' - Osasco')

#%%

metric = 'qtd_des_de42'
plot_point(df, 'subs_asterisk', metric)



metric = 'qtd_linhas'
plot_point(df, 'subs_asterisk', metric)


metric = 'qtd_mmhid'
plot_point(df, 'subs_asterisk', metric)

#%%


query_template = """
select
reference_month reference_date,
cod_muni=3534401 is_osasco,
CASE WHEN STARTS_WITH(TRIM(merchant_descriptor), 'MP*') then 'MP*'
     WHEN STARTS_WITH(TRIM(merchant_descriptor), 'MERCADOPAGO*') then 'MERCADOPAGO*'
     WHEN (STARTS_WITH(TRIM(de42_merchant_id), 'M') AND LENGTH(TRIM(de42_merchant_id)) = 15) then 'Start M'
     when TRIM(de42_merchant_id) IN ('34', '31', '103', '29', '148', '147', '000000007187449', '7187449', '138006') then de42_merchant_id
     else 'ERRO' end as padrao,
LEFT(dE42_merchant_id, 3) de42_trunc_3, 
REGEXP_CONTAINS(merchant_descriptor, 'RUA') is_rua,
count(*) qtd_linhas,
count(distinct concat(de42_merchant_id, merchant_descriptor)) qtd_des_de42,
count(distinct concat(de42_merchant_id, nome_limpo)) qtd_nome_de42,
count(distinct concat(nome_limpo, cod_muni)) qtd_nome_muni,
count(distinct merchant_descriptor) qtd_des,
count(distinct merchant_market_hierarchy_id) qtd_mmhid,
count(distinct nome_limpo) qtd_nome,
count(distinct LEFT(nome_limpo, 6)) qtd_nome_trunc,
count(distinct merchant_tax_id) qtd_tax_id,
count(distinct de42_merchant_id) qtd_de42,
from  `dataplatform-prd.master_contact.aux_tam_subs_asterisk_city{{version}}`
left join `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places` p using(merchant_market_hierarchy_id, reference_month)
where 1=1
and subs_asterisk = 'MercadoPago'
and reference_month >= '2024-10-01' 
group by 1, 2, 3, 4, 5
"""




df_orig_subs_mp = read_gbq_from_template(query_template, dict_query={'version': '_v2'})

#%%

df = df_orig_subs_mp.copy().rename(columns={'padrao': 'padrao_identificador_mercadopago'})

df = df[~df['is_osasco']]

df = df[~df['padrao_teste'] ]


title = 'padrao_identificador_mercadopago'

metric = 'qtd_linhas'
plot_point(df, title, metric, obs_title=' - Qtd. linhas')


metric = 'qtd_mmhid'
plot_point(df, title, metric, obs_title=' - Qtd. MMHID')

metric = 'qtd_des_de42'
plot_point(df, title, metric, obs_title=' - Qtd. Concat(de42_merchant_id, merchant_descriptor)')

metric = 'qtd_nome_de42'
plot_point(df, title, metric, obs_title=' - Qtd. Concat(de42_merchant_id, nome_limpo)')



metric = 'qtd_nome_muni'
plot_point(df, title, metric, obs_title=' - Qtd. Concat(municipio, nome_limpo)')


metric = 'qtd_tax_id'
plot_point(df, title, metric, obs_title=' - Qtd. Tax ID')


metric = 'qtd_nome'
plot_point(df, title, metric, obs_title=' - Qtd. Nome')

metric = 'qtd_nome_trunc'
plot_point(df, title, metric, obs_title=' - Qtd. Nome Truncado (6 primeiros caracteres)')




metric = 'qtd_de42'
plot_point(df, title, metric, obs_title=' - Qtd. DE42')


#%%


df = df_orig_subs_mp.copy().rename(columns={'padrao': 'padrao_identificador_mercadopago'})

df = df[~df['is_osasco']]


title = 'is_rua'

metric = 'qtd_linhas'
plot_point(df, title, metric, obs_title=' - Qtd. linhas')


metric = 'qtd_mmhid'
plot_point(df, title, metric, obs_title=' - Qtd. MMHID')

metric = 'qtd_des_de42'
plot_point(df, title, metric, obs_title=' - Qtd. Concat(de42_merchant_id, merchant_descriptor)')

metric = 'qtd_nome_de42'
plot_point(df, title, metric, obs_title=' - Qtd. Concat(de42_merchant_id, nome_limpo)')



metric = 'qtd_nome_muni'
plot_point(df, title, metric, obs_title=' - Qtd. Concat(municipio, nome_limpo)')


metric = 'qtd_tax_id'
plot_point(df, title, metric, obs_title=' - Qtd. Tax ID')


metric = 'qtd_nome'
plot_point(df, title, metric, obs_title=' - Qtd. Nome')

metric = 'qtd_nome_trunc'
plot_point(df, title, metric, obs_title=' - Qtd. Nome Truncado (6 primeiros caracteres)')




metric = 'qtd_de42'
plot_point(df, title, metric, obs_title=' - Qtd. DE42')
#%%



query_template = """
select
reference_month reference_date,
subs_asterisk,
count(*) qtd_linhas,
count(distinct concat(de42_merchant_id, merchant_descriptor)) qtd_des_de42,
count(distinct concat(de42_merchant_id, nome_limpo)) qtd_nome_de42,
count(distinct concat(nome_limpo, cod_muni)) qtd_nome_muni,
count(distinct merchant_descriptor) qtd_des,
count(distinct merchant_market_hierarchy_id) qtd_mmhid,
count(distinct nome_limpo) qtd_nome,
count(distinct LEFT(nome_limpo, 6)) qtd_nome_trunc,
count(distinct merchant_tax_id) qtd_tax_id,
count(distinct de42_merchant_id) qtd_de42,
from  `dataplatform-prd.master_contact.aux_tam_subs_asterisk_city{{version}}`
left join `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places` p using(merchant_market_hierarchy_id, reference_month)
where 1=1
and reference_month >= '2024-10-01' 
group by 1, 2
"""




df_orig_subs_mp3 = read_gbq_from_template(query_template, dict_query={'version': '_v2'})

#%%

df = df_orig_subs_mp3.copy()
df = df[df['subs_asterisk']!='delete_asterisk']
df = df[df['subs_asterisk']!='Outros']

df['subs_asterisk'] = df['subs_asterisk'].replace({'Outros_Pags': 'PagSeguro',
                                                  'Outros_SumUp': 'SumUp'})
title = 'subs_asterisk'

metric = 'qtd_linhas'
plot_point(df, title, metric, obs_title=' - Qtd. linhas')


metric = 'qtd_mmhid'
plot_point(df, title, metric, obs_title=' - Qtd. MMHID')

metric = 'qtd_des_de42'
plot_point(df, title, metric, obs_title=' - Qtd. Concat(de42_merchant_id, merchant_descriptor)')

metric = 'qtd_nome_de42'
plot_point(df, title, metric, obs_title=' - Qtd. Concat(de42_merchant_id, nome_limpo)')



metric = 'qtd_nome_muni'
plot_point(df, title, metric, obs_title=' - Qtd. Concat(municipio, nome_limpo)')


metric = 'qtd_tax_id'
plot_point(df, title, metric, obs_title=' - Qtd. Tax ID')


metric = 'qtd_nome'
plot_point(df, title, metric, obs_title=' - Qtd. Nome')

metric = 'qtd_nome_trunc'
plot_point(df, title, metric, obs_title=' - Qtd. Nome Truncado (6 primeiros caracteres)')




metric = 'qtd_de42'
plot_point(df, title, metric, obs_title=' - Qtd. DE42')

#%%



df = df_orig_subs_mp.copy().rename(columns={'padrao': 'padrao_identificador_mercadopago'})

df = df[~df['is_osasco']]


title = 'padrao_identificador_mercadopago'

metric = 'qtd_linhas'

df['padrao_teste1'] = df['de42_trunc_3'].isin(['000'])
df['padrao_teste2'] = df['de42_trunc_3'].isin(['M10'])
df['padrao_teste3'] = df['de42_trunc_3'].isin(['M10', '000', '26', '280', '29', '38', '39', '44', '141'])



plot_point(df, title, metric, obs_title=' - Qtd. linhas - Todos')

plot_point(df[~df['padrao_teste1']], title, metric, obs_title=' - Qtd. linhas - Sem padrao 000')

plot_point(df[~df['padrao_teste2']], title, metric, obs_title=' - Qtd. linhas - Sem padrao M10')

plot_point(df[~df['padrao_teste1'] & ~df['padrao_teste2']], title, metric, obs_title=' - Qtd. linhas - Sem padrao 000 e M10')

plot_point(df[~df['padrao_teste3']], title, metric, obs_title=' - Qtd. linhas - Sem padrao 000, M10, padrao_apenas_maio')



plot_point(df[df['padrao_teste3']], title, metric, obs_title=' - Qtd. linhas - Apenas padrao 000, M10, padrao_apenas_maio')

#%%



query_template = """
select
reference_month reference_date,
cod_muni=3534401 is_osasco,
LEFT(dE42_merchant_id, 3) de42_trunc_3,
LEFT(dE42_merchant_id, 6) de42_trunc_6,
LEFT(dE42_merchant_id, 9) de42_trunc_9,
count(*) qtd_linhas,
count(distinct concat(de42_merchant_id, merchant_descriptor)) qtd_des_de42,
count(distinct concat(de42_merchant_id, nome_limpo)) qtd_nome_de42,
count(distinct concat(nome_limpo, cod_muni)) qtd_nome_muni,
count(distinct merchant_descriptor) qtd_des,
count(distinct merchant_market_hierarchy_id) qtd_mmhid,
count(distinct nome_limpo) qtd_nome,
count(distinct LEFT(nome_limpo, 6)) qtd_nome_trunc,
count(distinct merchant_tax_id) qtd_tax_id,
count(distinct de42_merchant_id) qtd_de42,
from  `dataplatform-prd.master_contact.aux_tam_subs_asterisk_city{{version}}`
left join `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places` p using(merchant_market_hierarchy_id, reference_month)
where 1=1
and subs_asterisk = 'MercadoPago'
and STARTS_WITH(TRIM(merchant_descriptor), 'MP*')
and reference_month >= '2024-10-01'
group by 1, 2, 3, 4, 5
"""




df_orig_subs_mpv2 = read_gbq_from_template(query_template, dict_query={'version': '_v2'})

#%%
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

title = 'de42_trunc_3'

df = df_orig_subs_mpv2.copy().rename(columns={'padrao': 'padrao_identificador_mercadopago'})

df = df[~df['is_osasco']]


dfg_total = df.groupby(['reference_date', 'de42_trunc_3'])['qtd_linhas'].sum().reset_index()
dfg_total = dfg_total[dfg_total['qtd_linhas'] > 10000]

#df = df[df['de42_trunc_3'].isin(dfg_total['de42_trunc_3'])]


comeco_em_maio = df.groupby('de42_trunc_3')['reference_date'].min()
comeco_em_maio = pd.to_datetime(comeco_em_maio)
comeco_em_maio = comeco_em_maio[comeco_em_maio >= '2025-05-01'].index.tolist()

print('de42_trunc_3 comecando em maio:', comeco_em_maio)

grupo_flat = [
    '028', '060', '079', '087', '089', 
    #'118',
                                    '130', 
                                    #'124', 
                                    #'205', 
                                    '876', 
                                    #'41', 
                                    #'211'

                                    '000', 'M10', '141'
                                    ]

metric = 'qtd_linhas'
plot_point(df[df['de42_trunc_3'].isin(grupo_flat)], title, metric, obs_title=' - Qtd. linhas',
           dodge=False)


plot_point(df[df['de42_trunc_3'].isin(comeco_em_maio)], title, metric, obs_title=' - Qtd. linhas - Começando em Maio',
              dodge=False)



df = df[~df['de42_trunc_3'].isin(grupo_flat)]
df = df[~df['de42_trunc_3'].isin(comeco_em_maio)]



unique_de42_trunc_3 = df['de42_trunc_3'].unique()
print('qtd. de42_trunc_3:', len(unique_de42_trunc_3))




#%%

dfor = df.copy()
for range_10 in chunks(sorted(unique_de42_trunc_3), 15):
    df = dfor[dfor['de42_trunc_3'].isin(range_10)]
    #df = df[~df['de42_trunc_3'].isin(grupo_flat)]



    metric = 'qtd_linhas'
    plot_point(df, title, metric, obs_title=' - Qtd. linhas', dodge=True)





#%% check adq

query_template = """
with temp_table as(
select distinct
t.reference_date, tam_id, cpf_cnpj, 
modelo_previsao,
coalesce(case when adquirente = 'CloudWalk' then 'InfinitePay'
     else adquirente end, 'Sem info') acquirer, 
from `dataplatform-prd.master_contact.aux_tam_final_tam` t 
left join `dataplatform-prd.master_contact.aux_tam_acquirers_v2` a using(tam_id, reference_date)
)

select 
reference_date, 
acquirer, 
modelo_previsao,
count(distinct tam_id) qtd_ecs, 
count(distinct cpf_cnpj) qtd_ecs_docs 
from temp_table 
group by 1,2, 3
"""

df_orig_adq = read_gbq_from_template(query_template)
#%%

df = df_orig_adq.copy()
#df = df[~df['modelo_previsao']]


df = df[df['acquirer'].isin([
    'Sem info',
    'InfinitePay', 'PagSeguro', 'MercadoPago', 'Rede', 'Cielo', 'GetNet', 'Stone', 'SumUp', 'Ton'
])]


acquirer_micro = ['InfinitePay', 'PagSeguro', 'MercadoPago', 'Ton', 'SumUp']
acquirer_smb = ['Rede', 'Cielo', 'GetNet', 'Stone', 'Sem info']


plot_point(df[df['acquirer'].isin(acquirer_micro)], 'acquirer', metric='qtd_ecs', obs_title='- Qtd. ECs - Adqs. Micro')
plot_point(df[df['acquirer'].isin(acquirer_smb)], 'acquirer', metric='qtd_ecs', obs_title='- Qtd. ECs - Adqs. SMB')

plot_point(df[df['acquirer'].isin(acquirer_micro)], 'acquirer', metric='qtd_ecs_docs', obs_title='- Qtd. Docs')

plot_point(df[df['acquirer'].isin(acquirer_smb)], 'acquirer', metric='qtd_ecs_docs', obs_title='- Qtd. Docs')


#%%


df = df_orig_adq.copy()
#df = df[~df['modelo_previsao']]
df['reference_date'] = pd.to_datetime(df['reference_date'])


df = df[~df['modelo_previsao']]
df = df[df['acquirer'].isin([
    'Sem info',
    'InfinitePay', 'PagSeguro', 'MercadoPago', 'Rede', 'Cielo', 'GetNet', 'Stone', 'SumUp', 'Ton'
])]


acquirer_micro = ['InfinitePay', 'PagSeguro', 'MercadoPago', 'Ton', 'SumUp']
acquirer_smb = ['Rede', 'Cielo', 'GetNet', 'Stone', 'Sem info']

df['share_com_doc'] = df['qtd_ecs_docs'] / df['qtd_ecs']

df = df[df['reference_date'] >= '2024-08-01']

plot_point(df[df['acquirer'].isin(acquirer_smb)], 'acquirer', metric='share_com_doc', obs_title='- Share com doc - Adqs. SMB')


#%%
plot_point(df[df['acquirer'].isin(['InfinitePay',  'MercadoPago', 'SumUp', 'GetNet'])], 'acquirer', metric='qtd_ecs_docs', obs_title='- Qtd. Docs')

#%%

query_template = """

with cidade_places as (
  SELECT DISTINCT 
  reference_month,
    merchant_market_hierarchy_id,
    merchant_tax_id,
  FROM 
    `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places`
)

select
reference_month reference_date,
case 
     when merchant_tax_id = '10573521000191' then 'MP'
     when merchant_tax_id = '16668076000120' then 'SumUp'
     when merchant_tax_id = '08561701000101' then 'PagSeguro'
        when merchant_tax_id = '14380200000121' then 'Ifood'
        when merchant_tax_id = '18727053000174' then 'Pagarme'
        when merchant_tax_id = '03816413000137' then 'Pagueveloz'
        when merchant_tax_id = '06308851000182' then 'MOOZ SOLUCOES FINANCEIRAS LTDA'
        when merchant_tax_id = '01109184000438' then 'UOL Pags'
        when merchant_tax_id = '21689483000153' then 'Bilheteria Digital'
        when merchant_tax_id = '' then 'Vazio'
        else 'Outros' end tax_id_provider,
b.cnpj is not null participante_homologado,
count(distinct merchant_market_hierarchy_id) qtd_ecs,
from `dataplatform-prd.master_contact.aux_tam_pre_acquirer`
left join `dataplatform-prd.master_contact.aux_tam_get_document_master` a using(merchant_market_hierarchy_id, reference_month)
left join cidade_places using(merchant_market_hierarchy_id, reference_month)
left join `dataplatform-prd.master_contact.participantes_homologados` b on merchant_tax_id=b.cnpj
where adquirente_padroes = 'Rede'
and reference_month >= '2024-10-01'
group by 1,2,3
"""

df_rede = read_gbq_from_template(query_template)


#%%
from IPython.display import display
df = df_rede.copy()
display(df.sort_values(['participante_homologado', 'tax_id_provider', 'reference_date', ]))

df['new'] = (df['tax_id_provider']!='Outros') | (df['participante_homologado']==True)

df = df.groupby(['reference_date', 'new'])[['qtd_ecs']].sum().reset_index()
df

#%%

query_template = """
select
reference_month reference_date,
adquirente_padroes,
count(*) qtd_linhas,
sum(qtd_nomes_x_muni) qtd_ecs,
from  `dataplatform-prd.master_contact.aux_tam_pre_acquirer{{version}}`
group by 1, 2
"""

df_orig_adq_v1 = read_gbq_from_template(query_template, dict_query={'version': ''})
df_orig_adq_v2 = read_gbq_from_template(query_template, dict_query={'version': '_v2'})
#%%

acquirers = ['Stone', 'Rede', 'Cielo', 'unknown']
df = df_orig_adq_v1.copy()
df = df[df['adquirente_padroes'].isin(acquirers)]

metric = 'qtd_ecs'
plot_point(df, 'adquirente_padroes', metric, obs_title=' - versao original')

df = df_orig_adq_v2.copy()
df = df[df['adquirente_padroes'].isin(acquirers)]

plot_point(df, 'adquirente_padroes', metric, obs_title=' - versao nova usando de42')

#%%

query_template = """

select
reference_date,
count(*) qtd_linhas,
count(distinct tam_id) qtd_ecs,
from  `dataplatform-prd.master_contact.aux_tam_final_tam`
group by 1
"""

df_orig_test_ids = read_gbq_from_template(query_template)

df = df_orig_test_ids.copy()
df = df.sort_values('reference_date')
df['diff'] = df['qtd_ecs'] - df['qtd_linhas']

df

#%%

query_template = """
select
reference_month reference_date,
adquirente_padroes,
count(*) qtd_linhas,
sum(qtd_nomes_x_muni) qtd_ecs,
from  `dataplatform-prd.master_contact.aux_tam_pre_acquirer{{version}}`
group by 1, 2
"""

#%%

df = df_orig_subs_v2.copy()
df = df[df['subs_asterisk']=='Ton']
df['reference_date'] = pd.to_datetime(df['reference_date'])
df = df[df['reference_date'] >= '2024-08-01']

df = df.groupby(['reference_date', 'subs_asterisk'])[['qtd_linhas', 'qtd_des_de42', 'qtd_des', 'qtd_mmhid']].sum().reset_index()
df = df.sort_values('reference_date')
df



#%%

query_template = """
select 
reference_date,
de42_online,
asterisk_online,
ecom_nsr,
adq_ifood,
adq_mp_osasco,
adyen_online,
count(*) qtd_linhas,
from   `dataplatform-prd.master_contact.aux_tam_presenca_online` 
group by 1, 2, 3, 4, 5, 6, 7
"""
df_orig_online = read_gbq_from_template(query_template)
#%%

df = df_orig_online.copy()

cols = ['de42_online', 'asterisk_online', 'ecom_nsr', 'adq_ifood',
        'adq_mp_osasco', 'adyen_online']




dfg = [qtd_linhas_by(df, col) for col in cols]


dfg = pd.concat(dfg)
dfg = dfg[dfg['var2']]
g = sns.catplot(data=dfg,
                x='reference_date',
                y='qtd_linhas',
                kind='point',
                hue='var',
                aspect=1.7)
