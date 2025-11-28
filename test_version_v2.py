#%%
from utils import download_data, read_data
import pandas as pd

def update_data():
    download_data('test_version_v2', 
                  update=True,
                  dict_query={'min_ref_month': '2025-03-01'}
                  )
    
    download_data('teste_versao_fix', 
                  update=True,
                  dict_query={'min_ref_month': '2025-03-01'}
                  )
    
    
import matplotlib.pyplot as plt
import seaborn as sns

#EngFormatter
from matplotlib.ticker import EngFormatter
import matplotlib.dates as mdates



import itertools
import numpy as np

from IPython.display import display

from IPython.display import Markdown

#%% Preparação dos dados

#del prepare_data
from functools import cache

#@cache
def prepare_data(table_name='test_version_v2'):

    df = read_data(table_name)
  

    df['is_osasco'] = df['is_osasco'].fillna(False)

    #df = df[df['is_osasco'].fillna(False)==False]
    df['reference_date'] = df['reference_date'].dt.to_period('M').dt.to_timestamp()

    df = df[df['reference_date']>='2025-03-01']


    def clean_data_adqs(x:str) -> str:
        if pd.isna(x):
            return x
        
        x_list = x.split(', ')
        
        dict_map = {
            'EC': 'MercadoPago',
            'Adiqplus': 'Adiq',
            'Magalu1': 'Magalu',
            'Magalu2': 'Magalu',
        }

        x_mapped = [dict_map.get(i, i) for i in x_list]
        return ', '.join(np.sort(list(set(x_mapped))))

    df['acquirer_array'] = df['acquirer_array'].apply(clean_data_adqs)


    #df['acquirer_array'] = df['acquirer_array'].str.split(', ')
    #drop duplicates
    #df = df[df['acquirer_array'].notnull()]
    return df

df_orig = prepare_data(table_name='test_version_v2')

df = df_orig.copy()


#%% Funções de tratamento e plotagem

def treat_date_plot_adq(df_filter):
    df = df_filter.copy()


    df = df[~df['is_osasco'].fillna(False)]

    df = df.copy()
    df['acquirer_array'] = df['acquirer_array'].str.split(', ')
    df['version'] = 'x'

    def stone_or_ton_to_st(x:list):
        
        dict_map = {
            'Stone': 'StoneCo',
            'Ton': 'StoneCo',
            'Pagarme': 'StoneCo'
        }

        x = [dict_map.get(i, i) for i in x]
        return list(set(x))


    #df['acquirer_array'] = df['acquirer_array'].apply(stone_or_ton_to_st)


    df = df.explode('acquirer_array').groupby(['version', 'reference_date', 'acquirer_array']).agg({'value': 'sum'}).reset_index()
    df = df[df['acquirer_array'].notnull()]


    #df = df[df['acquirer_array'].isin(list_interest)]

    df['reference_date'] = df['reference_date'].dt.to_period('M').dt.to_timestamp()
    return df


def treat_data_total_and_plot(df_filter, metric_name):
    df = df_filter.copy()

    df = pd.concat([
        df.assign(version='total'),
        df[~df['is_osasco']].assign(version='sem osasco'),
        df[~(df['is_osasco']) & (df['acquirer_array'].notnull())].assign(version='apenas com adquirente'),
        df[~(df['is_osasco']) & (df['acquirer_array'].notnull()) & (df['acquirer_array']!='Não identificado')].assign(version='apenas com adquirente identificada'),
    ])


    df_total = df.groupby(['version', 'reference_date']).agg({'value': 'sum'}).reset_index()

    g = sns.relplot(data=df_total, 
                x='reference_date', 
                y='value', 
                hue='version', 
                #col='variable',
                marker='o',
                kind='line',
                palette='tab10',
                aspect=3,
                )

    for ax in g.axes.flat:
        ax.yaxis.set_major_formatter(EngFormatter())
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b%y'))

    g.figure.suptitle(f'{metric_name} - diferentes versões', y=1, va='bottom', fontweight='bold', fontsize=16)
    plt.show()



def display_table_qtd(df_filter):
    df = df_filter.copy()

    df['acquirer_array'] = df['acquirer_array'].str.split(', ')

    df = df[df['reference_date'].dt.year==2025]


    df = df.explode('acquirer_array').groupby(['reference_date', 'acquirer_array']).agg({'value': 'sum'}).reset_index()
    df = df[df['acquirer_array'].notnull()]


    df['ref_dt_str'] = df['reference_date'].dt.strftime('%m-%y').str.lower()


    df = df.pivot(
        index=['acquirer_array'],
        columns='ref_dt_str',
        values='value',
    )

    #df['diff'] = df_pivot['2025-08-01'] - df_pivot['2025-07-01']
    df['diff_AUG_JUL'] = df['08-25'] - df['07-25']
    display(df)


#%% Gráfico total

tipo_choice = 'empresa'  # 'empresa' or 'estabelecimento'

display(Markdown(f'# Análise de {tipo_choice}s'))

if tipo_choice not in ['empresa', 'estabelecimento']:
    raise ValueError("tipo_choice deve ser 'empresa' ou 'estabelecimento'")

metric_name = 'Qtd. Empresas' if tipo_choice == 'empresa' else 'Qtd. Estabelecimentos'
    
df_filter = df_orig.copy()

if tipo_choice == 'empresa':
    df_filter['value'] = df_filter['qtd_empresa'] 
else:
    df_filter['value'] = df_filter['qtd_ec']



def display_table_qtd(df_filter):
    df = df_filter.copy()

    df['acquirer_array'] = df['acquirer_array'].str.split(', ')

    df = df[df['reference_date'].dt.year==2025]


    df = df.explode('acquirer_array').groupby(['reference_date', 'acquirer_array']).agg({'value': 'sum'}).reset_index()
    df = df[df['acquirer_array'].notnull()]

    

    list_interest = ['Cielo', 'GetNet', 'Stone']


    if list_interest:
        df = df[df['acquirer_array'].isin(list_interest)]


    df['ref_dt_str'] = df['reference_date'].dt.strftime('%m-%y').str.lower()


    df = df.pivot(
        index=['acquirer_array'],
        columns='ref_dt_str',
        values='value',
    )

    #df['diff'] = df_pivot['2025-08-01'] - df_pivot['2025-07-01']
    df['diff_AUG_JUL'] = df['08-25'] - df['07-25']
    df['growth_AUG_JUL'] = df['diff_AUG_JUL'] *100 / (df['07-25'] + 1e-9)
    display(df)



print('As IS')
display_table_qtd(df_filter)

#%%
print('Versão com solução aplicada (removendo problemáticos)')
display_table_qtd(df_filter[df_filter['is_problematic']==False])


print('Versão com solução aplicada (removendo problemáticos) e apenas com mmhid')
display_table_qtd(df_filter[(df_filter['is_problematic']==False)
                            & (df_filter['has_mmhid'])
                            ])

print('Apenas com mmhid')
display_table_qtd(df_filter[(df_filter['has_mmhid'])
                            ])



#%%
var = 'is_problematic'
df = df_filter.copy()

df['acquirer_array'] = df['acquirer_array'].str.split(', ')

df = df[df['reference_date'].dt.year==2025]


df = df.explode('acquirer_array')

df = df.groupby(['reference_date', 'acquirer_array', var]).agg({'value': 'sum'}).reset_index()


list_interest = ['Cielo', 'GetNet', 'Stone']


if list_interest:
    df = df[df['acquirer_array'].isin(list_interest)]
    print('Apenas adquirentes de interesse:')
    print(list_interest)


df[var] = df[var].fillna(False).astype(str)

df = df.pivot(
    index=['acquirer_array', 'reference_date'],
    columns=var,    
)

df.columns = df.columns.droplevel(0)

df['Total'] = df.sum(axis=1)

df = (df.divide(df['Total'], axis=0)*100).round(2)
df[['True']]
#%%

df = df['True'].pivot(
    index=['acquirer_array'],
    columns='reference_date',
    values='value'
).fillna(0)
df


#%%

df['ref_dt_str'] = df['reference_date'].dt.strftime('%m-%y').str.lower()


df = df.pivot(
    index=['acquirer_array'],
    columns='ref_dt_str',
    values='value',
)

#df['diff'] = df_pivot['2025-08-01'] - df_pivot['2025-07-01']
df['diff_AUG_JUL'] = df['08-25'] - df['07-25']
df['growth_AUG_JUL'] = df['diff_AUG_JUL'] *100 / (df['07-25'] + 1e-9)
display(df)


#%%

treat_data_total_and_plot(df_filter, metric_name)


#%% Gráficos por adquirente
df = treat_date_plot_adq(df_filter)

if df is None:
    raise ValueError("A função treat_date_plot_adq deve retornar um DataFrame")


list_interest = [
    'Cielo', 'GetNet', 
    'Stone', 
    'MercadoPago',
    'PagSeguro', 
    'Rede', 'SumUp',
    'Ton', 
    'StoneCo', 
    'Pagarme',
    'InfinitePay',

]

def post_treat_plot(g):
    g.set(ylim=(0, None))
    g.set_titles(template='{col_name}')
    for ax in g.axes.flat:
        ax.yaxis.set_major_formatter(EngFormatter())
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b%y'))
    g.figure.suptitle(f'{metric_name} - por adquirente', y=1, va='bottom', fontweight='bold', fontsize=16)

    plt.show()
    return g



g = sns.relplot(data=df,
            x='reference_date', 
            y='value', 
            #hue='version',
            col='acquirer_array',
            col_wrap=3,
            col_order=[x for x in df['acquirer_array'].unique() if x in list_interest],
            marker='o',
            kind='line',
            height=3,
            aspect=2,

            facet_kws={'sharey': False},
            )
                
post_treat_plot(g)



g = sns.relplot(data=df[df['reference_date']>='2025-03-01'],
            x='reference_date', 
            y='value', 
            #hue='version',
            col='acquirer_array',
            col_wrap=3,
            col_order=[x for x in df['acquirer_array'].unique() if x not in list_interest],
            marker='o',
            kind='line',
            height=3,
            aspect=2,

            facet_kws={'sharey': False},
            )
                
post_treat_plot(g)


#%% Tabela de quantidades


display_table_qtd(df_filter)


#%% Divisão de balcão

df = df_filter.copy()
#df = df[df['document_type']=='CNPJ']
df['version'] = 'v2'
df = df[df['acquirer_array'].notnull()]

df['qtd'] = df['value']
acquirers_of_interest = ['InfinitePay', 'PagSeguro', 'MercadoPago', 'Rede', 
                        'GetNet',
                        'Ton', 'Stone', 'Pagarme', 
                        ]


for adq in acquirers_of_interest:
        df[f'has_{adq}'] = df['acquirer_array'].apply(lambda x: adq in x)

def get_div_balcao(df, adq_principal):
    df = df.copy()
    print('Criando dados para', adq_principal)

    df['only'] = df['acquirer_array']==adq_principal

    df['acquirer_array'] = df['acquirer_array'].str.split(', ')



    df = df[df['acquirer_array'].apply(lambda x: adq_principal in x if isinstance(x, list) else False
                                    
                                    )].assign(adq_origem=adq_principal)




    df = pd.concat(
        [df.assign(adq_destino=adq, has_destino=df[f'has_{adq}']) for adq in acquirers_of_interest]
        
        + [df.assign(adq_destino='Ninguém', has_destino=df['only'])]
    )
    return df



df = [get_div_balcao(df, adq) for adq in ['Stone', 'Ton', 'InfinitePay', 'Pagarme', 'PagSeguro']]
df = pd.concat(df)


df = df.groupby(['version', 'reference_date', 'adq_origem', 'adq_destino', 'has_destino']).agg({'qtd': 'sum'}).reset_index()
df['qtd_total'] = df.groupby(['version', 'reference_date', 'adq_origem', 'adq_destino'])['qtd'].transform('sum')

df['share'] = df['qtd'] *100 / df['qtd_total']



df = df[df['has_destino']]


df = df[df['adq_origem'] != df['adq_destino']]

df['ref_dt_str'] = df['reference_date'].dt.strftime('%b%y').str.lower()
unique_dt_str = df['ref_dt_str'].unique()[0]


df['date_version'] = df['ref_dt_str'] 


g =sns.catplot(data=df[df['reference_date']>='2024-10-01'],
            x='adq_origem', 
            y='share', 
            col='adq_destino',
            #row='adq_origem',
            col_wrap=3,
            hue='date_version',
            kind='bar',
            #style='ref_dt_str',
            height=3,
            aspect=2,
            dodge=True,
            sharey=False,
            #facet_kws={'xlim': (0, None)},
            ).set(ylim=(0, None),
                    ylabel='(%)',
                    xlabel='Adquirente Origem',
                    )

g.set_titles(template='Divide com {col_name}')




g.figure.suptitle(f'Share de clientes que dividem balcão - {metric_name} ', y=1, va='bottom', fontweight='bold', fontsize=16)
plt.show()

#%%


def criar_matrix_prevalencia(df):
    """
    Cria uma matriz de prevalência de coocorrência entre itens em listas.
    
    Parâmetros:
    df : DataFrame
        DataFrame contendo uma coluna 'itens' com listas de itens e uma coluna 'qtd' com a quantidade de ocorrências.
    
    Retorna:
    DataFrame
        Matriz de prevalência normalizada por linha, incluindo uma coluna 'só' para casos em que o item aparece sozinho.
    """
    # universo de itens
    itens = sorted({x for xs in df["itens"] for x in xs})

    # totais por item (denominador de cada linha)
    expl = df[["qtd"]].join(df["itens"]).explode("itens")
    totais = expl.groupby("itens")["qtd"].sum().reindex(itens).astype(float)

    # coocorrências ponderadas por qtd (inclui diagonal)
    cooc = pd.DataFrame(0.0, index=itens, columns=itens)
    for _, row in df.iterrows():
        s, q = row["itens"], row["qtd"]
        # diagonal: presença do próprio item
        for i in s:
            cooc.at[i, i] += q
        # pares dirigidos (preenche A→B e B→A)
        for i, j in itertools.permutations(s, 2):
            cooc.at[i, j] += q

    # “só”: casos em que o item aparece sozinho
    solo = (
        df[df["itens"].str.len() == 1]
        .assign(only=lambda d: d["itens"].str[0])
        .groupby("only")["qtd"].sum()
        .reindex(itens).fillna(0)
    )

    # matriz de prevalência (normalizada por linha)
    prev = cooc.div(totais, axis=0)
    prev["só"] = (solo / totais).fillna(0)

    # (opcional) ordenar colunas como itens + 'só'
    prev = prev.reindex(columns=itens + ["só"])

    return prev



def save_matrix_prevalencia_acquirers_jul2025(df_filter, metric_name):
    df = df_filter.copy()
    #df = df[df['document_type']=='CNPJ']
    df = df[df['acquirer_array'].notnull()]
    df = df[df['reference_date']=='2025-07-01']

    df['qtd'] = df['value']



    df = df.groupby(['reference_date', 'acquirer_array']).agg({'qtd': 'sum'}).reset_index()
    df['itens'] = df['acquirer_array'].str.split(', ')
    matrix_prevalencia = criar_matrix_prevalencia(df)


    mp = (matrix_prevalencia*100).round(1)
    mp = mp[mp>1]
    mp_str = mp.astype(str) + '%'
    mp_str = mp_str.replace('nan%', '-')
    mp_str = mp_str.replace('100.0%', '')
    mp_str = mp_str.replace('<NA>%', '0%')

    mp_str.to_csv(f'matrix_prevalencia_acquirers_jul2025_{metric_name}.csv')
    print('Arquivo com divisão de balcão salvo!')




#%%


def teste_qtd_stone():
        
    def update_data_test():
        download_data('teste2', 
                    update=True)  

    df = read_data('teste2')
    df = df.drop(columns=['f0_', 'qtd_id_stone_distinct', 'qtd_id_new_distinct'])

    df = df.sort_values(['reference_date'])

    df.loc[df['reference_date'] == '2025-08-31', 'qtd_id_new'] =  df['qtd_id_stone']
    df.loc[df['reference_date'] == '2025-08-31', 'qtd_empresa_new'] =  df['qtd_empresa_stone']
    df.loc[df['reference_date'] == '2025-08-31', 'qtd_mmhid_new'] =  df['qtd_mmhid_stone']

    df = df.melt(
        id_vars=['reference_date'],
        value_vars=[x for x in df.columns if x not in ['reference_date']],
        var_name='metric',
        value_name='value'
    )


    df['tipo'] = df['metric'].str.split('_').str[1:].str.join('_')

    df['metric'] = df['tipo'].str.split('_').str[0]
    df['tipo'] = df['tipo'].str.split('_').str[1:].str.join('_')

    df['tipo'] = df['tipo'].replace({
        'new': 'Novo',
        'stone': 'As Is',
        '': ''
    })

    g = sns.relplot(data=df[df['tipo']==''],
                x='reference_date', 
                y='value', 
                hue='metric',
                marker='o',
                kind='line',
                aspect=2,
                facet_kws={'sharey': False}
                )
    g.figure.suptitle(f'Quantidades total', y=1, va='bottom', fontweight='bold', fontsize=16)




    g = sns.relplot(data=df[df['tipo']!=''],
                x='reference_date', 
                y='value', 
                hue='tipo',
                marker='o',
                kind='line',
                col='metric',
                col_wrap=1,
                aspect=2,
                facet_kws={'sharey': False}
                )
    g.figure.suptitle(f'Quantidades Stone', y=1, va='bottom', fontweight='bold', fontsize=16)



    df_pivot = df[df['tipo']!=''].pivot(
        index=['reference_date', 'metric'],
        columns='tipo',
        values='value'
    )
    df_pivot['diff'] = df_pivot['Novo'] - df_pivot['As Is']

    df_pivot = df_pivot.reset_index()

    g = sns.relplot(data=df_pivot,
                x='reference_date', 
                y='diff', 
                hue='metric',
                marker='o',
                kind='line',
                aspect=2,
                facet_kws={'sharey': False}
                )
    g.figure.suptitle(f'Quantidades Adições ECs Stone', y=1, va='bottom', fontweight='bold', fontsize=16)

    df_pivot = df_pivot[df_pivot['reference_date']>='2025-03-01']
    df_pivot = df_pivot[df_pivot['metric']=='mmhid']
    df_pivot