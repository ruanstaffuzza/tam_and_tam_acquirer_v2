#%%
from utils import download_data, read_data

def update_data():
    download_data('test_versions', 
                  dict_query={'list_ref_months': "'2025-07-31', '2025-06-30', '2025-08-31'"},
                  update=True)
    
    download_data('test_version_v2', 
                  update=True)
import matplotlib.pyplot as plt
import seaborn as sns

#EngFormatter
from matplotlib.ticker import EngFormatter
import matplotlib.dates as mdates

unique_acq = ['99Food', 'Aliexpress', 'Amazon', 'Americanas', 'BMG/Granito',
       'BRB', 'Bolt', 'Cielo', 'EC', 'Fiserv', 'GetNet', 'Hotmart',
       'InfinitePay', 'Magalu1', 'Magalu2', 'MercadoLivre1',
       'MercadoLivre2', 'MercadoPago', 'MercadoPago_mmhid_problema',
       'PagSeguro', 'Pagarme', 'Rappi', 'Rede', 'Safra', 'Shein', 'Shopee', 'Sipag',
       'Sipag/Sicredi', 'Stone', 'SumUp', 'Ton', 'Tribanco', 'Vero', None]


unique_acq_v2 = [
    '99Food', 'Aliexpress', 'Amazon', 'Americanas', 'BMG/Granito', 'BRB', 'Bolt',
       'Cielo', 'EC', 'Fiserv', 'GetNet', 'Hotmart', 'InfinitePay',
       'MP_mmhid_problema', 'Magalu1', 'Magalu2', 'MercadoLivre1',
       'MercadoLivre1_mmhid_problema', 'MercadoLivre2', 'MercadoPago',
       'MercadoPago_mmhid_problema', 'PagSeguro', 'Pagarme', 'Rappi',
       'Rede', 'Safra', 'Shein', 'Shopee', 'Sipag', 'Sipag/Sicredi',
       'Stone', 'SumUp', 'Ton', 'Tribanco', 'Vero'
       ]


diff = [x for x in unique_acq if x not in unique_acq_v2]
print(diff)
#%%
import pandas as pd
import numpy as np

table_choice = 'test_version_v2'

nome_mp = 'MercadoPago + mmhid_problema'
nome_mp = 'MercadoPago'


#df = read_data('test_versions')
df = read_data(table_choice)

if table_choice == 'test_version_v2':
    df['qtd_empresa'] = df['qtd_ec']

df = df[df['is_osasco'].fillna(False)==False]
df['version'] = 2
df = df[df['reference_date'].isin(df[df['version']==2].reference_date)]
df['reference_date'] = df['reference_date'].dt.to_period('M').dt.to_timestamp()


if table_choice == 'test_versions':
    df = pd.concat([
        df[df['version']==1],
        df[(df['version']==2) & (df['reference_date']>'2025-06-01')],
        df[(df['version']==1) & (df['reference_date']=='2025-06-01')].assign(version=2)
    ])




def tranform_mercadopago(x:str) -> str:
    if pd.isna(x):
        return x
    
    x = x.split(', ')
    
    dict_map = {
        'MP_mmhid_problema': 'MercadoPago + mmhid_problema',
        'MercadoPago' : 'MercadoPago + mmhid_problema',
    }

    x = [dict_map.get(i, i) for i in x]
    return ','.join(np.sort(list(set(x))))

if nome_mp == 'MercadoPago + mmhid_problema':
    df['acquirer_array'] = df['acquirer_array'].apply(tranform_mercadopago)





#df['acquirer_array'] = df['acquirer_array'].str.split(', ')
#drop duplicates
#df = df[df['acquirer_array'].notnull()]
df_orig = df.copy()


#%%


df = df_orig.copy()




def only_with_acquirer(df, list_acq):
    df = df.copy()
    df['acquirer_array'] = df['acquirer_array'].str.split(', ')
    return df['acquirer_array'].apply(lambda x: all([i in list_acq for i in x]))


test_list = ['99Food', 'Aliexpress', 'Amazon', 'Americanas', 'Hotmart', 'Magalu1', 'Magalu2', 'MercadoLivre1',
                'MercadoLivre2',  'Shein', 'Shopee',
                'MP_mmhid_problema', 'MercadoLivre1_mmhid_problema',
                    'MercadoPago_mmhid_problema',
                    'Ifood',
                #'EC'
                ]


def tranform_mmhid_prob(x:str) -> str:
    if pd.isna(x):
        return x
    
    x = x.split(', ')
    
    dict_map = {
        'MercadoLivre1_mmhid_problema': 'MercadoLivre1_mmhid_problema',
        'MP_mmhid_problema' : 'ML',
        'MercadoPago_mmhid_problema': 'ML',
    }

    x = [dict_map.get(i, i) for i in x]
    return ','.join(np.sort(list(set(x))))


df['acquirer_array'] = df['acquirer_array'].apply(tranform_mmhid_prob)

df = df[df['acquirer_array']!='ML']



df['apenas_mktplace'] = only_with_acquirer(df, test_list)
df[df['apenas_mktplace']]['acquirer_array'].value_counts()

#%%

df = df[~df['apenas_mktplace']]


#%%
df = df_orig.copy()
df_total = df.groupby(['version', 'reference_date']).agg({'qtd_ec': 'sum', 'qtd_empresa': 'sum'}).reset_index()
df_total = df_total.melt(
    id_vars=['version', 'reference_date'],
    value_vars=['qtd_ec', 
                #'qtd_empresa'
                ],
)

g = sns.relplot(data=df_total, 
            x='reference_date', 
            y='value', 
            hue='version', 
            col='variable',
            marker='o',
            kind='line',
            palette='tab10',
            )

for ax in g.axes.flat:
    ax.yaxis.set_major_formatter(EngFormatter())
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b%y'))

#%%

df_por_adq = df.copy()
df_por_adq['acquirer_array'] = df_por_adq['acquirer_array'].str.split(', ')


def stone_or_ton_to_st(x:list):
    
    dict_map = {
        'Stone': 'StoneCo',
        'Ton': 'StoneCo',
        'Pagarme': 'StoneCo'
    }

    x = [dict_map.get(i, i) for i in x]
    return list(set(x))


#df_por_adq['acquirer_array'] = df_por_adq['acquirer_array'].apply(stone_or_ton_to_st)


df_por_adq = df_por_adq.explode('acquirer_array').groupby(['version', 'reference_date', 'acquirer_array']).agg({'qtd_ec': 'sum', 'qtd_empresa': 'sum'}).reset_index()
df_por_adq = df_por_adq[df_por_adq['acquirer_array'].notnull()]


list_interest = [
    'Cielo', 'GetNet', 
    'Stone', 
    nome_mp,
    'PagSeguro', 
    #'MercadoPago', 
    'Rede', 'SumUp',
    'Ton', 
    'StoneCo', 
    'Pagarme',
    'InfinitePay',

]

df_por_adq = df_por_adq[df_por_adq['acquirer_array'].isin(list_interest)]

df_por_adq = df_por_adq.melt(
    id_vars=['version', 'reference_date', 'acquirer_array'],
    value_vars=['qtd_ec', 'qtd_empresa'],
    var_name='metric',
    value_name='value'
)

df_por_adq['reference_date'] = df_por_adq['reference_date'].dt.to_period('M').dt.to_timestamp()


g = sns.relplot(data=df_por_adq[df_por_adq['metric']=='qtd_empresa'],
            x='reference_date', 
            y='value', 
            hue='version',
            col='acquirer_array',
            col_wrap=3,
            marker='o',
            kind='line',
            height=3,
            aspect=1.5,
            #dodge=True,
            #style='metric',
            #markers={'qtd_ec': 'o', 'qtd_empresa': 's'},
            palette='tab10',
            facet_kws={'sharey': False},
            ).set(ylim=(0, None))
                  
g.set_titles(template='{col_name}')
for ax in g.axes.flat:
    ax.yaxis.set_major_formatter(EngFormatter())
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b%y'))


#%%



df_por_adq = df.copy()
df_por_adq['acquirer_array'] = df_por_adq['acquirer_array'].str.split(', ')



df_por_adq = df_por_adq.explode('acquirer_array').groupby(['version', 'reference_date', 'acquirer_array']).agg({'qtd_ec': 'sum', 'qtd_empresa': 'sum'}).reset_index()
df_por_adq = df_por_adq[df_por_adq['acquirer_array'].notnull()]


df_por_adq = df_por_adq.melt(
    id_vars=['version', 'reference_date', 'acquirer_array'],
    value_vars=['qtd_ec', 'qtd_empresa'],
    var_name='metric',
    value_name='value'
)


df_pivot = df_por_adq[df_por_adq['metric']=='qtd_empresa'].pivot(
    index=['reference_date', 'acquirer_array'],
    columns='version',
    values='value',
)
df_pivot['diff'] = df_pivot[2] - df_pivot[1]
df_pivot['diff_pct'] = df_pivot['diff'] *100 / df_pivot[1]
df_pivot

#%%

df = df_orig.copy()

df['apenas_mktplace'] = only_with_acquirer(df, test_list)


df = df[~df['apenas_mktplace']]

df = df[df['version']==2]

df = df.copy()
df['acquirer_array'] = df['acquirer_array'].str.split(', ')



df = df.explode('acquirer_array').groupby(['version', 'reference_date', 'acquirer_array']).agg({'qtd_ec': 'sum', 'qtd_empresa': 'sum'}).reset_index()
df = df[df['acquirer_array'].notnull()]


df = df.melt(
    id_vars=['version', 'reference_date', 'acquirer_array'],
    value_vars=['qtd_ec', 'qtd_empresa'],
    var_name='metric',
    value_name='value'
)

df['ref_dt_str'] = df['reference_date'].dt.strftime('%b%y').str.lower()


df = df[~df['acquirer_array'].isin(test_list)]

df = df[df['metric']=='qtd_empresa'].pivot(
    index=['version', 'acquirer_array'],
    columns='ref_dt_str',
    values='value',
)

#df['diff'] = df_pivot['2025-08-01'] - df_pivot['2025-07-01']
df['diff'] = df['aug25'] - df['jul25']
df



#%%

from utils import high_contrast_color_palette

df = df_orig.copy()

test_list = ['99Food', 'Aliexpress', 'Amazon', 'Americanas', 'Hotmart', 'Magalu1', 'Magalu2', 'MercadoLivre1',
                'MercadoLivre2', 
                'MercadoPago_mmhid_problema', 
                'Shein', 'Shopee',
                'MP_mmhid_problema', 
                'MercadoLivre1_mmhid_problema',
                'EC']



df_por_adq = df.copy()
df_por_adq['acquirer_array'] = df_por_adq['acquirer_array'].str.split(', ')


df_por_adq = df_por_adq.explode('acquirer_array').groupby(['version', 'reference_date', 'acquirer_array']).agg({'qtd_ec': 'sum', 'qtd_empresa': 'sum'}).reset_index()
df_por_adq = df_por_adq[df_por_adq['acquirer_array'].notnull()]






df_por_adq = df_por_adq[df_por_adq['acquirer_array'].isin(test_list)]

test_list = [x for x in test_list if x in df_por_adq['acquirer_array'].unique()]

g = sns.catplot(data=df_por_adq,
            x='reference_date', 
            y='qtd_empresa', 
            hue='acquirer_array',
            #col_wrap=3,
  
            kind='bar',
            height=3,
            aspect=1.5,
            #facet_kws={'sharey': False},
            palette=high_contrast_color_palette()[:len(test_list)],
            ).set(ylim=(0, None))

for ax in g.axes.flat:
    ax.yaxis.set_major_formatter(EngFormatter())
    #ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    #ax.xaxis.set_major_formatter(mdates.DateFormatter('%b%y'))



#%%

def update_data_company():
    download_data('teste_stoneco',
                  dict_query={'list_ref_months': "'2025-04-30', '2025-06-30'"},
                  update=True)
    
df = read_data('teste_stoneco')


ref_date_choice = '2025-04-30'
df = df[df['reference_date']==ref_date_choice]

df.loc[df['version']==1, 'has_pagarme_tpv'] = False

for company in [
    'pagarme',
    'stone',
    'ton'
                ]:
    df[f'has_{company}_tpv'] = df[f'has_{company}_tpv'].fillna(False)
    df[f'has_{company}'] = df[f'has_{company}'].fillna(False)


# Correção 1: se não tem tpv não tá no TAM
for company in [
    'pagarme',
    'stone',
    'ton'
                ]:
    df.loc[df[f'has_{company}_tpv']==False, f'has_{company}'] = False





df['share'] = df['qtd_documents'] *100 / df.groupby(['version', 'reference_date'])['qtd_documents'].transform('sum')

ton_correto = df['has_ton'] == df['has_ton_tpv']
stone_correto = df['has_stone'] == df['has_stone_tpv']
pagarme_correto = df['has_pagarme'] == df['has_pagarme_tpv']

df['correto'] = (ton_correto & stone_correto & pagarme_correto).fillna(False)


df.groupby(['version', 'correto']).agg({'qtd_documents': 'sum', 'share': 'sum'}).reset_index()


#%%
import pandas as pd
df = df_orig.copy()
#df = df[df['document_type']=='CNPJ']

df['qtd'] = df['qtd_ec']
acquirers_of_interest = ['InfinitePay', 'PagSeguro', nome_mp, 'Rede', 
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


df['date_version'] = df['ref_dt_str'] + ' - v' + df['version'].astype(str)
#%%

df = df[df['version']==2]
g =sns.catplot(data=df,
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


g.figure.suptitle(f'Share de clientes que dividem balcão - {unique_dt_str}', y=1, va='bottom', fontweight='bold', fontsize=16)
#g.set_titles('{col_name}|{row_name}')

#%%


download_data('check_qtd_cloudwalk')

df = read_data('check_qtd_cloudwalk')
df