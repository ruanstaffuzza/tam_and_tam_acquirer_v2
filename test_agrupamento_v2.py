#%%
import sys
import os

# Add dir to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), "code_vm"))

from agrupamento_nomes import group_names_df_v2, business_names # type: ignore
from agrupamento_nomes_v2 import group_names_df_v2 as group_names_df_v3 #type: ignore
import pandas as pd
from IPython.display import display


def example_new(function):
    
    # Dados de exemplo
    data = {
        'nome': business_names
    }

    df = pd.DataFrame(data)
    df['inicio'] = df['nome'].str[0]

    df = df.sort_values(by='nome')

    df = function(df, group_cols='inicio', nome_col='nome')

    #df = df.drop(columns=['inicio', 'retirado_de_resultado', 'resultado_names'])
     #Exibir o DataFrame resultante

    return df

def display_examples():
        
    df = example_new(group_names_df_v2)
    display(df)


    df = example_new(group_names_df_v3)
    display(df)

#%%

def check_time():
        
    import timeit

    # tempo da func1
    tempo1 = timeit.timeit("example_new(group_names_df_v2)", globals=globals(), number=10000)
    # tempo da func2
    tempo2 = timeit.timeit("example_new(group_names_df_v3)", globals=globals(), number=10000)

    print(f"group_names_df_v2: {tempo1:.5f} segundos")
    print(f"group_names_df_v3: {tempo2:.5f} segundos")

    """group_names_df_v2: 102.76884 segundos
       group_names_df_v3: 71.47461 segundos"""


#%%

from group_tam_id import (
    load_to_gbq,
    group_and_clean,

    deal_merged_places,
    deal_merged_docs,
    pos_tratamento,
    execute_with_context,
    assign_group_ids,
)

import time
import numpy as np

def prepare_data(df):
    return df.copy()


def init_group_id(df, LEVEL_GROUP):

    QTD_NOME_COMUM = 20
    df = df.copy()

    nomes_comuns = df['nome_master'].value_counts()
    nomes_comuns = nomes_comuns[nomes_comuns>QTD_NOME_COMUM].index.to_list()

    df['group_id'] = df['resultado_names'].apply(lambda x: list(np.sort(x))[-1]) 

    df['new_id'] = np.arange(df.shape[0])
    df.loc[df['nome_master'].isin(nomes_comuns), 'group_id'] = 'Nome Comum: ' + df['new_id'].astype(str)

    df = df.rename(columns={'resultado_names': 'grouped_names'})
    df = df.drop(columns=['new_id'])

    df = assign_group_ids(df, ['group_id'], final_col='group_idx_nome', LEVEL_GROUP=LEVEL_GROUP)

    return df





def deal_unmerged_places(df, LEVEL_GROUP):
    df = df.copy()


    df['id_merge_places'] = df['merchant_market_hierarchy_id'].apply(lambda x: str(x) if not pd.isna(x) else np.nan) #deal with nan in Int
    df['new_id'] = np.arange(df.shape[0])
    df.loc[df['subs_asterisk']=='Ton', 'id_merge_places'] = 'Ton' + df['id_ton'].astype(str)
    df.loc[df['id_merge_places'].isnull(), 'id_merge_places'] = 'created' + df['new_id'].astype(str)


    nome_grupo = 'group_idx_merge_places'

    df = assign_group_ids(df, ['group_idx_nome', 'id_merge_places'], final_col=nome_grupo, LEVEL_GROUP=LEVEL_GROUP)
    

    return df


def main_data_treat_muni_v1(df, file_name):

    """
    Main function to process data with options to display intermediate DataFrames and print verbose messages.

    Parameters:
    - read_data_func: Function to read initial data.
    - display_flag: Boolean flag indicating whether to display the DataFrame after each processing step.
    - verbose: Boolean flag indicating whether to print verbose messages during processing.
    """
    display_flag=False

    verbose = True
    LEVEL_GROUP = ['cod_muni']
    
    
    print(f'Starting main data treat for file {file_name}, with size {len(df)}')

    start = time.time()
    execute_ctx = execute_with_context(LEVEL_GROUP, display_flag=display_flag, verbose=verbose)
    
    df = execute_ctx(prepare_data, df)
    df = execute_ctx(init_group_id, df)
    df = execute_ctx(deal_unmerged_places, df)
    df = execute_ctx(deal_merged_places, df)
    #df = execute_ctx(deal_merged_docs, df)
    #df = execute_ctx(pos_tratamento, df)
    print(f'Main data treat Execution time file {file_name}: {time.time() - start:.2f} seconds')
    return df


business_names = [
 'ANDREINA',
 'ANDREIA',
 'ANDREIAESTETICAEB',
 'ANDREIAESTETICAEBRONZ',
 'AUTOCAR',
 'AUTOCARRO1',
 'AUTOCARCENTRO',
 'AUTOCARRO2'
]


# Dados de exemplo
data = {
    'nome': business_names
}

df = pd.DataFrame(data)

mmhid_groups = {
    #'1': ['AUTOCARCENTRO', 'AUTOCARRO1'],
    #'2': ['ANDREIA', 'ANDREINA'],
    '3': ['AUTOCARRO2', 'ANDREIA'],
    '4': ['AUTOCAR', 'ANDREIAESTETICAEB'],

}

df['merchant_market_hierarchy_id'] = df['nome'].apply(
    lambda x: next((key for key, values in mmhid_groups.items() if any(v == x for v in values)), pd.NA)
)

df['subs_asterisk']= 'Outros'
df['id_ton'] = 'x'
df['numero_inicio'] = ''
df['cpf'] = ''
df['cnpj'] = ''



df['inicio'] = df['nome'].str[0]



df = df.sort_values(by='nome')

function = group_names_df_v3
df = function(df, group_cols='inicio', nome_col='nome')


df['cod_muni'] = '1'
df['merchant_tax_id'] = '39988858892'
df['nome_master'] = df['nome']

display(df)


df = main_data_treat_muni_v1(df, 'nome')
if 'tam_id' in df.columns:
    df['tam_id'] = df['tam_id'].astype(str).str[:-1]
display(df)

df['nome_mmhid'] = df['nome'] + '|' + df['merchant_market_hierarchy_id'].astype(str).replace('<NA>', '  ')

if 'tam_id' in df.columns:
    display(df.groupby(['tam_id'])['nome_mmhid'].apply(list).reset_index())


