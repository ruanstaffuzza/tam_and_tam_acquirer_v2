#%%
import sys
import os
import numpy as np

# Add dir to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), "code_vm"))

from agrupamento_nomes import group_names_df_v2, business_names # type: ignore
from agrupamento_nomes_v2 import group_names_df_v2 as group_names_df_v3 #type: ignore
import pandas as pd
from IPython.display import display


#%%

cod_muni = 3541406
df_orig = pd.read_parquet(f'teste_agrupamento_novo_{cod_muni}.parquet')



from agrupamento_nomes_v2 import group_names_df_v2 as group_names_df_v3 #type: ignore


df = df_orig.copy()
df1 = df[df['inicio'] == df['idx_group']].drop(columns=['idx_group'])

df_final = df_orig[df_orig['inicio'] != df_orig['idx_group']]


df = df1[['cod_muni', 'nome_master', 'inicio']].drop_duplicates()

df = group_names_df_v3(df, group_cols=['inicio', 'cod_muni'], nome_col='nome_master')

df['idx_group'] = df['resultado_names'].apply(lambda x: x[0])
df = df[['cod_muni', 'nome_master', 'inicio', 'idx_group']]

df = df.merge(df1, on=['cod_muni', 'inicio', 'nome_master'], how='left')


df_final = pd.concat([
    df_final.assign(res_group_antigo = False), 
    df.assign(res_group_antigo = True)
    ], ignore_index=True)
df = df_final.copy()

df





#%%

def check_number_groups(df):
    df = df.copy()  
    s = df.groupby('inicio')['res_str'].nunique()

    s = s[s>1]
    print('Quantidade de inicios com apenas um grupo:', (df['inicio'].nunique() - s.shape[0]))
    print('Quantidade de inicios com mais de um grupo:', s.shape[0])

    #histograma
    import matplotlib.pyplot as plt
    plt.hist(s.values, bins=range(1, s.max()+2), align='left', edgecolor='black')
    plt.xlabel('Número de grupos por início')

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
df['id_ton'] = np.nan
df['numero_inicio'] = ''
df['cpf'] = ''
df['cnpj'] = ''



df['inicio'] = df['nome'].str[0]


df['cod_muni'] = '1'
df['merchant_tax_id'] = '39988858892'
df['nome_master'] = df['nome']

df_original = df.copy()

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
    df.loc[df['id_ton'].notnull(), 'id_merge_places'] = 'Ton' + df['id_ton'].astype(str)
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

    df = df.sort_values(by='nome')
    function = group_names_df_v3
    df = df.groupby(LEVEL_GROUP, as_index=False
                    ).apply(function, group_cols='inicio', nome_col='nome')

    
    df = execute_ctx(prepare_data, df)
    df = execute_ctx(init_group_id, df)
    df = execute_ctx(deal_unmerged_places, df)
    df = execute_ctx(deal_merged_places, df)
    df = execute_ctx(deal_merged_docs, df)
    df = execute_ctx(pos_tratamento, df)
    print(f'Main data treat Execution time file {file_name}: {time.time() - start:.2f} seconds')
    return df




df = df_original.copy()
df = main_data_treat_muni_v1(df, 'nome')

verbose = False

if verbose:    
    if 'tam_id' in df.columns:
        df['tam_id'] = df['tam_id'].astype(str).str[:-1]
    display(df)

    df['nome_mmhid'] = df['nome'] + '|' + df['merchant_market_hierarchy_id'].astype(str).replace('<NA>', '  ')

    if 'tam_id' in df.columns:
        display(df.groupby(['tam_id'])['nome_mmhid'].apply(list).reset_index())


#%%


def deal_merged(df, LEVEL_GROUP,
                unmerge_col,
                pre_level, post_level):
    
    df = df.copy()
    gcols = LEVEL_GROUP + [pre_level]

    # Per-group stats
    grp = df.groupby(gcols, dropna=False)[unmerge_col]
    df['nunique_unmerge_col'] = grp.transform(lambda s: s.dropna().nunique())
    df['unique_unmerge_col'] = grp.transform(lambda s: s.dropna().iloc[0] if s.dropna().size else np.nan)

    multi_mask = df['nunique_unmerge_col'] > 1
    
    # Base post_level from pre_level
    df[post_level] = df[pre_level].astype(str)

    # Append both unmerge_col and unique_unmerge_col in one go
    df.loc[multi_mask, post_level] = (
        df.loc[multi_mask, post_level]
        + ' - ' + df.loc[multi_mask, unmerge_col].astype(str)
        + ' - ' + df.loc[multi_mask, 'unique_unmerge_col'].astype(str)
    )

    # Assign group ids based on post_level
    df = assign_group_ids(df, [post_level], final_col=post_level, LEVEL_GROUP=LEVEL_GROUP)

    # Same combined append for group_id
    if 'group_id' in df.columns:
            
        df.loc[multi_mask, 'group_id'] = (
            df.loc[multi_mask, 'group_id'].astype(str)
            + ' - ' + df.loc[multi_mask, unmerge_col].astype(str)
            + ' - ' + df.loc[multi_mask, 'unique_unmerge_col'].astype(str)
        )

    df = df.drop(columns=[unmerge_col, 'unique_unmerge_col', 'nunique_unmerge_col'])
    return df
    

orig_cols = ['nome', 'merchant_market_hierarchy_id', 'subs_asterisk', 'id_ton',
       'numero_inicio', 'cpf', 'cnpj', 'inicio', 'cod_muni', 'merchant_tax_id',
       'nome_master']

can_exclude = ['merchant_market_hierarchy_id', 'id_ton', 'numero_inicio', 'cpf', 'cnpj',
                'merchant_tax_id', 'nome', 'subs_asterisk']
            


df = df_original.copy()
df = df.drop(columns=[] + can_exclude)

df['res_group_antigo'] = True

df['doc_unmerge'] = np.nan

LEVEL_GROUP = ['cod_muni']
    
df = df.sort_values(by='nome_master')
function = group_names_df_v3
df = df.groupby(LEVEL_GROUP, as_index=False
                ).apply(function, group_cols='inicio', nome_col='nome_master')
print(df.columns)
df['group_id'] = df['resultado_names'].apply(lambda x: x[0])
df = assign_group_ids(df, ['group_id'], final_col='idx_group', LEVEL_GROUP=LEVEL_GROUP)


#%%



"""
#df['doc_unmerge'] = df['numero_inicio']
#mask = df['subs_asterisk'].isin(['Outros_Stone', 'Ton']) & df['res_group_antigo']
#df.loc[mask, 'doc_unmerge'] = coalesce(df[mask], ['cnpj', 'cpf'])
#df.loc[mask, 'doc_unmerge'] = df.loc[mask, 'cnpj'].fillna(df['cpf'])
"""

display(df)


def prepare_data(df):
    return df.copy()




def deal_merged_docs_v2(df, LEVEL_GROUP):
    df = df.copy()


    pre_level = 'idx_group'
    post_level = 'group_idx_unmerge_docs'
    unmerge_col = 'doc_unmerge'

    df = deal_merged(df, LEVEL_GROUP, unmerge_col, pre_level, post_level)

    #df['tam_id'] = (df['group_idx_unmerge_docs'].astype(str) + df['cod_muni'].astype(str)).astype(int)
    return df


def main_data_treat_muni_v2(df, file_name):

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


    df = execute_ctx(deal_merged_docs_v2, df)

    df = execute_ctx(pos_tratamento, df)
    print(f'Main data treat Execution time file {file_name}: {time.time() - start:.2f} seconds')
    return df


df = main_data_treat_muni_v2(df, 'nome')
df
#%%
from utils import download_data, read_data




def create_tam_id_orig(df):
    df['tam_id'] = df.groupby(['group_idx_unmerge_docs']).ngroup()+1
    return df

def create_tam_id(df, LEVEL_GROUP):
    df = df.groupby(LEVEL_GROUP, group_keys=False,
                    ).apply(lambda x: create_tam_id_orig(x))
    return df

def pos_tratamento(df, LEVEL_GROUP):
    df = df.copy()
    
    df = create_tam_id(df, LEVEL_GROUP)
    df['tam_id'] = (df['tam_id'].astype(str) + df['cod_muni'].astype(str)).astype(int)

    return df




from agrupamento_nomes_v2 import group_names_df_v2 as group_names_df_v3 #type: ignore



download_data('sample_final_nomes_completo')

cod_muni = 3541406
df_orig = pd.read_parquet(f'teste_agrupamento_novo_{cod_muni}.parquet')


df = df_orig.copy()
df1 = df[df['inicio'] == df['idx_group']].drop(columns=['idx_group'])

df_final = df_orig[df_orig['inicio'] != df_orig['idx_group']]


df = df1[['cod_muni', 'nome_master', 'inicio']].drop_duplicates()

df = group_names_df_v3(df, group_cols=['inicio', 'cod_muni'], nome_col='nome_master')

df['idx_group'] = df['resultado_names'].apply(lambda x: x[0])
df = df[['cod_muni', 'nome_master', 'inicio', 'idx_group']]

df = df.merge(df1, on=['cod_muni', 'inicio', 'nome_master'], how='left')

df_final = pd.concat([
    df_final.assign(res_group_antigo = False), 
    df.assign(res_group_antigo = True)
    ], ignore_index=True)


#%%

df1 = df_final.copy()
df1 = df1[df1['nome_master']=='AAAKKKKK']


df2 = read_data('sample_final_nomes_completo')
df2 = df2[df2['nome_master']=='AAAKKKKK']

cols = ['mmhid_merge', 'nome_master']
df1 = df1[cols]
df2 = df2[cols]


res = df1.merge(df2, on='mmhid_merge', how='outer', indicator=True)


print('Descrevendo o merge:')
print('Comecei com o df1')
print(df1)
print('E o df2')
print(df2)
print('O resultado do merge foi:')
print(res)
#%%
df = df_final.merge(read_data('sample_final_nomes_completo'), 
              on=['cod_muni', 'inicio', 'nome_master', 'is_mp', 'mmhid_merge'])

df['doc_unmerge'] = df['pre_doc_unmerge'].where(df['res_group_antigo'])

df = main_data_treat_muni_v2(df, file_name='sample_final_nomes_completo')

#%%




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


def get_completed(OUT_DIR):
    files = os.listdir(OUT_DIR)
    files = [f for f in files if f.endswith('.parquet')]
    #files = sorted(OUT_DIR.glob("cod_muni=*.parquet"))
    cod_munis = [int(f.split('=')[1].split('.')[0]) for f in files]
    return cod_munis
