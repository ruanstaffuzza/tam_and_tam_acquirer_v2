#%%

from utils import download_data, read_data

import sys
import os
import numpy as np

# Add dir to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), "code_vm"))
from agrupamento_nomes_v2 import group_names_df_v2 as group_names_df_v3 #type: ignore

import pandas as pd
from IPython.display import display

download_data('sample_final_nomes_completo')

from agrupamento_nomes import group_names_df_v2 as group_names_df_v3 #type: ignore
import os
import pandas as pd
from pathlib import Path
import time 
import numpy as np




def processar_grupos(df_orig: pd.DataFrame) -> pd.DataFrame:

    df = df_orig.copy()
    df['qtd_nome_sem_mmhid'] = df[df['mmhid_merge'].isna()].groupby(['cod_muni', 'idx_group'])['nome_master'].transform('nunique')
    df['qtd_nome_sem_mmhid'] = df.groupby(['cod_muni', 'idx_group'], dropna=True)['qtd_nome_sem_mmhid'].transform('max')
    mask = (df['inicio'] == df['idx_group']) | (df['qtd_nome_sem_mmhid'] > 20)
             
    df1 = df[mask].drop(columns=['idx_group'])
    
    df_final = df_orig[~mask]


    df = df1[['cod_muni', 'nome_master', 'inicio']].drop_duplicates()
    df = group_names_df_v3(df, group_cols=['inicio', 'cod_muni'], nome_col='nome_master')
    df['idx_group'] = df['resultado_names'].apply(lambda x: x[0])
    df = df[['cod_muni', 'nome_master', 'inicio', 'idx_group']]
    df = df.merge(df1, on=['cod_muni', 'inicio', 'nome_master'], how='left')

    df_final = pd.concat([
        df_final.assign(res_group_antigo=False), 
        df.assign(res_group_antigo=True)
    ], ignore_index=True)

    return df_final



    
from group_tam_id import (
    load_to_gbq,
    pos_tratamento,
)



import pandas as pd
from google.cloud import bigquery
import datetime

def main_get_data(cod_munis, OUT_DIR, DF_COMP):

    fnames = [OUT_DIR / f"cod_muni={cod_muni}.parquet" for cod_muni in cod_munis]
    df_orig = pd.concat([pd.read_parquet(fname) for fname in fnames], ignore_index=True)
    
    df_completo = DF_COMP[DF_COMP['cod_muni'].isin(cod_munis)]
    return df_orig, df_completo



 
#%%
cod_muni = 3541406
df_orig = pd.read_parquet(f'teste_agrupamento_novo_{cod_muni}.parquet')

#%%
LEVEL_GROUP = ['cod_muni']


def filter_example1(df):
    return df[df['idx_group'] == '890232625']





df_final = processar_grupos(df_orig)


df_completo = read_data('sample_final_nomes_completo')


df = df_final.merge(df_completo, 
              on=['cod_muni', 'inicio', 'nome_master', 'is_mp', 'mmhid_merge'])
    
#df['doc_unmerge'] = df['pre_doc_unmerge']#.where(df['res_group_antigo'])

df['doc_unmerge'] = df['pre_doc_unmerge'].where(df['pre_doc_unmerge'].str.len().isin([11,14]))

#df = filter_example1(df)


# Preenche por grupo:
def fill_group(s: pd.Series):
    # valores únicos não nulos na ordem de ocorrência
    uniq = pd.unique(s.dropna())
    # se o grupo for todo NaN, dexa [] mesmo
    rep = list(uniq) if len(uniq) else []
    return s.apply(lambda v: [v] if pd.notna(v) else rep)

    
def deal_merged_doc_teste(df, LEVEL_GROUP,
                unmerge_col,
                pre_level, post_level):
    
    df = df.copy()
    gcols = LEVEL_GROUP + [pre_level]


    start = time.time()
    print('Starting peer group stats')

    # 1) Cálculo por grupo (uma passada + merge)
    df['nunique_unmerge_col'] = (
    df.groupby(gcols, sort=False)[unmerge_col]
      .transform('nunique'))

    multi_mask = df['nunique_unmerge_col'] > 1

    df[post_level] = df[pre_level].astype(str)

    dff = df[multi_mask].copy()
    df = df[~multi_mask].copy()

    

    dff['unique_unmerge_col'] = dff.groupby(gcols)[unmerge_col].transform(lambda x: x.dropna().iloc[0] if x.dropna().size else np.nan)

    print(f'Peer group stats Execution time: {time.time() - start:.1f} seconds')


    # Append both unmerge_col and unique_unmerge_col in one go

    dff[unmerge_col] = dff.groupby(gcols)[unmerge_col].transform(fill_group)
    dff = dff.explode(unmerge_col).reset_index(drop=True)


    dff['post_level'] = (
        dff[post_level]
        + ' - ' + dff[unmerge_col].astype(str)
        + ' - ' + dff['unique_unmerge_col'].astype(str)
    )

    start = time.time()
    print('Starting factorize')

    df = pd.concat([df, dff], ignore_index=True)

    # Assign group ids based on post_level
    df[post_level] = (
        df.groupby(LEVEL_GROUP)['post_level']
          .transform(lambda x: pd.factorize(x)[0] + 1)  # numeração 1,2,3...
    )
    print(f'Factorize Execution time: {time.time() - start:.1f} seconds')

    #df = df.drop(columns=[unmerge_col, 'unique_unmerge_col', 'nunique_unmerge_col'])
    return df


def deal_merged_docs_v2(df, LEVEL_GROUP):
    df = df.copy()


    pre_level = 'idx_group'
    post_level = 'group_idx_unmerge_docs'
    unmerge_col = 'doc_unmerge'

    #df = deal_merged(df, LEVEL_GROUP, unmerge_col, pre_level, post_level)
    df = deal_merged_doc_teste(df, LEVEL_GROUP, unmerge_col, pre_level, post_level)

    #df['tam_id'] = (df['group_idx_unmerge_docs'].astype(str) + df['cod_muni'].astype(str)).astype(int)
    return df


start = time.time()
    
df2 = deal_merged_docs_v2(df, LEVEL_GROUP)
final_time = (time.time() - start)
    
print(f'deal_merged_docs_v2 Execution time: {final_time:.1f} seconds')




df2[df2['idx_group'] == '890232625']


#%%

df2[
    (df2['inicio'] == 'AMAND') 
    #& (df2['nome_merge'].str.startswith('A')) 
    #& (df2['mmhid_merge']== 1032025774)
    & (df2['is_mp'] == False)
    & ((df2['mmhid_choice'] == 1032025774) | (df2['mmhid_merge']== 1032025774))
    #& (df2['qtd_nome_sem_mmhid'] >10)
    ].reset_index(drop=True)#.idx_group.nunique()


#%%
df2[df2['nunique_unmerge_col'] > 1]


#%%



df[
    (df['inicio'] == 'AMAND') 
    #& (df2['nome_merge'].str.startswith('A')) 
    #& (df2['mmhid_merge']== 1032025774)
    & (df['is_mp'] == False)
    & ((df['choice'] == 'A') | (df['mmhid_merge']== 1032025774))
    & (df['qtd_nome_sem_mmhid'] >10)
    ]



#%%

def filter_exemplo(df):
    return df[
    (df['inicio'] == 'AMAND') 
    #& (df2['nome_merge'].str.startswith('A')) 
    #& (df2['mmhid_merge']== 1032025774)
    & (df['is_mp'] == False)
    & ((df['choice'] == 'A') | (df['mmhid_merge']== 1032025774))
    ]




res = processar_grupos(filter_exemplo(df_orig))
res