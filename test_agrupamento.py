#%%


import pandas as pd

#df = pd.read_csv(r"C:\Users\ruan.staffuzza\Downloads\bquxjob_2f3ed6a8_193db61dc81.csv", sep=',')
table = r"C:\Users\ruan.staffuzza\Downloads\ruan_debugar_deal_merged_places.parquet"
df = pd.read_parquet(table)

#%%



import pyarrow as pa
import pyarrow.parquet as pq

table = pq.read_table(table)

# Inspecione o esquema para ver a coluna com dbdate
print(table.schema)

# Supondo que a coluna 'coluna_dbdate' seja do tipo problemático,
# tente convertê-la para um tipo compatível, por exemplo datetime64[ns].
# Primeiro obtenha a coluna, depois aplique um cast:
table = table.set_column(
    table.schema.get_field_index('reference_month'),
    'reference_month',
    pa.compute.cast(table['reference_month'], pa.timestamp('ns'))
)

# Agora converta para pandas
df = table.to_pandas()


print(df.columns)


display(df.head())


#%%


from code_vm.group_tam_id import assign_group_ids, deal_merged_places


OUTROS = ['Outros', 'Outros_Pags', 'Outros_SumUp', 'Outros_Stone']

def deal_merged(df, LEVEL_GROUP,
                unmerge_col,
                pre_level, post_level):
    
    df_grouped = df.copy()

    
    df_grouped = df_grouped[df_grouped[unmerge_col].notnull()]
    df_grouped = df_grouped[LEVEL_GROUP + [pre_level, unmerge_col]].drop_duplicates()
    # Group by the specified columns and aggregate
    df_grouped = df_grouped.groupby(LEVEL_GROUP + [pre_level]).agg(
        unique_unmerge_col=(unmerge_col, 'first'),
        nunique_unmerge_col=(unmerge_col, 'nunique')
    ).reset_index()

    df = df.merge(df_grouped, on=LEVEL_GROUP + [pre_level], how='left')


    df[post_level] = df[pre_level].astype(str)
    
    df.loc[(df['nunique_unmerge_col']>1)
           & (df['subs_asterisk'].isin(OUTROS))
           , post_level] = df[post_level].astype(str) + ' - ' + df[unmerge_col].astype(str)
    
    df.loc[(df['nunique_unmerge_col']>1)
           & (~df['subs_asterisk'].isin(OUTROS))
           , post_level] = df[post_level].astype(str) + ' - ' + df['unique_unmerge_col'].astype(str)

    
    df = assign_group_ids(df, [post_level], final_col=post_level, LEVEL_GROUP=LEVEL_GROUP)

    df.loc[(df['nunique_unmerge_col']>1)
           & (df['subs_asterisk'].isin(OUTROS))
           , 'group_id'] = df['group_id'].astype(str) + ' - ' + df[unmerge_col].astype(str)
    
    df.loc[(df['nunique_unmerge_col']>1)
           & (~df['subs_asterisk'].isin(OUTROS))
           , 'group_id'] = df['group_id'].astype(str) + ' - ' + df['unique_unmerge_col'].astype(str)
    
    df = df.drop(columns=[unmerge_col, 'unique_unmerge_col', 'nunique_unmerge_col'])
    return df
    

def deal_merged_places_v2(df, LEVEL_GROUP):

    df = df.copy()

    df['mmhid_places'] = df.loc[df['subs_asterisk'].isin(OUTROS), 'merchant_market_hierarchy_id']

    pre_level = 'group_idx_merge_places'
    post_level = 'group_idx_unmerge_places'
    unmerge_col = 'mmhid_places'

    df = deal_merged(df, LEVEL_GROUP, unmerge_col, pre_level, post_level)

    return df

#%%

deal_merged_places(df, LEVEL_GROUP=['cod_muni'])

#%%
deal_merged_places_v2(df, LEVEL_GROUP=['cod_muni'])