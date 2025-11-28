#%%
from utils import download_data, read_data
from IPython.display import display

import pandas as pd

def update_data():
    download_data('get_sample_auth', update=True)
    download_data('get_sample_mastercard', update=True)
    download_data('get_sample_ba', update=True)

    download_data('get_ton_auth', update=True)
    download_data('get_ton_mastercard', update=True)



def colapse_ref_date(df): #type: ignore
    cols_df = df.columns.tolist()
    except_ref = [x for x in cols_df if x not in ['reference_date']]
    df['ref_min'] =  df.groupby(except_ref,
                                   dropna=False,
                                   )['reference_date'].transform('min')
    df['ref_max'] = df.groupby(except_ref,
                            dropna=False,
                            )['reference_date'].transform('max')
    df = df[except_ref + ['ref_min', 'ref_max']].drop_duplicates()
    return df



def colapse_ref_date(df):
    cols_df = df.columns.tolist()
    except_ref = [x for x in cols_df if x not in ['reference_date']]
    df['year'] = df['reference_date'].dt.year

    df['reference_date'] = (df['year']-2023).astype(str)  + '.'          +  df['reference_date'].dt.strftime('%m')
    df['ref_dates'] =  df.groupby(except_ref,
                                   dropna=False,
                                   )['reference_date'].transform(lambda x: ', '.join(sorted(x.unique())))
    df = df[except_ref + ['ref_dates']].drop_duplicates()
    return df


def clean_name(s):
    s = s.copy()
    s.loc[s.str.startswith('PG *TON ')] = s.str[8:]
    s.loc[s.str.startswith('TON ')] = s.str[4:]
    s.loc[s.str.startswith('PG *')] = s.str[4:]
    return s 

#%%

df = read_data('get_sample_ba')
df = df[df['company'] == 'TON']
df['document_type'] = df['document'].apply(lambda x: 'CPF' if len(str(x)) == 11 else 'CNPJ')
df = df[df['document_type'] == 'CNPJ']
print('Total de CNPJs:', df['document'].nunique())

df_ton = df.copy()

#%%

choice_doc = '48576309000150' # MARIA ISABELLE VAZ BAG	
choice_doc = '27323675000128' # JOAO LAURO (COSTA)
choice_doc = '55662815000128'

df = read_data('get_sample_ba')
df = df[df['document'] == choice_doc]
df = df.sort_values(by='reference_date', ascending=False)


df = colapse_ref_date(df)
df

#%%

df = read_data('get_sample_auth').rename(columns={'reference_month': 'reference_date'})
df = df[df['document'] == choice_doc]
df = df.sort_values(by='reference_date', ascending=False)
df = df[df['parc'].isna()]
df.columns = [x.lower() for x in df.columns]
df['name'] = df['name'].str.upper()
df['de43_v2'] = df['de43'].apply(lambda x: ' '.join(x.split(' ')[:-1]))
df['de43_v2'] = df['de43_v2'].str.strip()
df['de43_v2'] = df['de43_v2'].str.replace(r'\s+', ' ', regex=True)

df['de43_v3'] = df['de43'].str[:22]

df['name'] = clean_name(df['name'])
df = df[['document', 
         'name', 
         #'smid', 
         #'de42', 'de43_v2', 
         #'de43_v3',
         'reference_date', 'postalcode']].drop_duplicates()


cols_df = df.columns.tolist()


df = colapse_ref_date(df)
df



#%%

df = read_data('get_sample_mastercard').rename(columns={'reference_month': 'reference_date',
                                                        #'merchant_tax_id': 'document'
                                                        })

df = df.reset_index(drop=True)

df = df[df['document'] == choice_doc]

df = df.sort_values(by='reference_date', ascending=False)

available_columns = ['document', 'reference_date', 'merchant_market_hierarchy_id',
       'merchant_tax_id', 'merchant_name', 'merchant_name_cleansed', 'address',
       'address_cleansed', 'city_name', 'city_name_cleansed',
       'state_province_code', 'state_province_code_cleansed', 'postal_code',
       'postal_code_cleansed', 'country_code', 'country_code_cleansed',
       'latitude', 'longitude', 'geocoding_quality_indicator', 'phone_number',
       'phone_number_cleansed', 'merchant_category_code',
       'legal_corporate_name', 'legal_corporate_name_cleansed',
       'aggregate_merchant_id', 'aggregate_merchant_name',
       'key_aggregate_merchant_id', 'parent_aggregate_merchant_id',
       'parent_aggregate_merchant_name', 'industry', 'super_industry',
       'msa_code', 'naics_code', 'dma_code', 'first_seen_week',
       'last_seen_week', 'primary_channel_of_distribution', 'new_bus_flag',
       'in_bus_flag_7d', 'in_bus_flag_30d', 'in_bus_flag_60d',
       'in_bus_flag_90d', 'in_bus_flag_180d', 'in_bus_flag_360d',
       'cashback_flag', 'pay_at_pump_available', 'nfc_flag', 'emv_flag',
       'ecom_nsr', 'brick_and_mortar', 'local_favorite_number_accounts',
       'local_favorite_transaction_amount', 'local_favorite_transaction_count',
       'hidden_gem', 'reference_month_1', 'merchant_market_hierarchy_id_1',
       'de42_merchant_id', 'merchant_descriptor', 'original_name',
       'city_limpo', 'state_limpo', 'subs_asterisk', 'qtd_cidades_distintas',
       'original_name_split', 'geo_case', 'cod_muni', 'nome_limpo',
       'numero_inicio']

choice_cols = ['document', 'reference_date', 'merchant_market_hierarchy_id',
               'merchant_name', 'merchant_name_cleansed', 
               #'postal_code',
               'postal_code_cleansed', 
               #'legal_corporate_name', 'legal_corporate_name_cleansed', 

               'de42_merchant_id', 
               #'merchant_descriptor', 
               'original_name',
       #'city_limpo', 
       #'state_limpo', 
       'subs_asterisk', 'qtd_cidades_distintas',
       #'original_name_split', 
       'cod_muni', 
       #'nome_limpo',
]

df['original_name'] = clean_name(df['original_name'])

df = df[choice_cols].drop_duplicates()

df = colapse_ref_date(df)
df



#%% Check nunique de42 by company

df = read_data('get_sample_auth')
df = df[['document', 'de42']].drop_duplicates()
df =  df.merge(read_data('get_sample_ba'),
                on='document',
                how='left',
                indicator=True)

df = df.groupby(['company', 'document'])['de42'].nunique().reset_index()
df


#%%
import pandas as pd
# Variables para o match
# 1. None (distancia)
# 2. Postal code (distancia)
# 3. cod muni (distancia)
# 4. de42 (exato)



# Teste regra 1, mesmo postal code 7d

def to_postal_code_7d(x):
    x = x.copy()
    x = x.str.replace('-', '')
    x = x.str[:7]
    return x


def common_treat_df(df):
    df = df.copy()
    df = df[df['name'].notnull()]
    df = df[df['reference_date']>'2024-10-31']

    if 'merchant_market_hierarchy_id' in df.columns:
        id_col = ['merchant_market_hierarchy_id']
    else:
        id_col = []

    wanted_cols = ['document', 'name', 'postal_code1', 'de42', 'reference_date', 'cod_muni', 'ref_lead'] + id_col
    wanted_cols = [x for x in wanted_cols if x in df.columns]


    df = df[wanted_cols].drop_duplicates()
    #df = df[df['document'].isin(df_ton['document'])]
    df['postal_code_7d'] = to_postal_code_7d(df['postal_code1'])
    df['postal_code_6d'] = df['postal_code_7d'].str[:6]
    df['postal_code_5d'] = df['postal_code_7d'].str[:5]
    df['postal_code_4d'] = df['postal_code_7d'].str[:4]
    df['cod_muni'] = df['cod_muni'].astype(str)


    del  df['postal_code1']
    df['name'] = clean_name(df['name'])
    df = df.sort_values(by='reference_date', ascending=False)
    return df


def clean_ref_lag_if_missing(df):
    # Garante que datas estão no formato datetime
    df['reference_date'] = pd.to_datetime(df['reference_date'])
    col_name = 'ref_lead'
    df[col_name] = (df['reference_date'] + pd.DateOffset(months=1)).dt.to_period('M').dt.to_timestamp('M')


    # Cria um set de pares (document, reference_date) existentes
    valid_pairs = set(zip(df['document'], df['reference_date']))

    # Define ref_lag como NaT se o par (document, ref_lag) não estiver presente
    df[col_name] = df.apply(
        lambda row: row[col_name] if (row['document'], row[col_name]) in valid_pairs else pd.NaT,
        axis=1
    )

    return df


def create_atividade_lag():   
       df = read_data('get_sample_ba')
       df = df[['document', 'reference_date']].drop_duplicates()
       df = clean_ref_lag_if_missing(df)
       return df


def treat_auth():
    df = read_data('get_sample_auth').rename(columns={'reference_month': 'reference_date',
                                                      'postalcode': 'postal_code1',
                                                      })
    df = df[df['parc'].isna()]
    df['name'] = df['name'].str.upper()

    df = df.merge(create_atividade_lag(), on=['document', 'reference_date'], 
                  how='left')
                  

    df = common_treat_df(df)
    return df

def treat_new_auth():
    df = read_data('get_ton_auth').rename(columns={'reference_month': 'reference_date',
                                                        'postalcode': 'postal_code1',
                                                        })
    df = df[df['parc'].isna()]
    df['name'] = df['name'].str.upper()
    df['inicio'] = df['name'].str[:4]

    df = common_treat_df(df)
    return df



def treat_mastercard():
    df = read_data('get_sample_mastercard').rename(columns={'reference_month': 'reference_date',
                                                            'postal_code_cleansed': 'postal_code1',
                                                            })
    df = df.reset_index(drop=True)
    df['name'] = df['original_name']
    df['de42'] = df['de42_merchant_id']
    df = df[df['de42'].notnull()]
    df = df[df['de42'].str.endswith('000000')]
    df['ref_lag'] = df['reference_date'] 
    df['ref_lead'] = df['reference_date'] 
    df = common_treat_df(df)
    return df


def treat_new_mastercard():
    df = read_data('get_ton_mastercard').rename(columns={'reference_month': 'reference_date',
                                                         'postal_code_cleansed': 'postal_code1',
                                                         })
    df = df.reset_index(drop=True)
    df['name'] = df['nome_limpo']
    df['inicio'] = df['name'].str[:4]
    df['de42'] = df['de42_merchant_id']
    df = df[df['de42'].notnull()]
    
    df = common_treat_df(df)
    return df

#df = treat_new_mastercard()


#%%
import os
from fuzzywuzzy.fuzz import ratio, partial_ratio

def fuzzy_merge(df1, df2, exact_keys, fuzzy_key, suffixes=('_x', '_y'),
                how='inner', indicator=True, score_cutoff=80,
                ):
    df1 = df1.copy()
    df2 = df2.copy()

    if how!='inner':
        raise ValueError("Currently, only 'inner' join is supported for fuzzy matching.")
    

    intersection_cols = list(set(df1.columns).intersection(set( df2.columns)))   
    intersection_cols = [x for x in intersection_cols if x not in exact_keys + [fuzzy_key]]
    #print("Intersection columns:", intersection_cols)
    rename_cols1 = {col: f"{col}{suffixes[0]}" for col in intersection_cols}
    rename_cols2 = {col: f"{col}{suffixes[1]}" for col in  intersection_cols}


    
    # CROSS JOIN on exact keys
    df1['_tmp_key'] = 1
    df2['_tmp_key'] = 1
    #df1_filtered = df1[exact_keys + [fuzzy_key, '_tmp_key']+ intersection_cols].copy()
    #df2_filtered = df2[exact_keys + [fuzzy_key, '_tmp_key']+ intersection_cols].copy()
    df1_filtered = df1
    df2_filtered = df2


    df1_renamed = df1_filtered.rename(columns=rename_cols1)
    df2_renamed = df2_filtered.rename(columns=rename_cols2)

    print('Left DataFrame length:', len(df1_renamed))
    print('Right DataFrame length:', len(df2_renamed))

    df_cross = pd.merge(df1_renamed, df2_renamed, on=exact_keys + ['_tmp_key'], suffixes=suffixes,
                        how=how, indicator=True, 
                        )
    df_cross = df_cross[df_cross['_merge']== 'both']


    print('Join DataFrame length:', len(df_cross))

    print('Merge Ended. Checking fuzzy match conditions...')

    if df_cross.empty:
        print("No matches found after cross join.")
        return pd.DataFrame([])



    df_cross.drop('_tmp_key', axis=1, inplace=True)

    # Compute fuzzy score
    left_col = f"{fuzzy_key}{suffixes[0]}"
    right_col = f"{fuzzy_key}{suffixes[1]}"


    if score_cutoff<100:
       df_cross['__fuzzy_score__'] = df_cross.apply(
              lambda row: partial_ratio(row[left_col], row[right_col]), axis=1
       )
       
       # Filter above threshold
       df_cross = df_cross[df_cross['__fuzzy_score__'] >= score_cutoff]

       df_cross['gratest_score'] = df_cross.groupby(exact_keys)['__fuzzy_score__'].transform('max')

       df_cross = df_cross[df_cross['__fuzzy_score__'] == df_cross['gratest_score']]
    else:
        df_cross =  df_cross[df_cross[left_col] == df_cross[right_col]]
        df_cross['__fuzzy_score__']  = 100

    

    result = df_cross.copy()
    print('Fuzzy match completed. Resulting DataFrame length:', len(result))
    
    result = result.drop(columns=[
        #'__fuzzy_score__', 
        fuzzy_key, '_tmp_key'], errors='ignore').reset_index(drop=True)
    #result = result.drop(columns=[col for col in result.columns if '_tmp_key' in col], errors='ignore')
    return result


def normal_merge(df1, df2, exact_keys, fuzzy_key,
                 suffixes=('_x', '_y')):
    
    df1 = df1.copy()
    df2 = df2.copy()

    intersection_cols = list(set(df1.columns).intersection(set( df2.columns)))   
    intersection_cols = [x for x in intersection_cols if x not in exact_keys + [fuzzy_key]]
    #print("Intersection columns:", intersection_cols)
    rename_cols1 = {col: f"{col}{suffixes[0]}" for col in intersection_cols}
    rename_cols2 = {col: f"{col}{suffixes[1]}" for col in  intersection_cols}


    


    #df1_filtered = df1[exact_keys + [fuzzy_key, '_tmp_key']+ intersection_cols].copy()
    #df2_filtered = df2[exact_keys + [fuzzy_key, '_tmp_key']+ intersection_cols].copy()
    df1_filtered = df1
    df2_filtered = df2


    df1_renamed = df1_filtered.rename(columns=rename_cols1)
    df2_renamed = df2_filtered.rename(columns=rename_cols2)

    # Compute fuzzy score
    left_col = f"{fuzzy_key}{suffixes[0]}"
    right_col = f"{fuzzy_key}{suffixes[1]}"


    df_cross = pd.merge(df1_renamed, df2_renamed, 
                        on=exact_keys +  [fuzzy_key],
                        suffixes=suffixes,
                        how='inner', indicator=True, 
                        )
    
        # Compute fuzzy score
    left_col = f"{fuzzy_key}{suffixes[0]}"
    right_col = f"{fuzzy_key}{suffixes[1]}"
    df_cross[left_col] = df_cross[fuzzy_key]
    df_cross[right_col] = df_cross[fuzzy_key]


    if df_cross.empty:
        print("No matches found after cross join.")
        return pd.DataFrame([])



    df_cross['__fuzzy_score__']  = 100

    

    result = df_cross.copy()
    print('Normal match completed. Resulting DataFrame length:', len(result))
    
    result = result.drop(columns=[
        #'__fuzzy_score__', 
        fuzzy_key, '_tmp_key'], errors='ignore').reset_index(drop=True)
    #result = result.drop(columns=[col for col in result.columns if '_tmp_key' in col], errors='ignore')
    return result



def exclude_id(df: pd.DataFrame, df_level_id: pd.DataFrame) -> pd.DataFrame:
    df_level_id_cols = df_level_id.columns
    # 1. Colunas pelas quais você quer excluir
    keys = list(df_level_id_cols)

    # 2. Conjunto (MultiIndex) com os IDs que devem ser removidos
    ids_a_excluir = pd.MultiIndex.from_frame(df_level_id[keys])

    # 3. Cria um MultiIndex para df nas mesmas colunas e faz a negação da pertença
    mask = ~pd.MultiIndex.from_frame(df[keys]).isin(ids_a_excluir)

    # 4. Filtra só as linhas que não aparecem em df_level_id
    return df.loc[mask]


def create_new_dfs(df_a, df_m, level_merge,
                   fuzzy=True,
                   ):
    

    df = normal_merge(df_a, df_m, exact_keys=level_merge, fuzzy_key='name',
                      suffixes=('_auth', '_mastercard'))
    
    df['tipo_merge'] = ', '.join(level_merge) + ', name'

    if not df.empty:
            df_level_auth = df.rename(columns={'reference_date_auth': 'reference_date'})[level_id_auth].drop_duplicates()
            df_level_mastercard = df.rename(columns={'reference_date_mastercard': 'reference_date'})[level_id_mastercard].drop_duplicates()
            df_a = exclude_id(df_a, df_level_auth)
            df_m = exclude_id(df_m, df_level_mastercard)

    df_normal_merge = df.copy()


    if fuzzy:
        print('Fuzzy merge...')


        df = fuzzy_merge(df_a, df_m, exact_keys=level_merge, fuzzy_key='name', how='inner',
                        indicator=False,
                        suffixes=('_auth', '_mastercard'), score_cutoff=80,
                        )
        df['tipo_merge'] = ', '.join(level_merge) 

        if df.empty:
            return df, df_a, df_m


        df_level_auth = df.rename(columns={'reference_date_auth': 'reference_date'})[level_id_auth].drop_duplicates()
        df_level_mastercard = df.rename(columns={'reference_date_mastercard': 'reference_date'})[level_id_mastercard].drop_duplicates()
        df_a = exclude_id(df_a, df_level_auth)
        df_m = exclude_id(df_m, df_level_mastercard)

    df = pd.concat([df, df_normal_merge], ignore_index=True)


    return df, df_a, df_m


#%%



df_a = treat_new_auth()
df_m = treat_new_mastercard()

df_a = df_a.rename(columns={'document': 'document_auth'})
df_m = df_m.rename(columns={'document': 'document_mastercard'})



level_id_auth = ['document_auth', 'reference_date']
level_id_mastercard = ['merchant_market_hierarchy_id', 'reference_date']


#%%

def create_final_merge(df_a, df_m):
        
    dataframes = []

    for level_merge in [
        ['reference_date', 'postal_code_7d', 'de42'],
        ['reference_date', 'postal_code_7d'],
        ['reference_date', 'postal_code_6d'],
        ['reference_date', 'postal_code_5d'],
        ['reference_date', 'postal_code_4d'],
        ['reference_date', 'cod_muni', 'de42'],
        #['reference_date', 'cod_muni'],
        #['ref_lead', 'postal_code_7d'],
                        ]:
        print('Merge level:', level_merge)

        if level_merge == ['reference_date', 'postal_code_4d'] or level_merge == ['reference_date', 'postal_code_5d']:
            df, df_a, df_m = create_new_dfs(df_a, df_m, level_merge, fuzzy=False)
        else:
            df, df_a, df_m = create_new_dfs(df_a, df_m, level_merge)
        dataframes.append(df)


    df_left_only = df_a

    df_left_only['_merge'] = 'left_only'
    df_left_only['tipo_merge'] = 'none'

    df =  pd.concat(dataframes + [df_left_only], ignore_index=True)

    return df



def read_populacao():
    df = pd.read_parquet(r"G:\.shortcut-targets-by-id\11oihSUrjIBMrKMTPPH43oBLwcxGYIND_\Stone Economic Research\0. Projetos\Concorrência - MasterCard\data\populacao_municipio.parquet")
    df = df[['id_municipio', 'populacao']].rename(columns={'id_municipio': 'cod_muni'})
    return df



cod_munis = list(df_a['cod_muni'].unique())

completed_munis = [x.split('_')[-1].split('.')[0] for x in os.listdir('results/merge_ton') if x.endswith('.parquet')]

print(f'Qtd. de municípios completados: {len(completed_munis)}')

pop = df_a.groupby(['cod_muni'])['name'].count().rename('populacao').reset_index()


#pop =  read_populacao()

pop = pop.merge(pd.DataFrame(cod_munis, columns=['cod_muni']), on='cod_muni', how='inner')

pop = pop.merge(pd.DataFrame(completed_munis, columns=['cod_muni']).assign(completed=True),
                on='cod_muni', how='left')

pop = pop.sort_values(by='populacao', ascending=False).reset_index(drop=True)
pop['completed'] = pop['completed'].fillna(False)

pop = pop[~pop['completed']]
pop = pop[pop['cod_muni']!='<NA>']
pop.head(30)



#%%
for cod_muni in  pop['cod_muni'].unique():
    final_path = f'results/merge_ton/merge_ton_{cod_muni}.parquet'
    if os.path.exists(final_path):
        print(f'Skipping cod_muni: {cod_muni}, already processed.')
        continue
    else:
        df_a_filtered = df_a[df_a['cod_muni'] == cod_muni]
        df_m_filtered = df_m[df_m['cod_muni'] == cod_muni]
        print(f'Processing cod_muni: {cod_muni} with {df_a_filtered.shape[0]} auth records and {df_m_filtered.shape[0]} mastercard records')
        df = create_final_merge(df_a_filtered, df_m_filtered)
        print(f'Created {df.shape[0]} merged records for cod_muni: {cod_muni}')
        df.to_parquet(f'results/merge_ton/merge_ton_{cod_muni}.parquet', index=False)
        print(f'Saved results\n--------------------------------------\n')


#%%


cod_munis = list(df_a['cod_muni'].unique())

completed_munis = [x.split('_')[-1].split('.')[0] for x in os.listdir('results/merge_ton') if x.endswith('.parquet')]

missing_munis = [x for x in cod_munis if x not in completed_munis]

df_a_filtered = df_a[df_a['cod_muni'].isin(missing_munis)]
df_m_filtered = df_m[df_m['cod_muni'].isin(missing_munis)]
print(f'Processing {len(missing_munis)} missing municipalities with {df_a_filtered.shape[0]} auth records and {df_m_filtered.shape[0]} mastercard records')
df = create_final_merge(df_a_filtered, df_m_filtered)
df.to_parquet(f'results/merge_ton/merge_ton_missing.parquet', index=False)
print(f'Saved results for missing municipalities\n--------------------------------------\n')

#%%

from tqdm import tqdm


def read_files(file_name):
    path = 'results/merge_ton'
    final_path = os.path.join(path, file_name)
    df = pd.read_parquet(final_path)
    df['file_name'] = file_name
    return df

dfs = [] 
for file_name in tqdm(os.listdir('results/merge_ton')):
    if file_name.endswith('.parquet'):
        df = read_files(file_name)
        dfs.append(df)

#%%

df = pd.concat(dfs)

df = df[['document_auth', 'merchant_market_hierarchy_id']].drop_duplicates()

# eliminar casos de mmhid que estão associados a varios documentos
dfg = df.copy()
dfg = dfg.groupby(['merchant_market_hierarchy_id'])['document_auth'].nunique().reset_index()
dfg = dfg[dfg['document_auth']>1]

df = pd.concat([
    df[(~df['merchant_market_hierarchy_id'].fillna(9999999999999).isin(dfg['merchant_market_hierarchy_id']))],
       df[df['merchant_market_hierarchy_id'].isin(dfg['merchant_market_hierarchy_id'])][['document_auth']].drop_duplicates()
], ignore_index=True)



# eliminar casos de documentos que estão associados a mais de 5 mmhid
dfg = df.copy()
dfg = dfg.groupby(['document_auth'])['merchant_market_hierarchy_id'].nunique().reset_index()
dfg = dfg[dfg['merchant_market_hierarchy_id']>5]

df = pd.concat([
    df[~(df['document_auth'].isin(dfg['document_auth']))],
    dfg['document_auth'].drop_duplicates().to_frame()], ignore_index=True)



df.loc[df['merchant_market_hierarchy_id'].isnull(), 'merchant_market_hierarchy_id'] = pd.NA


#%%

import pandas_gbq as pd_gbq

df['inserted_at'] = pd.to_datetime('now').normalize()
df['reference_date'] = pd.to_datetime('2025-04-30').normalize()

df['inserted_at'] = df['inserted_at'].dt.date
df['reference_date'] = df['reference_date'].dt.date


pd_gbq.to_gbq(df, 'dataplatform-prd.master_contact.aux_tam_ton_to_mmhid', if_exists='append')

#%%
df = df[['document_auth', 'name_auth', 'merchant_market_hierarchy_id']].drop_duplicates()
#%%

dfg = df.copy()
dfg['name_not_null'] = dfg['merchant_market_hierarchy_id'].notnull()
dfg = dfg.groupby(['document_auth'])['name_not_null'].max()
dfg.mean()

#%%

dfg = df.copy()
dfg = dfg.groupby(['document_auth'])['merchant_market_hierarchy_id'].nunique().reset_index()
dfg[dfg['merchant_market_hierarchy_id']>3]


#%%
dfg = df.copy()
dfg = dfg.groupby(['merchant_market_hierarchy_id'])['document_auth'].nunique().reset_index()
dfg = dfg[dfg['document_auth']>1]
#%%


df['file_name'].nunique()

df = df[df['__fuzzy_score__'].notnull()]

df['inicio_auth'] = df['name_auth'].str[:4]
df['inicio_mastercard'] = df['name_mastercard'].str[:4]

df['inicio_diff'] = df['inicio_auth'] != df['inicio_mastercard']


df.groupby('tipo_merge')['inicio_diff'].mean()


df.groupby('tipo_merge')['__fuzzy_score__'].mean()
#%%


#%%

#%%

import pandas as pd
df = pd.read_parquet('dev_identificacao_ton.parquet')

#%%

s = df[df['merchant_market_hierarchy_id'].notnull()][['document_auth', 'reference_date', 'merchant_market_hierarchy_id',]].drop_duplicates()

#%%

df['tipo_merge'].value_counts()

#%%
df_m = treat_mastercard()
df_m = df_m.rename(columns={'document': 'document_mastercard'})
df_left_only = df_left_only.drop(columns=['_merge', 'tipo_merge'], errors='ignore')
erro = df_left_only.merge(df_m, left_on=['reference_date', 'document_auth'],
                             right_on=['reference_date', 'document_mastercard'],
                             how='inner')

erro

#%%

doc_choice = '06089647000118'
ref_date_choice = '2025-03-31'

def filter_int(df):
    df = df.copy()
    #df = df[df['reference_date'] == ref_date_choice]
    df = df[df['document'] == doc_choice]
    return df

df_a = treat_auth()
df_a = filter_int(df_a)
df_m = treat_mastercard()
df_m =  filter_int(df_m)
display(df_a)
display(df_m)