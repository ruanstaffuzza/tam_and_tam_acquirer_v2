#%%
from utils import download_data

download_data('10_acquirers_model',
                #update=True
                )



from utils import (save_query, download_data, 
                   read_text, read_gbq_from_template, get_current_time,
                   read_data)
import pandas as pd
import numpy as np


from sklearn.model_selection import train_test_split
from sklearn.metrics import ConfusionMatrixDisplay
from matplotlib import pyplot as plt
from catboost import CatBoostClassifier


def create_dummies(df: pd.DataFrame, 
                   series: pd.Series,
                   prefix: str
                   ):
    dummies = pd.get_dummies(series, prefix=prefix)
    df = pd.concat([df, dummies], axis=1)
    return df


def correct_rede_formatting(df):
    """- Aparentemente os dados da Rede estão com problemas de formatação.
       - Muitas vezes estamos recebendo o DE42 com menos de 9 digitos, que sabemos que não ocorre na base da master"""
    df = df.copy()
    
    # Print initial message
    print('Fixing Rede problems')
    
    # Define the problem filter
    filter_problem = (df['de42'].str.strip(' ').str.len() < 9) & (df['adq_s'] == 'Rede')
    
    # Print the number of problems
    print('Number of problems:', filter_problem.sum())
    
    # Fix the problems
    df.loc[filter_problem, 'de42'] = (
        df['de42']
        .str.strip(' ')  # Remove leading and trailing spaces
        .str.lstrip('0')  # Remove leading zeros
        .str.rjust(9, '0')  # Right justify and fill with zeros to length 9
        .str.ljust(15, ' ')  # Left justify and fill with spaces to length 15
    )
    
    return df


def create_features(df):
    # left pad 15 , with ':')
    df['de42'] = df['de42'].str.strip(' ').str.ljust(15, ' ')


    df = create_dummies(df, df['de42'].str.strip(' ').str.len(), f'len_digits_')

    df = create_dummies(df, df['de42'].str.strip(' ').str.lstrip('0').str.len(), f'len_int_')

    # create category for each position
    for i in range(15):
        print('Creating feature for position', i)
        df = create_dummies(df, df['de42'].str[i], f'pos_{i}_')

    for i in range(6):
        print('Creating feature for position (no leading zeros)', i)
        df = create_dummies(df, df['de42'].str.lstrip('0').str[i], f'int_{i}_')

    for i in range(2,4):
        print('Creating feature for range 0-', i)
        df = create_dummies(df, df['de42'].str[0:i], f'pos0_{i}_')


    #df['lower_name'] = df['merchant_name'].apply(lambda x: any([y.islower() for y in x]))
    return df

def load_model(exclude_tags: bool, handle_rede_format: bool):
    if exclude_tags and handle_rede_format:
        model_name = 'model_test_4_tag_8digits'
    elif exclude_tags and not handle_rede_format:
        model_name = 'model_test_2_tag'
    elif not exclude_tags and handle_rede_format:
        model_name = 'model_test_3_8digits'
    elif not exclude_tags and not handle_rede_format:
        model_name = 'model_test_1_base'
    else:
        raise Exception('Model not found')
    
    print(f'Loading model that exclude_tags={exclude_tags} and handle_rede_format={handle_rede_format}')
    
    model = CatBoostClassifier()
    model = model.load_model(f'results/models/{model_name}.cbm')
    return model

def open_de42_new():
    import numpy as np
    df = read_data('10_acquirers_model')
    df = df.drop(columns=['lower_name'])
    return df


def open_mastercard_de42(version, sample_n_tax_id=None, filter_ref_months=None):
    print('Opening mastercard de42')
    df = open_de42_new()
    
    if filter_ref_months:
        df = df[df['reference_month'].isin(filter_ref_months)]
        print('Filtering reference months:', filter_ref_months)

    #df = df[df['merchant_market_hierarchy_id']!=479879720] # remove pagarme
    
    return df

def creat_data_apply_model(df, exclude_tags, handle_rede_format, perfect_match):

    if exclude_tags:
        raise Exception('Not implemented')
    if handle_rede_format:
        raise Exception('Not implemented')
    if perfect_match:
        raise Exception('Not implemented')

    tag = pd.DataFrame(columns=['de42', 'merchant_market_hierarchy_id', 'acquirer'])

    # this condition is not currently being used
   
    df_match = pd.DataFrame(columns=['de42', 'merchant_market_hierarchy_id', 'acquirer'])

    return [df, tag, df_match]




def apply_model(df, to_gbq=True):



    print('Loading Model')
    model = load_model(exclude_tags=True, handle_rede_format=True)

    feature = model.get_feature_importance(prettified=True)
    feature.columns = ['feature', 'importance']
    int_cols = feature['feature'].tolist()
    

    info_mmhi_ref = df.copy()

    df = df[[
        'de42'
    ]].drop_duplicates().reset_index(drop=True)

    
    print('Eliminatin double id. obs changed from', info_mmhi_ref.shape[0], 'to', df.shape[0]) 

    [df, tag, df_match] = creat_data_apply_model(df, exclude_tags=False, handle_rede_format=False, perfect_match=False)

    lenght_de42 = df['de42'].str.strip(' ').str.len()

    print('Number of rows to the model:', df.shape[0])
    
    create_features_choice = create_features


    print('Creating features')
    df = create_features_choice(df.copy())

    # add features (bool) not created by create_features
    s = [x for x in int_cols if x not in df.columns]
    df[s] = False

    def result_from_model(model, df, int_cols):
        df = df.copy()

        X = df[int_cols].copy()
                
        predict_proba = model.predict_proba(X)

        import numpy as np
        predictions_idx = np.argmax(predict_proba, axis=1)

        pred = np.array(model.classes_)[predictions_idx]
        #pred = model.predict(X) # outra forma de fazer, mais lenta

        confidence = pd.DataFrame(predict_proba).max(axis=1)

        res = df[['de42', 
        #'lower_city', 'lower_name'
        ]].copy().reset_index(drop=True)
        res['acquirer'] = pd.DataFrame(pred).reset_index(drop=True)[0]
        res['confidence_index'] = confidence
        return res

    print('Applying model')
    l = []
    chunk_size = int(5e5)
    for i in range(0, df.shape[0], chunk_size):
        print(i)
        df_sample = df.iloc[i:i+chunk_size]
        l.append(result_from_model(model, df_sample, int_cols))

    # model results
    res = pd.concat(l)
    res['outside_model'] = False

     
    
    df_final = res.copy()
    
    df_final = info_mmhi_ref.merge(df_final, 
                              on=['de42'], how='left')
    

    df_final['high_confidence'] = (df_final['confidence_index']>0.8)

    df_final.to_parquet(r"results/data/de42_acquirer.parquet", index=False)
     
    #df_final = df_final[df_final['high_confidence']]

    df_final.loc[~df_final['high_confidence'], 'acquirer'] = 'Previsão Incerta'

    #df_final = df_final[['tam_id', 'reference_date', 'acquirer']]
    

    #df_final['confidence_index'] = df_final['confidence_index'].round(6)
    

    #df_final.to_parquet(r"G:\.shortcut-targets-by-id\11oihSUrjIBMrKMTPPH43oBLwcxGYIND_\Stone Economic Research\0. Projetos\Descriptors Master\data\de42_acquirer_new.parquet", index=False)
    if to_gbq:
        df_final['outside_model'] = True
        df_final['metadata__insert_time'] = pd.to_datetime('now')
        print('Saving to Google Big Query')
        df_final.to_gbq('dataplatform-prd.master_contact.tmp_mid_de42_acquirer_model_v2', project_id='dataplatform-prd',
                        if_exists='append')
    
    #df_final.to_parquet(r"data\de42_acquirer_old_sample.parquet", index=False)
    #df_final.to_parquet(r"data\de42_acquirer_new_old.parquet", index=False)
    return df_final

create_features_choice = create_features

if __name__ == '__main__':
    print('Loading data')
    #df = open_mastercard_de42(version='mid_de42', sample_n_tax_id=None, )
    #apply_model(df, to_gbq=True)


"""
CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.temp_ruan_tam_acquirers` 
PARTITION BY reference_date 
AS 


Select *,
False modelo_previsao
 from `dataplatform-prd.master_contact.temp_ruan_tam_acquirers` 

union distinct
SELECT 
tam_id,
DATE(reference_date) reference_date,
acquirer adquirente,
TRue modelo_previsao,
from `dataplatform-prd.master_contact.tmp_mid_de42_acquirer_model`
"""
None
