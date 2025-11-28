#%%
from utils import download_data, read_data
import pandas_gbq

import numpy as np
import pandas as pd


from main_model_acquirer import load_model, creat_data_apply_model, create_features
import pandas as pd



#%%

def get_predictions_with_index(model, X, p=0.1):
    """
    Returns a long-form DataFrame with:
        - the original index from X,
        - class predictions (top + prob > p),
        - their probabilities,
        - and prediction rank.

    Parameters:
        model: Trained classifier with `predict_proba` and `classes_`.
        X: Input features (Pandas DataFrame or array-like).
        p: Probability threshold (float between 0 and 1).

    Returns:
        pd.DataFrame with columns:
            - 'index': index from X
            - 'prediction': predicted class
            - 'prob': predicted probability
            - 'rank': order of prediction (1 = top prediction)
    """
    # Ensure X is a DataFrame so we can retrieve its index
    if not isinstance(X, pd.DataFrame):
        X = pd.DataFrame(X)

    probas = model.predict_proba(X)
    classes = np.array(model.classes_)
    indices = X.index

    rows = []

    for obs_idx, prob in enumerate(probas):
        sorted_indices = np.argsort(prob)[::-1]
        rank = 1

        for i in sorted_indices:
            if prob[i] > p or i == sorted_indices[0]:
                rows.append({
                    'de42': indices[obs_idx],
                    'prediction': classes[i],
                    'prob': round(prob[i], 4),
                    'rank': rank
                })
                rank += 1
            else:
                break

    return pd.DataFrame(rows)



def apply_model_v2(df, to_gbq=True):



    print('Loading Model')
    model = load_model(exclude_tags=True, handle_rede_format=True)

    feature = model.get_feature_importance(prettified=True)
    feature.columns = ['feature', 'importance']
    int_cols = feature['feature'].tolist()
    

    df = df[[
        'de42', 
    ]].drop_duplicates().reset_index(drop=True)


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

        df42 = df['de42']

        X = df.set_index('de42')[int_cols].copy()
        res = get_predictions_with_index(model, X, p=0.1)

        #res = df42.to_frame().reset_index(drop=True).join(res)

        return res
        
    print('Applying model')
    return result_from_model(model, df, int_cols)


def main():
    download_data('de42_model', update=True)
    df_orig = read_data('de42_model')
    if df_orig.shape[0] >= 100000:
        print('Warning: More than 100,000 rows. There might be an eror in the query.')
        return None
    df = apply_model_v2(df_orig.copy())
    if df.shape[0] > 0:
        print('Saving results to BigQuery')
        pandas_gbq.to_gbq(df, 'dataplatform-prd.master_contact.de42_unknown_acquirer_model', if_exists='append')
    else:
        print('No results to save.')

if __name__ == '__main__':
    main()
