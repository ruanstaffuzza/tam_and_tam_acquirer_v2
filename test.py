#%%
import pandas as pd

from utils import read_data, download_data

def update_data():
    download_data('check_cielo_get', update=True)

from IPython.display import display, Markdown
#%%

df = read_data('check_cielo_get')

#%%
cols_interesse = [
    
    #'reference_date', 
    'tam_id', 
    'cod_muni',
    'merchant_market_hierarchy_id',
    'new_mmhid_merge',
    'mmhid_original',
    #'merchant_name', 
    'subs_asterisk', 
    #'document', 'source_document',
    #'merchant_tax_id', 'numero_inicio', 'cnpj', 'cpf', 'cpf_brasil',
    'inicio', 
    #'new_mmhid_merge', 
    'nome_master', 
    
    'id_merge_places', 'doc_unmerge', 'group_idx_nome', 'id_merge_places2',
    'group_idx_merge_places', 'group_id', 'group_idx_unmerge_places',
    'group_idx_unmerge_docs',
    'adquirente_padroes',
]


mmhid_sample = df['merchant_market_hierarchy_id'].drop_duplicates().sample(1).values[0]


dfs = df[df['merchant_market_hierarchy_id'] == mmhid_sample][cols_interesse].copy()
dfs['tam_id'] = dfs['tam_id'].str[:-13]

dfs.drop_duplicates()

#%%

df['cod_muni2'] = df['tam_id'].str[-10:-6]
df[df['cod_muni']==9999][['cod_muni','cod_muni2', 'tam_id']].drop_duplicates()