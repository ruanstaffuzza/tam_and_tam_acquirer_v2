#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import pandas_gbq as pd_gbq
import jinja2
import os

import time

# function to get current date and time
def get_current_time():
    from datetime import datetime
    return "[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "]"

def read_gbq_(query):
  project_id = 'sfwthr2a4shdyuogrt3jjtygj160rs' # ri-nonprod
  print(f'{get_current_time()} Getting dataset from BQ...')
  return pd_gbq.read_gbq(query, progress_bar_type='tqdm',
                         use_bqstorage_api=True,
      project_id=project_id)

def read_gbq_from_template(template_query, dict_query):
  query = template_query
  if dict_query:
      from jinja2 import Template
      # Reads a query from a template and returns the query with the variables replaced
      # template_query: query as string, may use jinja2 templating
      # dict_query: dictionary of query parameters, to render from the template with jinja2
      query = Template(template_query).render(dict_query)
  return read_gbq_(query)


def read_text(file_name, encoding='utf-8'):
  with open(file_name, 'r', encoding=encoding) as f:
      return f.read()
    


def download_data(query_name,
                  queries_path,
                  data_path,
                  file_format='parquet',
                  update=False,
                  dict_query = {}, **kwargs):
  import os

  if not query_name.endswith('.sql'):
      query_name += '.sql'

  if file_format.startswith('.'):
      file_format = file_format[1:]

  from os.path import exists
  #file_path = fr"data\{query_name[:-4]}.{file_format}"
  #query_path = fr"queries\{query_name}"
  file_path = os.path.join(data_path, f'{query_name[:-4]}.{file_format}')
  query_path = os.path.join(queries_path, query_name)

  template_query = read_text(query_path)

  if update or not exists(file_path):
      df = read_gbq_from_template(template_query, dict_query)

      for c in df.dtypes[df.dtypes=='dbdate'].index:
          df[c] = df[c].astype('datetime64[ns]')

      z = 'index=False' if file_format=='csv' else ''
      file_format = 'excel' if file_format=='xlsx' else file_format
      print(file_format)
      if len(df)>0:
          eval(f'df.to_{file_format}(file_path,{z})')
          print(f'{get_current_time()} File successfully recorded on ', file_path)
      return df

#nome_projeto = '64. TAM/spinoffs/subs_exploratorio'
#queries_path = os.path.join(GDRIVE_PATH, nome_projeto, 'queries')
#data_path = os.path.join(GDRIVE_PATH, nome_projeto, 'data')
queries_path = 'queries'
data_path = 'data'


# In[2]:


query = """
select distinct
cod_muni,
nome_master,
inicio,
from  `dataplatform-prd.master_contact.aux_tam_final_nomes` 
where 1=1
{{add_filter}}
and inicio<>'SYMPLA'
"""



dates = [ 
 '2024-06-30',
 '2024-05-31',
 '2024-04-30',
 '2024-03-31',
 '2024-02-29',
 '2024-01-31',
 '2023-12-31',
 '2023-11-30',
 '2023-10-31',
 '2023-09-30']

date = '2024-08-31'


# In[3]:


from agrupamento_nomes import group_names_df_v2

def group_and_clean(df, LEVEL_GROUP):
    df = df.copy()
    
    #print('Tamanho do DataFrame:', df.shape)

    #df['qtd_por_inicio'] = df.groupby(['nome_muni', 'uf'])['nome_master'].transform('count')
    #df = df[df['qtd_por_inicio']>1]

    df = df.sort_values(by=LEVEL_GROUP + ['nome_master']).reset_index(drop=True)

    df = group_names_df_v2(df, group_cols=[
                                        'inicio',
                                        ] + LEVEL_GROUP, 
                           delete_aux_cols=False,
                        nome_col='nome_master')
    
    if LEVEL_GROUP:
        df['agrupamento_nome_1'] = df['res_aux'].apply(lambda x: ', '.join(x)) + df[LEVEL_GROUP].astype(str).apply(lambda x: '|'.join(x), axis=1)
    else:
        df['agrupamento_nome_1'] = df['res_aux'].apply(lambda x: ', '.join(x))
    
    df = df.drop(columns=['resultado_prefixes', 'res_aux', 'retirado_de_resultado'])
    
    return df


import pandas as pd
import os
from tqdm.notebook import tqdm

def process_in_chunks(df, LEVEL_GROUP, output_dir="temp_dir"):
    from tqdm import tqdm

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Conta o número de grupos
    num_groups = df.groupby(LEVEL_GROUP).ngroups

    # Cria a barra de progresso
    progress_bar = tqdm(total=num_groups, desc="Processando grupos", unit="grupo")

    for group_values, group_df in df.groupby(LEVEL_GROUP):
        # Gera o nome do arquivo baseado nos valores do grupo
        filename = f"{output_dir}/processed_group_{group_values}.parquet"
        
        # Verifica se o arquivo já existe
        if os.path.exists(filename):
            progress_bar.update(1)  # Atualiza a barra de progresso
            continue  # Pula o processamento deste grupo

        # Processa e salva o resultado se o arquivo ainda não existir
        processed_df = group_and_clean(group_df, LEVEL_GROUP)
        processed_df.to_parquet(filename, index=False)
        
        # Atualiza a barra de progresso
        progress_bar.update(1)
    
    progress_bar.close()
    return None



# In[4]:


def main_agrupamento_nomes(date):
    query = """
    select distinct
    cod_muni,
    nome_master,
    inicio,
    from  `dataplatform-prd.master_contact.aux_tam_final_nomes` 
    where 1=1
    {{add_filter}}
    and inicio<>'SYMPLA'
    """

    print(date)
    anomes = date[:7].replace('-', '')
    print('Tratando dados de ', date)
    df = read_gbq_from_template(query, {'add_filter': f'AND reference_month = "{date}"'})
    df = process_in_chunks(df, ['cod_muni'], output_dir=f"data/temp_dir_{anomes}")


# In[9]:


def process_in_chunks_v2(df, LEVEL_GROUP, output_dir="temp_dir"):
    from tqdm import tqdm

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Conta o número de grupos
    num_groups = df.groupby(LEVEL_GROUP).ngroups

    # Cria a barra de progresso
    progress_bar = tqdm(total=num_groups, desc="Processando grupos", unit="grupo")

    for group_values, group_df in df.groupby(LEVEL_GROUP):
        # Gera o nome do arquivo baseado nos valores do grupo
        filename = f"{output_dir}/processed_group_{group_values}.parquet"
        
        # Verifica se o arquivo já existe
        if os.path.exists(filename):
            progress_bar.update(1)  # Atualiza a barra de progresso
            continue  # Pula o processamento deste grupo

        # Processa e salva o resultado se o arquivo ainda não existir
        processed_df = group_df
        processed_df.to_parquet(filename, index=False)
        
        # Atualiza a barra de progresso
        progress_bar.update(1)
    
    progress_bar.close()
    return None



# In[10]:


#anomes = '202310'
#df = pd.read_parquet(f'data/agrupamento_nomes_{anomes}.parquet')
#df = process_in_chunks_v2(df, ['cod_muni'], output_dir=f"data/temp_dir_{anomes}")


# In[ ]:




