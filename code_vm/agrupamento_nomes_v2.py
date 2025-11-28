#%%
import time 
import pandas as pd
from IPython.display import display

def prefixo_comum(str1, str2):
    if not str1 or not str2 or str1[0] != str2[0]:
        return None

    # Find the shortest length to ensure the loop does not exceed the shorter string
    min_len = min(len(str1), len(str2))

    # Use zip to pair up characters and enumerate for index, stopping at the first mismatch
    i = next((i for i, (c1, c2) in enumerate(zip(str1, str2)) if c1 != c2), min_len)

    # Return the common prefix if its length is greater than 1, else None
    return str1[:i] if i > 1 else None


def find_common_prefix(strs):
    """Encontra o prefixo comum mais longo entre uma lista de strings."""
    if not strs:
        return ""
    shortest_str = min(strs, key=len)
    for i, char in enumerate(shortest_str):
        for other in strs:
            if other[i] != char:
                return shortest_str[:i]
    return shortest_str


# Função para verificar se um nome está contido em outro
def is_contained(name1, name2):
    return name1 in name2 or name2 in name1

def starts_with(name1, name2):
    return name1.startswith(name2) or name2.startswith(name1)

def separate_groups(names, verbose=False):
    selection = {}

    # order names
    
    #names = list(set(names))
    #names = sorted(names)


    len_names = len(names)
    if len_names > 1000:
        print('Quantidade de nomes:', len_names)
        start_time = time.time()



    # Percorre todos os nomes
    i=0
    for name in names:
        added = False
        # Tenta adicionar o nome a um grupo existente
        if verbose:
            print('------------------------------------------------\n0. Estudando o nome: ', name)
        for k, sel in selection.items():
            group = sel['names']

            if verbose:
                print(f' 2. Verificando se {name} pode ser adicionado ao grupo {group}')
            

            if all(is_contained(name, existing_name) for existing_name in group):
                if verbose:
                    print(f'  3. Adicionando {name} ao grupo {group}')
                group.append(name)
                added = True
                selection[k] = {'names': list(set(group)), 'common_prefixes': sel['common_prefixes']}

            else:
                # Verifica se o nome pode ser adicionado ao grupo atual
                prefix = list(set([prefixo_comum(g, name) for g in group]))
                prefix = [p for p in prefix if p is not None and p != name]
                if prefix:
                    if verbose:
                        print(f'   4. Adicionando prefixo {prefix} ao grupo {group}')
                    prefixes = list(set(sel['common_prefixes'] + prefix))                
                    selection[k] = {'names': list(set(group)), 'common_prefixes': prefixes}
        # Se não foi adicionado a nenhum grupo, cria um novo grupo
        if not added:
            if verbose:
                print(f'1. Criando novo grupo com {name}')
            selection[i] = {'names': [name], 'common_prefixes': []}
            i = i+1
    
    if len_names > 1000:
        print('Tempo de execução:', time.time() - start_time)
    return selection

# Função encapsulando a lógica
def group_names(names, verbose=False):

    names_s = sorted(set(names.tolist()))

    if len(names_s) == 1:
        return pd.DataFrame({'res_aux': [[names_s[0]]], 'resultado_prefixes': [[]]}, index=names.index)
        
    grouped = separate_groups(names_s, verbose=verbose)
    
    grouped = [sel for k, sel in grouped.items()]
    
    group_dict = {name: {'names': group['names'], 'common_prefixes': group['common_prefixes'] if group['common_prefixes'] else None} for group in grouped for name in group['names']}

    result_names = [group_dict[name]['names'] for name in names]
    result_prefixes = [group_dict[name]['common_prefixes'] for name in names]
    return pd.DataFrame({'res_aux': result_names, 'resultado_prefixes': result_prefixes}, index=names.index)



def group_names_df(df, group_cols, nome_col, delete_aux_cols=True, verbose=False):
    df = df.copy()
    df[['res_aux', 'resultado_prefixes']] = df.groupby(group_cols, group_keys=False)[nome_col].apply(group_names, verbose=verbose)
    df['resultado_prefixes'] = df['resultado_prefixes'].apply(lambda x: x if x is not None else [])
    df['resultado_prefixes'] = df.apply(lambda x: [y for y in x['resultado_prefixes'] if y in x['res_aux']], axis=1)
    df['retirado_de_resultado'] = df.apply(lambda x: x[nome_col] in x['resultado_prefixes'], axis=1)
    df['resultado_names'] = df.apply(lambda x: x['resultado_prefixes'] if  (x[nome_col] in x['resultado_prefixes']) else [y for y in x['res_aux'] if y not in x['resultado_prefixes']], axis=1)
    if delete_aux_cols:
        df = df.drop(columns=['resultado_prefixes', 'res_aux', 'retirado_de_resultado'])
    return df




business_names = [ 
    'BORRACHARIAROSARI', 'BORRACHARIAROSARIO',
    'GALETOOSVALDO', 
    'GALETOAOSVALDO',
    'GALETO', 
    'GALETOAMANDA', 'GALET', 
    'GALETO', 
    'GAL',
    'GALETA', 'GALETARIA', 
    'GALETO', 
    'GALETOA',
    'ANDREINA', 'ANDREIA', 'ANDREIA', 'ANDREIAESTETICAEB', 'ANDREIAESTETICAEBRONZ',
    'AUTOCAR', 'AUTOCARRO', 'AUTOCARCENTRO',
    'BARBEARIAAGAPE', 'BARBEARIASTYLLODARAPA',
    'SOZINHO',
    'BARBEARIAVALHALLA', 'BARBEARIA',
]


def transformar_intervalo_em_lista(intervalo, lista_strings):
    inicio, fim = intervalo
    inicio -= 1
    return lista_strings[inicio:fim]



def example():

    # Dados de exemplo
    data = {
        'nome': business_names
    }



    df = pd.DataFrame(data)
    df['inicio'] = df['nome'].str[0]

    df = df.sort_values(by='nome')

    df = group_names_df(df, group_cols='inicio', nome_col='nome',
                        delete_aux_cols=False, verbose=False)

    df = df.drop(columns=['inicio', 'retirado_de_resultado', 'resultado_names'])
     #Exibir o DataFrame resultante

    

    #print(separate_groups(business_names))
    #print(optimized_grouping(business_names))
    return df

import numpy as np


def insere_um_apos_zero(lista):
  """Insere um 1 após cada zero em uma lista.

  Args:
    lista: Uma lista de números.

  Returns:
    Uma nova lista com os 1s inseridos.
  """

  nova_lista = []
  for num in lista:
    nova_lista.append(num)
    if num == 0:
      nova_lista.append(1)
  return np.array([1] + nova_lista)



def segunda_diagonal(matrix):
    # Adicionando uma linha de zeros no topo
    linha_zeros = np.zeros((1, matrix.shape[1]), dtype=matrix.dtype)
    matriz_ampliada = np.vstack((matrix, linha_zeros))

    # Adicionando uma coluna de zeros à direita
    coluna_zeros = np.zeros((matriz_ampliada.shape[0], 1), dtype=matrix.dtype)
    matriz_ampliada = np.hstack((coluna_zeros, matriz_ampliada))

    # Extraindo a diagonal principal da nova matriz ampliada
    segunda_diagonal_ampliada = np.diag(matriz_ampliada)

    return segunda_diagonal_ampliada[1:-1]


def generate_intervals(arr):

    modified_arr = insere_um_apos_zero(arr)
    
    # Passo 2: Quebrar o array nos 0s
    sub_arrays = []
    current_sub_array = []
    for num in modified_arr:
        if num == 0:
            if current_sub_array:
                sub_arrays.append(current_sub_array)
                current_sub_array = []
        else:
            current_sub_array.append(num)
    if current_sub_array:
        sub_arrays.append(current_sub_array)
    
    # Passo 3: Calcular os intervalos para cada subsequência
    intervals = []
    start_index = 1  # Iniciando em 1 conforme a lógica de intervalos fornecida
    for sub_array in sub_arrays:
        end_index = start_index + len(sub_array) - 1
        intervals.append((start_index, end_index))
        start_index = end_index + 1
    
    return intervals



def intervalo_por_numero(lista_intervalos, labels, lista2):
  """
  Dada uma lista de intervalos [(inicio, fim)], retorna um dicionário onde a chave é um número
  e o valor é o intervalo ao qual ele pertence.

  Args:
    lista_intervalos: Uma lista de tuplas representando intervalos [(inicio, fim)].

  Returns:
    Um dicionário onde a chave é um número e o valor é o intervalo ao qual ele pertence.
  """

  resultado = {}
  for i, (inicio, fim) in enumerate(lista_intervalos):
    for numero in range(inicio, fim + 1):
      name = labels[numero-1]

      elemento2 = lista2[i]
      if elemento2 is not None:
          inicio2, fim2 = elemento2
          labels2 = labels[inicio2-1:fim2]
      else:
          labels2 = []
      resultado[name] = [labels[inicio-1:fim],labels2]
  return resultado


def gen_interv2(matrix, lista_intervalos):
    # Use matrix.dtype para garantir que a linha de zeros tenha o mesmo tipo da matriz original
    linha_zeros = np.zeros((1, matrix.shape[1]), dtype=matrix.dtype)
    matrix = np.vstack((matrix, linha_zeros))

    resultado = []
    for inicio, fim in lista_intervalos:
        inicio -= 1
        fim -= 1
        linha = matrix[fim+1,inicio:fim+1]
        posicao_primeiro_zero = np.where(linha==0)[0][0]
        if posicao_primeiro_zero>0:
            resultado.append((inicio+1, inicio+posicao_primeiro_zero))
        else: 
            resultado.append(None)
    return resultado

def matrix_treat(matrix, row_labels):

    segunda_diagonal_arr = segunda_diagonal(matrix)
    res = generate_intervals(segunda_diagonal_arr)

    intervalo_prefixes = gen_interv2(matrix, res)
    res = intervalo_por_numero(res, list(row_labels), intervalo_prefixes)

    #res = pd.DataFrame(res).T.reset_index()
    #res.columns = ['nome', 'res_aux', 'resultado_prefixes']

    #display(res)

    return res

import time

   
def to_str(df):
    df['res_aux'] = df['res_aux'].apply(lambda x: sorted(x)).astype(str)
    df['resultado_prefixes'] = df['resultado_prefixes'].apply(lambda x: sorted(x)).astype(str)
    if 'resultado_names' in df.columns:
        df['resultado_names'] = df['resultado_names'].apply(lambda x: sorted(x)).astype(str)
    return df

def transform_check(df):
    df = df.copy()
    df = df[['nome_master', 'res_aux', 'resultado_prefixes', 'resultado_names']]
    df = to_str(df).drop_duplicates()
    df = df.sort_values(by='nome_master').reset_index(drop=True)
    return df

def transform_to_matrix_and_pivot(names_s):
    df = pd.Series(names_s, name='nome').to_frame()
    df['nome'] = df['nome'].astype(str)
    df = df[df['nome'].notnull()]
    df['key'] = 1

    df = df.merge(df, on='key', how='left', suffixes=('_1', '_2'))


    #df = df[df['nome_1'] != df['nome_2']]

    df = df[df['nome_1'].str.len() >=  df['nome_2'].str.len()]

    #df['len_nome2'] = df['nome_2'].str.len()
    #df['name_1_trunc'] = df['nome_1'].str[:df['len_nome2']]

    #df['starts_with'] = df.apply(lambda x: x['nome_1'][:len(x['nome_2'])] == x['nome_2'], axis=1)
    
    nome1_array = df['nome_1'].values.astype(str)
    nome2_array = df['nome_2'].values.astype(str)

    df['starts_with'] = np.char.startswith(nome1_array, nome2_array)

    # to matrix nome_1 x nome_2
    df = df.pivot(index='nome_1', columns='nome_2', values='starts_with').fillna(False)*1
    return df

def make_as_matrix(names_s):
    names_array = np.array(names_s, dtype=str)
    n = len(names_array)
    # *** AQUI ESTÁ A MUDANÇA MAIS IMPORTANTE ***
    # Altere np.int8 para reduzir o consumo de memória.
    # Uma matriz de True/False (1/0) pode usar np.int8 (1 byte por elemento)
    # em vez de np.float64 (8 bytes por elemento).
    starts_with_matrix = np.zeros((n, n), dtype=np.int8) 

    for i, nome_1 in enumerate(names_array):
        for j, nome_2 in enumerate(names_array):
            if i == j:
                starts_with_matrix[i, j] = 1
            elif len(nome_1) >= len(nome_2) and nome_1.startswith(nome_2):
                starts_with_matrix[i, j] = 1
    return starts_with_matrix

def merge_and_matrix(names):

    
    names_s = sorted(set(names.tolist()))
    len_names = len(names_s)

    if len_names == 1:
        return pd.DataFrame({'res_aux': [[names_s[0]]], 'resultado_prefixes': [[]]}, index=names.index)

    if len_names > 1000:
        print('Quantidade de nomes:', len_names)
        start_time = time.time()
    
    #df = transform_to_matrix_and_pivot(names_s)

    #matrix = df.values
    #row_labels = df.index
    matrix = make_as_matrix(names_s)
    row_labels = names_s
    df = matrix_treat(matrix, row_labels)


    if len_names > 1000:
        print('Tempo de execução:', time.time() - start_time)

    group_dict = {k: {'names':v[0], 'common_prefixes':v[1]} for k, v in df.items()}
    result_names = [group_dict[name]['names'] for name in names]
    result_prefixes = [group_dict[name]['common_prefixes'] for name in names]
    
    return pd.DataFrame({'res_aux': result_names, 'resultado_prefixes': result_prefixes}, index=names.index)

def group_names_df_v2(df, group_cols, nome_col, delete_aux_cols=True):
    df = df.copy()
    df[['res_aux', 'resultado_prefixes']] = df.groupby(group_cols, group_keys=False)[nome_col].apply(merge_and_matrix)
    df['resultado_prefixes'] = df['resultado_prefixes'].apply(lambda x: x if x is not None else [])
    df['resultado_prefixes'] = df.apply(lambda x: [y for y in x['resultado_prefixes'] if y in x['res_aux']], axis=1)
    df['retirado_de_resultado'] = df.apply(lambda x: x[nome_col] in x['resultado_prefixes'], axis=1)
    df['resultado_names'] = df.apply(lambda x: x['resultado_prefixes'] if  (x[nome_col] in x['resultado_prefixes']) else [y for y in x['res_aux'] if y not in x['resultado_prefixes']], axis=1)
    if delete_aux_cols:
        df = df.drop(columns=['resultado_prefixes', 'res_aux', 'retirado_de_resultado'])
    return df



def example_new():
    
    # Dados de exemplo
    data = {
        'nome': business_names
    }

    df = pd.DataFrame(data)
    df['inicio'] = df['nome'].str[0]

    df = df.sort_values(by='nome')

    df = group_names_df_v2(df, group_cols='inicio', nome_col='nome')

    df = df.drop(columns=['inicio', 'retirado_de_resultado', 'resultado_names'])
     #Exibir o DataFrame resultante

    return df