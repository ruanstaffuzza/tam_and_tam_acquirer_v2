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


def separate_groups(names, verbose=False):
    selection = {}

    # order names
    
    #names = list(set(names))
    #names = sorted(names)


    len_names = len(names)
    if len_names > 1000:
        print('Quantidade de nomes:', len_names)
        start_time = time.time()

    # Função para verificar se um nome está contido em outro
    def is_contained(name1, name2):
        return name1 in name2 or name2 in name1

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
    df[['res_aux', 'resultado_prefixes']] = df.groupby(group_cols, group_keys=False)[nome_col].apply(group_names, verbose=verbose)
    df['resultado_prefixes'] = df['resultado_prefixes'].apply(lambda x: x if x is not None else [])
    df['resultado_prefixes'] = df.apply(lambda x: [y for y in x['resultado_prefixes'] if y in x['res_aux']], axis=1)
    df['retirado_de_resultado'] = df.apply(lambda x: x[nome_col] in x['resultado_prefixes'], axis=1)
    df['resultado_names'] = df.apply(lambda x: x['resultado_prefixes'] if  (x[nome_col] in x['resultado_prefixes']) else [y for y in x['res_aux'] if y not in x['resultado_prefixes']], axis=1)
    if delete_aux_cols:
        df = df.drop(columns=['resultado_prefixes', 'res_aux', 'retirado_de_resultado'])
    return df


def example():

    business_names = [ 
        'BORRACHARIAROSARI', 'BORRACHARIAROSARIO',
        'GALETOOSVALDO', 
        'GALETO', 
        'GALETOAMANDA', 'GALET', 
        #'GALETO', 
        'GAL',
        'GALETA', 'GALETARIA', 
        #'GALETO', 
        'GALETOA',
        'ANDREINA', 'ANDREIA', 'ANDREIA', 'ANDREIAESTETICAEB', 'ANDREIAESTETICAEBRONZ',
        'AUTOCAR', 'AUTOCARRO', 'AUTOCARCENTRO',
        #'BARBEARIAAGAPE', 'BARBEARIASTYLLODARAPA',
        'SOZINHO',
        #'BARBEARIAVALHALLA', 'BARBEARIA',
    ]
    # Dados de exemplo
    data = {
        'nome': business_names
    }



    df = pd.DataFrame(data)
    df['inicio'] = df['nome'].str[0]

    df = df.sort_values(by='nome')

    df = group_names_df(df, group_cols='inicio', nome_col='nome',
                        delete_aux_cols=False, verbose=False)

    df = df.drop(columns=['inicio'])
     #Exibir o DataFrame resultante
    display(df)

    #print(separate_groups(business_names))
    #print(optimized_grouping(business_names))
def example2():
    business_names = [ 
        #'BORRACHARIAROSARI', 'BORRACHARIAROSARIO',
        'GALETOAOSVALDO', 
        'GALETO', 
        'GALETOAMANDA',
        'GALET',
        'GALETO', 
        'GALETA', 
        'GALETARIA',
        #'GALETO',
        #'AUTOCAR', 'AUTOCARRO', 'AUTOCARCENTRO',
        #'BARBEARIAAGAPE', 'BARBEARIASTYLLODARAPA',
        #'BARBEARIAVALHALLA', 'BARBEARIA',
    ]
    print(separate_groups(business_names))
        

if __name__ == '__main__':
    example()


