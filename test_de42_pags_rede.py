#%%

from utils import (save_query, download_data, read_data)

query = """
with aux as (
select distinct 
de42,
merchant_market_hierarchy_id,
adquirente_padroes adquirente,
from `dataplatform-prd.master_contact.aux_tam_pre_acquirer_v3`
 where adquirente_padroes in ('Rede', 'PagSeguro')
 and reference_month = '2024-09-30'
 )


select distinct
a.de42,
a.adquirente = 'Rede' rede,
from aux a
left join (select distinct merchant_market_hierarchy_id from aux where adquirente = 'PagSeguro') b using(merchant_market_hierarchy_id)
left join (select distinct merchant_market_hierarchy_id from aux where adquirente = 'Rede') c using(merchant_market_hierarchy_id)
where b.merchant_market_hierarchy_id is null or c.merchant_market_hierarchy_id is null
qualify row_number() over (partition by a.adquirente order by rand()) <= 10000
"""
name_query = 'sample_de42_pags_rede'

save_query(name_query, query)
download_data(name_query)

#%%

import pandas as pd 

def load_and_format_de42_banking(handle_rede_format: bool):
    
    # Define the mapping of acquirer names to formatted names
    acquirer_name_mapping = {
        'PAGSEGURO INTERNET LTDA': 'PagSeguro',
        'REDECARD, S.A.': 'Rede',
        'CIELO S.A.': 'Cielo',
        'Pagseguro Internet S.A': 'PagSeguro',
        'GETNET ADQUIRENCIA E SERVICOS': 'GetNet',
        'Cielo S.A.': 'Cielo',
        'Redecard S.A.': 'Rede',
        'GetNet Adquirencia E Servicos Para Meios de Pagamento, S.A.': 'GetNet',
        'BANCO SAFRA S/A': 'Safra',
        'BANCO COOPERATIVO SICOOB S.A.': 'Sipag',
        'MERCADOPAGOCOM REPRESENTACOES': 'MercadoPago',
        'SUMUP SOLUCOES DE PAGAMENTO LT': 'SumUp',
        'Sumup Solucoes De Pagamentos Brasil Ltda': 'SumUp',
        'Banco Safra S.A.': 'Safra',
        'BANCO COOPERATIVO SICREDI SA': 'Sicredi',
        'BANCO BMG S/A': 'BMG',
        'FIRST DATA DO BRASIL SOLUCOES': 'Fiserv',
        'Banco BMG S.A.': 'BMG',
        'ADIQ SOLUCOES DE PAGAMENTO SA': 'Adiq',
        'BANCO TRIANGULO S/A': 'Tribanco',
        'Banco Cooperativo do Brasil S.A. - Bancoob': 'Sipag',
        'BANCO DO ESTADO DO RIO GRANDE': 'Vero/Banrisul',
        'FD DO BRASIL SOLUCOES DE PAGAMENTO LTDA.': 'Fiserv',
        'Banco Cooperativo Sicredi S.A.': 'Sicredi',
        'Banco Triangulo S.A.': 'Tribanco',
        'ADIQ Solucoes de Pagamento S.A.': 'Adiq',
        'CLOUDWALK MEIOS DE PAGAMENTOS': 'InfinitePay',
        'Banco do Estado do Rio Grande do Sul S/A': 'Vero/Banrisul',
        'ADYEN DO BRASIL LTDA': 'Adyen',    
        'Adyen do Brasil Ltda.': 'Adyen',
        'CloudWalk, Inc.': 'InfinitePay'}
    
    
    # Load the data
    df = pd.read_parquet(r"G:\.shortcut-targets-by-id\11oihSUrjIBMrKMTPPH43oBLwcxGYIND_\Stone Economic Research\0. Projetos\Descriptors Master\data\train_de42_camel_transactions.parquet")

    # Format the acquirer names
    df['adq_s'] = df['acquirer_name'].replace(acquirer_name_mapping)

    # Handle special case for 'Sicredi' and 'Sipag'
    df.loc[df['adq_s'].isin(['Sicredi', 'Sipag']), 'adq_s'] = 'Sipag/Sicredi'

    # Identify and handle other acquirers
    df['outros'] = ~df['acquirer_name'].isin(acquirer_name_mapping.keys())
    df.loc[df['outros'], ['adq', 'adq_s']] = 'Outras'

    # Remove rows with null merchant_id
    df = df[df['merchant_id'].notnull()]

    # Format the merchant_id
    df['de42'] = df['merchant_id'].str.strip(' ').str.ljust(15, ' ')

    return df

df = load_and_format_de42_banking(False)

df = df[df['adq_s'].isin(['Rede', 'PagSeguro'])]

#%%

df['len42'] = df['de42'].str.strip().str.len()

df['target'] = (df['len42'] == 9) & (df['de42'].str.strip().str[0]=='0')



df['len42'].value_counts(normalize=True)

df['de42'].str.strip()
df = df[df['len42'] == 9]

df.de42.str[0].value_counts(normalize=True)
#%%


#%%

df = read_data(name_query)
df = df[df['rede']]
df.de42.str[0].value_counts(normalize=True)


#%%


#%%

df[~df['rede']].de42.str[2].value_counts()