#%%
from google.cloud import bigquery

client = bigquery.Client()


# Substitua pelos seus valores
project_id = 'dataplatform-prd'
dataset_id = 'addressable_market'
table_id = 'tam'
#table_id = 'tam_acquirer'
full_table_id = f'{project_id}.{dataset_id}.{table_id}'
table = client.get_table(full_table_id)

schema = table.schema

#%%
schema

names = [field.name for field in schema]

#%%


id_empresa_str = {
    'tam': 'tam_tier',
    'tam_tier': 'tam',
    'tam_acquirer': 'tam',
}


novas_descricoes = {
 'reference_date': 'Último dia do mês de referência',
 'tam_id': 'Id único para um EC do TAM ativo no mês de referência',
 'id_empresa': f'Id da empresa (chave para linkar ao {id_empresa_str[table_id]})',
 'cpf_cnpj': 'CPF ou CNPJ do estabelecimento',
 'cpf_cnpj_raiz': 'CPF ou a raiz do CNPJ (CNPJ 8 digitos)',
 'document_type': 'Tipo de documento (CPF ou CNPJ)',
 'merchant_name': 'Nome do estabelecimento',
 'merchant_market_hierarchy_id': 'Id do estabelecimento na Base Places',
 'mcc': 'MCC do estabelecimento',
 'cnae': 'CNAE do estabelecimento',
 'porte_rfb': 'Porte do estabelecimento (Receita Federal)',
 'first_seen_week': 'Semana da primeira transação do estabelecimento',
 'is_online': 'Flag para estabelecimentos que vendem online',
 'cod_muni': 'Código do município do estabelecimento',
 'tier': 'Faixa de TPV (cartões + PIX POS)',
 'acquirer': 'Adquirente do estabelecimento',
 }

def atualizar_descricao_campo(campo, descricoes):
    if campo.name in descricoes:
        # Se o campo tiver subcampos (para tipos RECORD), atualize-os recursivamente
        novos_subcampos = [atualizar_descricao_campo(subcampo, descricoes) for subcampo in campo.fields] if campo.fields else ()
        # Retorne um novo SchemaField com a descrição atualizada
        return bigquery.SchemaField(
            name=campo.name,
            field_type=campo.field_type,
            mode=campo.mode,
            description=descricoes[campo.name],
            fields=novos_subcampos
        )
    else:
        # Se o campo tiver subcampos, continue a verificação
        novos_subcampos = [atualizar_descricao_campo(subcampo, descricoes) for subcampo in campo.fields] if campo.fields else ()
        return bigquery.SchemaField(
            name=campo.name,
            field_type=campo.field_type,
            mode=campo.mode,
            description=campo.description,
            fields=novos_subcampos
        )

novo_esquema = [atualizar_descricao_campo(campo, novas_descricoes) for campo in schema]
print(novo_esquema)
#%%

table.schema = novo_esquema
table = client.update_table(table, ['schema'])  # Requisição à API
print(f'Descrições das colunas atualizadas na tabela {full_table_id}.')


#%%

table_description = """Guia de Uso Dados TAM: https://docs.google.com/document/d/17PR9WIbl8XyMt8HjDsRHfZrapOS-swhi6v9Vl0F2KOw
Nota Técnica Dados TAM: https://docs.google.com/document/d/1ZHSWIWtq8Gl3b2dZW0lsUB7Hua3kGSX8W3hrordqris"""

table.description = table_description
table = client.update_table(table, 