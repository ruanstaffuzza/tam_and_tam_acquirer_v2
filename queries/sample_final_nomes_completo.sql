select distinct
  --substr(nome_master, 6) as nome_merge,
  mmhid_merge,
  subs_asterisk in ('MercadoLivre2', 'MercadoPago', 'MercadoPago_mmhid_problema',
       'MercadoPago_subPagarme') as is_mp,
  substr(nome_master, 1, 5) as inicio,
  cod_muni,
  coalesce(IF(subs_asterisk in ('Outros_Stone', 'Ton'), coalesce(cnpj, cpf), NULL), numero_inicio) as pre_doc_unmerge,
  reference_month,
  nome_master,
from `dataplatform-prd.master_contact.v2_aux_tam_final_nomes`
where reference_month= '2025-07-31'
--and cod_muni = 5300108 --# Brasilia
and cod_muni = 3541406 --# Presidente Prudente
