SELECT 
distinct de42
FROM `dataplatform-prd.master_contact.aux_tam_pre_acquirer` 
--inner join (select reference_date reference_month, merchant_market_hierarchy_id, tam_id from  `dataplatform-prd.master_contact.aux_tam_final_tam` ) using(reference_month, merchant_market_hierarchy_id)
where adquirente_padroes = 'unknown'
and de42 not in (select distinct de42 from `dataplatform-prd.master_contact.de42_unknown_acquirer_model`)
