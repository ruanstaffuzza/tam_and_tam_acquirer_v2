
with aux as (
select distinct 
de42,
merchant_market_hierarchy_id,
adquirente_padroes adquirente,
from `dataplatform-prd.master_contact.aux_tam_pre_acquirer`
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
