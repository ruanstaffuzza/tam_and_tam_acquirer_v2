
select *
from (select document from `dataplatform-prd.master_contact.temp_ruan_tpv_sample` limit 1000) s
inner join `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places` p on s.document = p.merchant_tax_id
left join `dataplatform-prd.master_contact.aux_tam_subs_asterisk_city_v2` sa on 
p.merchant_market_hierarchy_id = sa.merchant_market_hierarchy_id and p.reference_month = sa.reference_month
where p.reference_month>='2024-10-01'