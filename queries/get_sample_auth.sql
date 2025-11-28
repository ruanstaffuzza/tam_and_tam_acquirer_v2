
with city AS (
select distinct
documento document, 
CAST(cod_muni_espacial AS INT) cod_muni,
from `dataplatform-prd.economic_research.geo_stoneco`
)



select 
document,
TRIM(sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(REGEXP_REPLACE(TRIM(UPPER(name)),  r'[0-9.,\-+]', ''))) name,
postalcode,
city,
smid, 
de42,
TRIM(sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.NORMALIZE_ACCENTS(REGEXP_REPLACE(TRIM(UPPER(de43)),  r'[0-9.,\-+]', ''))) de43,
parc,
reference_month reference_date,
cod_muni,
from (select document from `dataplatform-prd.master_contact.temp_ruan_tpv_sample` limit 1000) s
inner join `dataplatform-prd.master_contact.temp_ruan_autorizador` a on s.document = a.tax_id
left join city c using (document)
where de42 LIKE