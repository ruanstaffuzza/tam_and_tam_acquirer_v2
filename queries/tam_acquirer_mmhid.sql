
CREATE OR REPLACE TABLE `dataplatform-prd.economic_research.tam_acquirer_mmhid` PARTITION BY reference_date AS
--INSERT INTO `dataplatform-prd.master_contact.aux_tam_final_tam_acquirer_completa`



with mid_de42 as (
select distinct
STARTS_WITH(merchant_descriptor, 'PAG*') starts_with_pag,
reference_month,
merchant_market_hierarchy_id,
de42_merchant_id,
RPAD(TRIM(de42_merchant_id), 15, ' ') AS de42,
TRIM(de42_merchant_id) AS trim_de42,
CASE WHEN REGEXP_CONTAINS(original_name, r'\p{Ll}') THEN TRUE ELSE FALSE END AS lower_name,
subs_asterisk,
from `dataplatform-prd.master_contact.aux_tam_subs_asterisk_city_v2`
left join `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places` p using(merchant_market_hierarchy_id, reference_month)
where 1=1
--and subs_asterisk in ('Outros', 'Outros_Pags', 'PagSeguro')
--and (not CONTAINS_SUBSTR(merchant_descriptor, "*") or subs_asterisk in ('Outros_Pags', 'PagSeguro'))
--and not CONTAINS_SUBSTR(merchant_descriptor, "LINK-")
--and not CONTAINS_SUBSTR(merchant_descriptor, "LOTERIASONLINE")
--and not CONTAINS_SUBSTR(merchant_descriptor, "FACEBOOK")
and merchant_market_hierarchy_id is not null
and de42_merchant_id is not null
)

, aux_repeated as (
SELECT  DISTINCT 
LTRIM(trim_de42, '0') AS l0trim_de42,
  merchant_market_hierarchy_id
FROM
  mid_de42
WHERE
   LTRIM(trim_de42, '0') != trim_de42
)

--# Eliminated repeated withou zero

, final_mid_de42 as (
select 
distinct 
reference_month,
a.merchant_market_hierarchy_id,
de42,
subs_asterisk,
starts_with_pag,
lower_name,
from mid_de42 a
left join aux_repeated b on a.trim_de42 = b.l0trim_de42 and a.merchant_market_hierarchy_id = b.merchant_market_hierarchy_id
where b.l0trim_de42 is null
)


, pre_acquirer as (

SELECT
a.*,
CASE WHEN subs_asterisk not in ('Outros', 'Outros_Pags', 'PagSeguro') then subs_asterisk
    WHEN (starts_with_pag or (LENGTH(TRIM(de42))=11 and TRIM(de42) LIKE '0%' )) then 'PagSeguro'  
    WHEN RIGHT(de42, 6) = '000000' then 'StoneCo'
    WHEN LENGTH(TRIM(de42)) = 9 and TRIM(de42) LIKE '0%' THEN 'Rede'
    WHEN LENGTH(TRIM(de42)) = 15 AND STARTS_WITH(TRIM(de42), '01200') THEN 'BRB'
    WHEN LENGTH(TRIM(de42)) = 15 AND STARTS_WITH(TRIM(de42), '0190000') THEN 'Bolt'
    WHEN REGEXP_CONTAINS(TRIM(de42), r'^0(1|2|4)\d{9}(0001|0002|\d{2}00)$') 
            OR REGEXP_CONTAINS(TRIM(de42), r'^00000[1-9]\d{9}$') THEN 'Cielo'
    WHEN REGEXP_CONTAINS(TRIM(de42), r'^00000001[0-8]|^00000000[4-9]|^000000001[4-9]') THEN 'GetNet'
    WHEN (STARTS_WITH(TRIM(de42), 'M') AND LENGTH(TRIM(de42)) = 15) 
            OR TRIM(de42) IN ('34', '31', '103', '29') THEN 'Mercado Pago'
    WHEN STARTS_WITH(TRIM(de42), 'M') AND LENGTH(TRIM(de42)) <> 15 THEN 'SumUp'
    WHEN LENGTH(TRIM(de42)) < 8 AND lower_name THEN 'InfinitePay'
    WHEN REGEXP_CONTAINS(TRIM(de42), r'[^0-9 ]') THEN 'Desconhecido'
    WHEN LENGTH(TRIM(de42)) = 12 AND STARTS_WITH(TRIM(de42), '10') THEN 'Sipag'
    WHEN STARTS_WITH(TRIM(de42), '000101000') THEN 'Vero'
    WHEN STARTS_WITH(TRIM(de42), '003000000') THEN 'BMG/Granito'
    WHEN NOT STARTS_WITH(TRIM(de42), '0') AND LENGTH(TRIM(de42)) < 15 THEN 'possivel_erro'
    ELSE 'unknown'
END AS adquirente_padroes,
--count(distinct concat(nome_master, cod_muni)) over(partition by reference_month, de42) tam_de42,
from final_mid_de42 a
) 



, unknown_data as (
select distinct
merchant_market_hierarchy_id,
reference_month,
de42,
from pre_acquirer
where subs_asterisk = 'Outros'
and adquirente_padroes='unknown'
)


, aux_final as (
select distinct 
merchant_market_hierarchy_id,
reference_month,
COALESCE(prediction, 'unknown') as adquirente, 
from unknown_data
left join `dataplatform-prd.master_contact.de42_unknown_acquirer_model` using(de42)
where rank=1

union all

select distinct
merchant_market_hierarchy_id,
reference_month,
adquirente_padroes as adquirente,
from pre_acquirer
where NOT (subs_asterisk = 'Outros' and adquirente_padroes='unknown')
)

select 
merchant_market_hierarchy_id,
DATE(reference_month) as reference_date,
adquirente,
from aux_final
