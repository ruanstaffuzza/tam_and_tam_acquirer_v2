DECLARE ref_month  DATE DEFAULT '{{ref_month}}';

--CREATE or REPLACE TABLE `dataplatform-prd.master_contact.v2_aux_tam_pre_acquirer` PARTITION BY reference_month AS
--Filtro de ref_month apenas no final. Não pode antes pois a elimitação de duplicados é feita usando todos os períodos


INSERT INTO `dataplatform-prd.master_contact.v2_aux_tam_pre_acquirer`


with mid_de42 as (
select distinct
STARTS_WITH(merchant_descriptor, 'PAG*') starts_with_pag,
reference_month,
merchant_market_hierarchy_id,
de42_merchant_id,
IF(merchant_tax_id='', CAST(merchant_market_hierarchy_id as STRING), merchant_tax_id)  tax_id_modified,
RPAD(TRIM(de42_merchant_id), 15, ' ') AS de42,
TRIM(de42_merchant_id) AS trim_de42,
--CASE WHEN REGEXP_CONTAINS(original_city, r'\p{Ll}') THEN TRUE ELSE FALSE END AS lower_city,
CASE WHEN REGEXP_CONTAINS(original_name, r'\p{Ll}') THEN TRUE ELSE FALSE END AS lower_name,
count(distinct concat(
  REGEXP_REPLACE(TRIM(REGEXP_REPLACE(nome_limpo, r'^SHOPEE|IFOOD', '')), r'[^A-Z]', ''),
  cod_muni)) over(partition by reference_month, RPAD(TRIM(de42_merchant_id), 15, ' ')
) as qtd_nomes_x_muni
from `dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city`
left join `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places` p using(merchant_market_hierarchy_id, reference_month)
where 1=1
and subs_asterisk in ('Outros')
and merchant_market_hierarchy_id is not null
and de42_merchant_id is not null
)

, aux_repeated as (
SELECT  DISTINCT 
LTRIM(trim_de42, '0') AS l0trim_de42,
  tax_id_modified
FROM
  mid_de42
WHERE
   LTRIM(trim_de42, '0') != trim_de42
)

--# Eliminated repeated withou zero

, final_mid_de42 as (
select 
distinct 
starts_with_pag,
reference_month,
merchant_market_hierarchy_id,
de42,
--lower_city,
lower_name, 
qtd_nomes_x_muni,
from mid_de42 a
left join aux_repeated b on a.trim_de42 = b.l0trim_de42 and a.tax_id_modified = b.tax_id_modified
where b.l0trim_de42 is null
)



SELECT
a.*,
CASE WHEN (starts_with_pag or (LENGTH(TRIM(de42))=11 and TRIM(de42) LIKE '0%' )) then 'PagSeguro'  
    WHEN RIGHT(de42, 6) = '000000' then 'Stone'
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
    WHEN STARTS_WITH(de42, 'XXXXXXXX') THEN 'Desconhecido - XXXX'
    WHEN LENGTH(TRIM(de42)) = 12 AND STARTS_WITH(TRIM(de42), '10') THEN 'Sipag'
    WHEN STARTS_WITH(TRIM(de42), '000101000') THEN 'Vero'
    WHEN STARTS_WITH(TRIM(de42), '003000000') THEN 'BMG/Granito'
    WHEN NOT STARTS_WITH(TRIM(de42), '0') AND LENGTH(TRIM(de42)) < 15 THEN 'possivel_erro'
    ELSE 'unknown'
END AS adquirente_padroes,
--count(distinct concat(nome_master, cod_muni)) over(partition by reference_month, de42) tam_de42,
from final_mid_de42 a
where 1=1
and TRIM(de42)<> '088557936' -- Exclusão do de42 de Ifood na Rede
and reference_month = ref_month
