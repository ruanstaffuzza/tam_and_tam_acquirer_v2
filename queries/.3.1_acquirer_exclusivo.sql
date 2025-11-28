
--DECLARE ref_month  DATE DEFAULT '{{ref_month}}';

--CREATE OR REPLACE TABLE `dataplatform-prd.economic_research.tam_acquirer_exclusivos` PARTITION BY reference_date AS

--INSERT INTO `dataplatform-prd.economic_research.tam_acquirer_exclusivos`



CREATE VIEW `dataplatform-prd.economic_research.tam_acquirer_exclusivos` AS


with final_mid_de42 as (
select distinct
reference_month,
merchant_market_hierarchy_id,
RPAD(TRIM(de42_merchant_id), 15, ' ') AS de42,
from `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.mid_de42` a
left join `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places` p using(merchant_market_hierarchy_id, reference_month)
where 1=1
and merchant_market_hierarchy_id is not null
and de42_merchant_id is not null
)


SELECT
reference_month reference_date,
merchant_market_hierarchy_id,
MAX(IF( NOT RIGHT(de42, 6) = '000000', 1, 0)) as de42_not_stone,
MAX(IF( RIGHT(de42, 6) = '000000', 1, 0)) as de42_stone,
FROM final_mid_de42 a
group by 1, 2