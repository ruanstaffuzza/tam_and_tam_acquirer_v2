DECLARE ref_month  DATE DEFAULT '{{ref_month}}';

DECLARE max_ref_acquirer   DATE;
DECLARE max_ref_tier       DATE;
DECLARE max_ref_tam        DATE;
DECLARE max_ref_tam_geo    DATE;

DECLARE max_destino_ref_acquirer   DATE;
DECLARE max_destino_ref_tier       DATE;
DECLARE max_destino_ref_tam        DATE;
DECLARE max_destino_ref_tam_geo    DATE;

-- Pega o max(reference_date) de cada tabela de origem
SET max_ref_acquirer = (
  SELECT MAX(reference_date)
  FROM `dataplatform-prd.master_contact.final_tam_acquirer`
);

SET max_ref_tier = (
  SELECT MAX(reference_date)
  FROM `dataplatform-prd.master_contact.final_tam_tier`
);

SET max_ref_tam = (
  SELECT MAX(reference_date)
  FROM `dataplatform-prd.master_contact.final_tam`
);

SET max_ref_tam_geo = (
  SELECT MAX(reference_date)
  FROM `dataplatform-prd.master_contact.final_tam_geocoded`
);

SET max_destino_ref_acquirer = (
  SELECT MAX(reference_date)
  FROM `dataplatform-prd.addressable_market.tam_acquirer`
);

SET max_destino_ref_tier = (
  SELECT MAX(reference_date)
  FROM `dataplatform-prd.addressable_market.tam_tier`
);

SET max_destino_ref_tam = (
  SELECT MAX(reference_date)
  FROM `dataplatform-prd.addressable_market.tam`
);

SET max_destino_ref_tam_geo = (
  SELECT MAX(reference_date)
  FROM `dataplatform-prd.addressable_market.tam_geocoded`
);


-- Garante que TODAS as datas máximas são iguais entre si e iguais ao ref_month
ASSERT (max_ref_acquirer = ref_month) AS 'Data máxima de final_tam_acquirer não é igual a ref_month.';
ASSERT (max_ref_tier = ref_month) AS 'Data máxima de final_tam_tier não é igual a ref_month.';
ASSERT (max_ref_tam = ref_month) AS 'Data máxima de final_tam não é igual a ref_month.';
ASSERT (max_ref_tam_geo = ref_month) AS 'Data máxima de final_tam_geocoded não é igual a ref_month.';

-- Garante que ref_month não existe nas tabelas de destino
ASSERT (max_destino_ref_acquirer < ref_month) AS 'ref_month já existe em addressable_market.tam_acquirer.';
ASSERT (max_destino_ref_tier < ref_month) AS 'ref_month já existe em addressable_market.tam_tier.';
ASSERT (max_destino_ref_tam < ref_month) AS 'ref_month já existe em addressable_market.tam.';
ASSERT (max_destino_ref_tam_geo < ref_month) AS 'ref_month já existe em addressable_market.tam_geocoded.';


--CREATE OR REPLACE TABLE `dataplatform-prd.addressable_market.tam_acquirer`  partition by reference_date as 
INSERT INTO `dataplatform-prd.addressable_market.tam_acquirer`
select * from `dataplatform-prd.master_contact.final_tam_acquirer`
where reference_date = ref_month
;

--CREATE OR REPLACE TABLE `dataplatform-prd.addressable_market.tam_tier`  partition by reference_date as 
INSERT INTO `dataplatform-prd.addressable_market.tam_tier`
select * from  `dataplatform-prd.master_contact.final_tam_tier`
where reference_date = ref_month
;

--CREATE OR REPLACE TABLE `dataplatform-prd.addressable_market.tam`  partition by reference_date as 
INSERT INTO `dataplatform-prd.addressable_market.tam`
select * except(cod_muni, only_market_place, only_mp_or_ml_osasco)  from `dataplatform-prd.master_contact.final_tam`
where reference_date = ref_month
;

--CREATE OR REPLACE TABLE `dataplatform-prd.addressable_market.tam`  partition by reference_date as 
INSERT INTO `dataplatform-prd.addressable_market.tam_geocoded`
select * from `dataplatform-prd.master_contact.final_tam_geocoded`
where reference_date = ref_month
;
