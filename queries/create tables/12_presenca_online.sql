DECLARE ref_month  DATE DEFAULT '{{ref_month}}';

--CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.v2_aux_tam_presenca_online` PARTITION BY reference_date AS 


INSERT INTO `dataplatform-prd.master_contact.v2_aux_tam_presenca_online`
-- Objetivo: Identificar a presença online dos ECs

with aux1 as (
select *
from (
select 
distinct
reference_month,
subs_asterisk,
merchant_market_hierarchy_id,
nome_master nome_master_com_espaco,
numero_inicio,
cod_muni,
REGEXP_REPLACE(nome_master, r'[^A-Z]', '') nome_master,
from  `dataplatform-prd.master_contact.v2_aux_tam_get_document_master` 
where 1=1
and reference_month = ref_month
)
where 1=1
and not starts_with(nome_master, 'PICPAY')
and not starts_with(nome_master, 'GOOGLE')
and not starts_with(nome_master_com_espaco, 'ZUL ')
and not starts_with(nome_master, 'GOLLIN')
and not starts_with(nome_master, 'DEEZER')
and not starts_with(nome_master, 'RECARGA')
and not starts_with(nome_master, 'OLXBRA')
and not starts_with(nome_master, 'GRUPOOLX')
and not starts_with(nome_master, 'AVIANCA')
and not starts_with(nome_master, 'ETSAMERICAN')
and not starts_with(nome_master, 'EMIRATES')
and not starts_with(nome_master, 'AZULLINHA')
)


, big_mmhid as (
  select merchant_market_hierarchy_id , reference_month from (
  select merchant_market_hierarchy_id, reference_month,
  count(*) as qtd
  from (select distinct reference_month, merchant_market_hierarchy_id, nome_master, cod_muni from aux1)
  group by 1, 2
  )
  where qtd>10
)



, dados_de42 as (
select distinct
DATE(reference_month) reference_month,
IF(subs_asterisk not in ('CloudWalk', 'PagSeguro', 'SumUp') and big_mmhid.merchant_market_hierarchy_id is null , CAST(merchant_market_hierarchy_id as STRING), '') merchant_market_hierarchy_id,
subs_asterisk,
COALESCE(if(length(numero_inicio) in (8, 14), numero_inicio, null), '') numero_inicio,
REGEXP_REPLACE(REPLACE(nome_limpo,'IFOOD', ''), r'[^A-Z]', '') nome_master,
cod_muni,
RPAD(TRIM(de42_merchant_id), 15, ' ') AS de42,
original_name, 
from `dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city`
left join big_mmhid using(merchant_market_hierarchy_id, reference_month)
where 1=1
and date(reference_month) = ref_month
)


, res_de42 as (
select distinct
tam_id,
merchant_market_hierarchy_id,
DATE(reference_month) reference_date,
TRIM(de42) in (
 '103', --#mp',
 '000000007187540', -- #mp',
 '733012632000000', -- #pagarme',
 '171851857000000', -- #pagarme',
 '000000012514203',
 '89752392',
 '85558958',
 '539601000041886',
 '000000000000029',
 '61965014',
 '000000012514206',
 '000000000000016',
 '000000012514206 ',
 '000000013211353',
 '84817259',
 '000000007187481',
 '85424226',
 '94797200',
 '539601000002524 ',
 '93956045',
 '85708674',
 '81832656',
 '93956053',
 '000002865178522',
 '000000013211352',
 '60272716'
) de42_online,
subs_asterisk = 'Ifood' adq_ifood,
(subs_asterisk = 'MercadoPago' and cod_muni=3534401) adq_mp_osasco,
--max(subs_asterisk not in ('MercadoPago', 'MercadoPago_subPagarme' )) over(partition by tam_id) has_adq_diff_mp
from (
    select * 
    except(numero_inicio,merchant_market_hierarchy_id, reference_date), 
    COALESCE(numero_inicio, '') numero_inicio, 
    COALESCE(CAST(merchant_market_hierarchy_id as STRING), '') merchant_market_hierarchy_id, 
    reference_date reference_month,
from  `dataplatform-prd.master_contact.v3_aux_tam_pos_python`
where 1=1
and date(reference_date) = ref_month
) b 
left join dados_de42 c using(reference_month, merchant_market_hierarchy_id, subs_asterisk, numero_inicio, nome_master, cod_muni)
)




, dados_asterisk as (
select distinct
DATE(reference_month) reference_date,
CAST(merchant_market_hierarchy_id as STRING) merchant_market_hierarchy_id, 
True asterisk_online,
from `dataplatform-prd.master_contact.v2_aux_tam_subs_asterisk_city` 
where 1=1
and subs_asterisk = 'delete_asterisk'
and REGEXP_CONTAINS(REPLACE(original_name, ' ', ''), r'^(SHO|SHOPEE|HTM|HT|HOTMART|VINDI|CARTPANDA|S2P|IUGU|SHOPTIME|KIWIFY|OLX|AMERICANAS|CLINICA|AIQFOME|NU|RAPPILINKBLU|NATURAPAY|SUBMARINO|DAFITI)\*')
and date(reference_month) = ref_month
)



, dados_adq as (
select distinct 
tam_id, reference_date
FROM `dataplatform-prd.master_contact.v2_aux_tam_acquirers` 
where adquirente in ("Adyen",
                '99Food', 'Aliexpress', 'Amazon', 'Americanas', 'Hotmart', 'Magalu1', 'Magalu2', 'MercadoLivre1',
                'MercadoLivre3',
                'MercadoLivre2', 
                'MercadoPago_mmhid_problema', 
                'Shein', 'Shopee',
                'MP_mmhid_problema', 
                'MercadoLivre1_mmhid_problema',
                'Ifood',
                'Hotmart',
                'Kiwify'
                ) 


and date(reference_date) = ref_month
)



, dados_places as ( 
select distinct
DATE(reference_month) reference_date,
CAST(merchant_market_hierarchy_id as STRING) merchant_market_hierarchy_id, 
ecom_nsr,
brick_and_mortar,
from  `sfwthr2a4shdyuogrt3jjtygj160rs.mastercard.places`
where 1=1
and date(reference_month) = ref_month
)



select distinct reference_date, tam_id,
de42_online,
asterisk_online,
ecom_nsr,
adq_ifood,
adq_mp_osasco,
adyen_online
from (
select distinct
reference_date,
tam_id,
COALESCE(de42_online, False) de42_online,
COALESCE(asterisk_online, False) asterisk_online,
coalesce(ecom_nsr, False) ecom_nsr,
brick_and_mortar,
dados_adq.tam_id is not null adyen_online,
adq_ifood,
adq_mp_osasco,
from res_de42 a
left join dados_asterisk b using(reference_date, merchant_market_hierarchy_id)
left join dados_places c using(reference_date, merchant_market_hierarchy_id)
left join dados_adq using(reference_date, tam_id)
)
where (de42_online or asterisk_online or ecom_nsr or adq_ifood or adq_mp_osasco or adyen_online) 