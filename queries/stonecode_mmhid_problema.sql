
CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.temp_ruan_stonecode_mmhid_problema` AS

with aux as (
SELECT distinct
reference_month,
nome_master,
stone_code stonecode,
document, 
mmhid_orig,
cod_muni,
IF(new_mmhid_merge=99, null , new_mmhid_merge) mmhid,
tam_id, 
FROM  `dataplatform-prd.master_contact.temp_ruan_tam_stone`
)


, aux1_qtd_mmhid as (
select
reference_month,
stonecode,
count(distinct mmhid) qtd_mmhid
from aux
group by 1, 2
)


, aux_qtd_mmhid as (
select
stonecode, 
sum(if(reference_month = '2025-09-30', qtd_mmhid, 0)) qtd_mmhid_set,
sum(if(reference_month = '2025-07-31', qtd_mmhid, 0)) qtd_mmhid_jul,
sum(if(reference_month = '2025-08-31', qtd_mmhid, 0)) qtd_mmhid_ago,
from aux1_qtd_mmhid
group by 1
)



select
stonecode,
mmhid, 
reference_month,
from aux
inner join (select distinct stonecode from aux_qtd_mmhid where qtd_mmhid_set>qtd_mmhid_jul) using(stonecode)
left join (select distinct stonecode, mmhid from aux where reference_month < '2025-09-01' ) c using(stonecode, mmhid)
where c.stonecode is null --# apenas quem não aparece em outros meses
and reference_month = '2025-09-30'

union all

select
stonecode,
mmhid, 
reference_month,
from aux
inner join (select distinct stonecode from aux_qtd_mmhid where qtd_mmhid_ago>qtd_mmhid_jul) using(stonecode)
left join (select distinct stonecode, mmhid from aux where reference_month < '2025-08-01' ) c using(stonecode, mmhid)
where c.stonecode is null --# apenas quem não aparece em outros meses
and reference_month = '2025-08-31'