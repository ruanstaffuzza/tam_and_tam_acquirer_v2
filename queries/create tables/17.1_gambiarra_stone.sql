CREATE OR REPLACE TABLE `dataplatform-prd.master_contact.temp_ruan_gambiarra_eliminar_stone` AS



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


, aux2 as ( --# uma linha por tam_id
select 
reference_month,
tam_id,
document, 
mmhid, 
min(p.stonecode is not null) all_stonecode_problem,
from aux a
left join `dataplatform-prd.master_contact.temp_ruan_stonecode_mmhid_problema`  p using(stonecode,
mmhid, 
reference_month)
group by 1, 2, 3, 4
)

, aux3 as (
select 
reference_month,
tam_id, 
document,
mmhid,
all_stonecode_problem,
min(all_stonecode_problem) over(partition by document, reference_month) doc_com_problema
from aux2 
)



select distinct 
reference_month,
tam_id, 
from aux3
where all_stonecode_problem and not doc_com_problema
and reference_month >= '2025-08-30'