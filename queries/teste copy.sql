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
sum(if(reference_month = '2025-08-31', qtd_mmhid, 0)) qtd_mmhid_ago,
sum(if(reference_month = '2025-07-31', qtd_mmhid, 0)) qtd_mmhid_jul,
from aux1_qtd_mmhid
group by 1
)



, table_stone_mmhid_com_problema as (
select
stonecode,
mmhid, 
reference_month,
from aux
inner join (select distinct stonecode from aux_qtd_mmhid where qtd_mmhid_ago>qtd_mmhid_jul) using(stonecode)
left join (select distinct stonecode, mmhid from aux where reference_month < '2025-08-01' ) c using(stonecode, mmhid)
where c.stonecode is null --# apenas quem não aparece em outros meses
and reference_month = '2025-08-31'
)


, aux2 as ( --# uma linha por tam_id
select 
reference_month,
tam_id,
document, 
mmhid, 
min(p.stonecode is not null) all_stonecode_problem
from aux a
left join table_stone_mmhid_com_problema p using(stonecode,
mmhid, 
reference_month)
group by 1, 2, 3, 4
)

select
reference_month,
count(distinct tam_id) qtd_tam_id,
count(distinct mmhid) qtd_mmhid,
count(distinct document) qtd_document,
count(distinct IF(not all_stonecode_problem, tam_id, null)) qtd_tam_id_sem_problema,
count(distinct IF(not all_stonecode_problem, mmhid, null)) qtd_mmhid_sem_problema,
count(distinct IF(not all_stonecode_problem, document, null)) qtd_document_sem_problema
from aux2
group by 1
order by 1 