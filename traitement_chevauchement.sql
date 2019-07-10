set mapreduce.job.reduces=1000;

DROP TABLE IF EXISTS  z_lab_drm_hive_temp.dm_ae_chevauchement; 
CREATE TABLE if not exists  z_lab_drm_hive_temp.dm_ae_chevauchement as 
with c as
(
	select a.*,
	b.score as score_s,b.duree as duree_s,b.tm_debut as tm_debut_s,b.tm_fin as tm_fin_s,b.type as type_s
	from  z_lab_drm_hive_temp.dm_ae_train_session b
	inner join z_lab_drm_hive_temp.dm_ae_autoroutes_session a 
	on a.sim_imsi = b.sim_imsi
	where 
		((a.tm_debut <= b.tm_debut and a.tm_fin >= b.tm_debut) or 
		(b.tm_debut <= a.tm_debut and b.tm_fin >= a.tm_debut))
)
select *,
	case when duree > duree_s then 'A' else 
	   (case when duree < duree_s then 'T' else 
		  (case when score > score_s then 'A' else 
			 (case when score < score_s then 'T' else null end)
		   end )
		end )
	end as decision,
	case
			when ((duree>duree_s and score>score_s) or (duree<duree_s and score<score_s) ) then 'coherent'
			when (duree=duree_s and score=score_s)  then 'egalite'
			else 'incoherent' 
	end as type_chevauchement
from c;
---------------

drop table if exists z_lab_drm_hive_temp.dm_ae_session_fusion_tmp;  
create table if not exists  z_lab_drm_hive_temp.dm_ae_session_fusion_tmp as 
select * from z_lab_drm_hive_temp.dm_ae_autoroutes_session
where concat(sim_imsi,tm_debut) not in (select concat(sim_imsi,tm_debut) from z_lab_drm_hive_temp.dm_ae_chevauchement);
--
with b as 
(
  select * from z_lab_drm_hive_temp.dm_ae_train_session
  where concat(sim_imsi,tm_debut) not in (select concat(sim_imsi,tm_debut_s) from z_lab_drm_hive_temp.dm_ae_chevauchement)
)
INSERT INTO z_lab_drm_hive_temp.dm_ae_session_fusion_tmp
select * from b 
ORDER BY year,month,day,sim_imsi,tm_debut;
--
with c as 
(
	select sim_imsi,case when decision='A' then score else score_s end as score,
	year,month,day,
	case when decision='A' then duree else duree_s end as duree,
	case when decision='A' then tm_debut else tm_debut_s end as tm_debut,
	case when decision='A' then tm_fin else tm_fin_s end as tm_fin,
	case when decision='A' then type else type_s end as type
from z_lab_drm_hive_temp.dm_ae_chevauchement
where type_chevauchement not in ('egalite','incoherent')
group by 
	sim_imsi,case when decision='A' then score else score_s end,
	year,month,day,
	case when decision='A' then duree else duree_s end,
	case when decision='A' then tm_debut else tm_debut_s end,
	case when decision='A' then tm_fin else tm_fin_s end,
	case when decision='A' then type else type_s end 
)
INSERT INTO z_lab_drm_hive_temp.dm_ae_session_fusion_tmp
select * from c 
ORDER BY year,month,day,sim_imsi,tm_debut;
--
INSERT INTO z_lab_drm_hive_temp.dimeng_ae_livrable
select * from z_lab_drm_hive_temp.dm_ae_session_fusion_tmp
order by year,month,day,sim_imsi,tm_debut;
--
INSERT INTO z_lab_drm_hive_temp.agent_embarque_livrable
select sim_imsi,year,month,day,tm_debut,tm_fin,type,duree,score from z_lab_drm_hive_temp.dm_ae_session_fusion_tmp
where duree >= 1800
order by year,month,day,sim_imsi,tm_debut;
-- Vérification --
-- select count(*) as nb_trajets,count(distinct sim_imsi) as nb_clients,year,month,day,type 
-- from z_lab_drm_hive_temp.agent_embarque_livrable
-- group by year,month,day,type 
-- order by year,month,day,type 