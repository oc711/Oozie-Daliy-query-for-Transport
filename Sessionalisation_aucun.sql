---- TRAITEMENT MENSUEL ----

SET mapreduce.job.reduces=1000;
SET hive.auto.convert.join=false;

DROP TABLE if exists z_lab_drm_hive_temp.dm_ae_autres_session; 
CREATE TABLE if not exists  z_lab_drm_hive_temp.dm_ae_autres_session as 
with a as
(
		SELECT *,
		CASE WHEN (gps_tm - LAG(gps_tm,1,gps_tm) over (PARTITION BY sim_imsi,year,month,day ORDER BY row_num) <= 3600 ) and (vitesse<=10) THEN 0 ELSE null END as bi_score_session
		FROM  z_lab_drm_hive_temp.dm_ae_daliy_route
		WHERE  distance_diag <10 and (d_tm is not null and distance is not null and vitesse is not null) and d_tm !=0  
),b as
( select a.*, case when bi_score_session=0 and LAG(bi_score_session) over (PARTITION BY sim_imsi,year,month,day ORDER BY row_num) is null then 1 else 0 end as bi_score_session_2 from a),
c as
(
	select 
		   sim_imsi,gps_tm,vitesse,bi_score_session_2,
		   row_number() over (PARTITION BY sim_imsi,year,month,day order by gps_tm asc) as row_num,
		   SUM(bi_score_session_2) over (PARTITION BY sim_imsi,year,month,day ORDER BY row_num rows between unbounded preceding and current row) AS session_id,
		   gps_lat,gps_lon,zip_code,city,rad_lac,year,month,day from b
		   where bi_score_session is not null
),l as 
(
	SELECT 
			sim_imsi,session_id,gps_tm,year,month,day,
			(SUM(case when bi_score_session_2 = 0 then 1 else 0 end) over (PARTITION BY sim_imsi,year,month,day,session_id order by session_id)+1) as score,
			CASE WHEN session_id - lag(session_id,1,0)  over (PARTITION BY sim_imsi,year,month,day ORDER BY row_num) = 1 THEN gps_tm ELSE null END AS tm_debut
	FROM c
),
r as 
(
	SELECT 
		sim_imsi,session_id,gps_tm,year,month,day,
		(SUM(case when bi_score_session_2 = 0 then 1 else 0 end) over (PARTITION BY sim_imsi,year,month,day,session_id order by session_id)+1) as score, 
		CASE WHEN session_id - lead(session_id,1,session_id+1) over (PARTITION BY sim_imsi,year,month,day ORDER BY row_num) = -1 THEN gps_tm ELSE null END AS tm_fin
        FROM  c
)
SELECT 
	l.sim_imsi,l.score,l.year,l.month,l.day,
	(tm_fin - tm_debut) as duree,
	from_unixtime(tm_debut) as tm_debut,
	from_unixtime(tm_fin) as tm_fin,
	'Autres' as type
FROM l
JOIN  r 
ON 
	l.sim_imsi = r.sim_imsi and 
	l.session_id = r.session_id and 
	l.year= r.year and 
	l.month = r.month and 
	l.day = r.day
WHERE
	tm_debut is not null and tm_fin is not null and l.score>=3; 
--
INSERT INTO z_lab_drm_hive_temp.dimeng_ae_autres_session
select * from z_lab_drm_hive_temp.dm_ae_autres_session
order by year,month,day,sim_imsi,tm_debut;
--
INSERT INTO z_lab_drm_hive_temp.agent_embarque_livrable
select sim_imsi,year,month,day,tm_debut,tm_fin,type,duree,score from .dimeng_ae_autres_session
where duree >= 1800
order by year,month,day,sim_imsi,tm_debut;

