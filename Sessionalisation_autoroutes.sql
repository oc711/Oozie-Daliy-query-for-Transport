SET mapreduce.job.reduces=1000;
SET hive.auto.convert.join=false;

DROP TABLE if exists z_lab_drm_hive_temp.dm_ae_autoroutes_bi_session; 
CREATE TABLE if not exists  z_lab_drm_hive_temp.dm_ae_autoroutes_bi_session as 
with a as
	(
		SELECT *,
		CASE WHEN (gps_tm - LAG(gps_tm,1,gps_tm) over (PARTITION BY sim_imsi ORDER BY row_num) <= 3600 ) THEN 0 ELSE -3 END as bi_score_1,
		CASE WHEN (
					(vitesse >= 30 and vitesse <= 160) and 
					(
						(dist_pt_to_geo_arcep <= 0.001 and 
						LAG(dist_pt_to_geo_arcep) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001 and
						LAG(dist_pt_to_geo_arcep,2) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001) 
						or
						(dist_pt_to_geo_arcep <= 0.001 and 
						LEAD(dist_pt_to_geo_arcep) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001 and
						LEAD(dist_pt_to_geo_arcep,2) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001)
						or
						(dist_pt_to_geo_arcep <= 0.001 and 
						LEAD(dist_pt_to_geo_arcep) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001 and
						LAG(dist_pt_to_geo_arcep)  over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001 )
					)
				)
	    THEN 1 ELSE 0 END AS bi_score_2, -- condition d'entrer d'un trajet 
		CASE WHEN (
					(
						(
							(vitesse < 30 or vitesse > 160) and
							dist_pt_to_geo_arcep > 0.001 and 
							LAG(dist_pt_to_geo_arcep) over (PARTITION BY sim_imsi ORDER BY row_num) > 0.001 and
							LAG(dist_pt_to_geo_arcep,2) over (PARTITION BY sim_imsi ORDER BY row_num) > 0.001
						) 
						or
						(
							(vitesse < 30 or vitesse > 160) and 
							dist_pt_to_geo_arcep > 0.001 and 
							LEAD(dist_pt_to_geo_arcep) over (PARTITION BY sim_imsi ORDER BY row_num) > 0.001 and
							LEAD(dist_pt_to_geo_arcep,2) over (PARTITION BY sim_imsi ORDER BY row_num) > 0.001
						)
						or
						(
							(vitesse < 30 or vitesse > 160) and
							dist_pt_to_geo_arcep > 0.001 and 
							LEAD(dist_pt_to_geo_arcep) over (PARTITION BY sim_imsi ORDER BY row_num) > 0.001 and
							LAG(dist_pt_to_geo_arcep)  over (PARTITION BY sim_imsi ORDER BY row_num) > 0.001
						)
					)
				  )
	    THEN 0 ELSE 1 END AS bi_score_3, -- condition d'arret d'un trajet 
	    CASE WHEN (
						(vitesse >= 30 and vitesse <= 160) 
						or
						(
							(
								dist_pt_to_geo_arcep <= 0.001 and 
								(
									( LAG(dist_pt_to_geo_arcep,2) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001 or LEAD(dist_pt_to_geo_arcep,2) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001)
									or 
									( LAG(dist_pt_to_geo_arcep) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001 or LEAD(dist_pt_to_geo_arcep) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001 )
								)
							) 
							or
							(
								dist_pt_to_geo_arcep > 0.001 and 
								(
									( LAG(dist_pt_to_geo_arcep,2) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001 and LEAD(dist_pt_to_geo_arcep,2) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001)
									or 
									( LAG(dist_pt_to_geo_arcep) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001 and LEAD(dist_pt_to_geo_arcep) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001 )
									or
									( LAG(dist_pt_to_geo_arcep) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001 and LEAD(dist_pt_to_geo_arcep,2) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001)
									or
									( LAG(dist_pt_to_geo_arcep,2) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001 and LEAD(dist_pt_to_geo_arcep) over (PARTITION BY sim_imsi ORDER BY row_num) <= 0.001)
								)
							) 
						)
				)
	    THEN 1 ELSE 0 END as bi_score_4
		FROM  z_lab_drm_hive_temp.dm_ae_autoroutes_resultat
	--	WHERE year=${Y} and month=${M} and day=${D} 
),
b as  
(	    SELECT 
		a.*, 
		CASE WHEN (bi_score_1+bi_score_2+ bi_score_3+bi_score_4)>2
		THEN 0 ELSE
		 (
			   CASE WHEN 
			  (
			     (bi_score_1+bi_score_2+bi_score_3+bi_score_4)=2 and 
			      LAG(bi_score_1+bi_score_2+bi_score_3+bi_score_4) over (PARTITION BY sim_imsi ORDER BY row_num)=3 and 
			      LEAD(bi_score_1+bi_score_2+bi_score_3+bi_score_4) over (PARTITION BY sim_imsi ORDER BY row_num)=3
			   )
			   THEN 0 ELSE null END
		 )
		END AS bi_score_session from a
),
c as 
( select b.*, case when bi_score_session=0 and LAG(bi_score_session) over (PARTITION BY sim_imsi ORDER BY row_num) is null then 1 else 0 end as bi_score_session_large from b)
select 
	   sim_imsi,gps_tm,dist_pt_to_geo_arcep,vitesse,num_route,bi_score_session_large,
	   row_number() over (PARTITION BY sim_imsi order by gps_tm asc) as row_num,
       SUM(bi_score_session_large) over (PARTITION BY sim_imsi ORDER BY row_num rows between unbounded preceding and current row) AS session_id_large,
	   gps_lat,gps_lon,zip_code,city,rad_lac,year,month,day from c
	   where bi_score_session is not null;

----

DROP TABLE IF EXISTS  z_lab_drm_hive_temp.dm_ae_autoroutes_session; 
CREATE TABLE if not exists  z_lab_drm_hive_temp.dm_ae_autoroutes_session as 
with l as 
(
	SELECT 
			sim_imsi,session_id_large,gps_tm,year,month,day,
			(SUM(case when bi_score_session_large = 0 then 1 else 0 end) over (PARTITION BY sim_imsi,session_id_large order by session_id_large )+1) as score,
			CASE WHEN session_id_large - lag(session_id_large,1,0)  over (PARTITION BY sim_imsi ORDER BY row_num) = 1 THEN gps_tm ELSE null END AS tm_debut
            -- CASE WHEN session_id_large - lag(session_id_large,1,0)  over (PARTITION BY sim_imsi ORDER BY row_num) = 1 THEN rad_lac ELSE null END AS lac_debut,
			-- CASE WHEN session_id_large - lag(session_id_large,1,0)  over (PARTITION BY sim_imsi ORDER BY row_num) = 1 THEN gps_lat ELSE null END AS lat_debut,
			-- CASE WHEN session_id_large - lag(session_id_large,1,0)  over (PARTITION BY sim_imsi ORDER BY row_num) = 1 THEN gps_lon ELSE null END AS lon_debut
	FROM  z_lab_drm_hive_temp.dm_ae_autoroutes_bi_session
),
r as 
(
	SELECT 
		sim_imsi,session_id_large,gps_tm,year,month,day,
		(SUM(case when bi_score_session_large = 0 then 1 else 0 end) over (PARTITION BY sim_imsi,session_id_large order by session_id_large)+1) as score, 
		CASE WHEN session_id_large - lead(session_id_large,1,session_id_large+1) over (PARTITION BY sim_imsi ORDER BY row_num) = -1 THEN gps_tm ELSE null END AS tm_fin
	    -- CASE WHEN session_id_large - lead(session_id_large,1,session_id_large+1) over (PARTITION BY sim_imsi ORDER BY row_num) = -1 THEN rad_lac ELSE null END AS lac_fin,
		-- CASE WHEN session_id_large - lead(session_id_large,1,session_id_large+1) over (PARTITION BY sim_imsi ORDER BY row_num) = -1 THEN gps_lat ELSE null END AS lat_fin,
		-- CASE WHEN session_id_large - lead(session_id_large,1,session_id_large+1) over (PARTITION BY sim_imsi ORDER BY row_num) = -1 THEN gps_lon ELSE null END AS lon_fin
        FROM  z_lab_drm_hive_temp.dm_ae_autoroutes_bi_session
)
SELECT 
	l.sim_imsi,l.score,l.year,l.month,l.day,
	(tm_fin - tm_debut) as duree,
	from_unixtime(tm_debut) as tm_debut,
	from_unixtime(tm_fin) as tm_fin,
	'Autoroute' as type
FROM l
JOIN  r 
ON 
	l.sim_imsi = r.sim_imsi and 
	l.session_id_large = r.session_id_large	
WHERE
	tm_debut is not null and tm_fin is not null and l.score>=3; 
--
INSERT INTO z_lab_drm_hive_temp.dimeng_ae_autoroutes_session
select * from z_lab_drm_hive_temp.dm_ae_autoroutes_session
order by year,month,day,sim_imsi,tm_debut;

