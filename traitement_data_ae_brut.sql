-- select 
	-- count(*) as nb_mesure,
	-- count(distinct sim_imsi) as nb_clients_ae,
	-- case when gps_lat is not null then 1 else 0 end as flag_gps_actif,
	-- case when gps_radius < 50 then 1 else 0 end as flag_gps_precis,
	-- mode,year,month,day from z_app_bdf_hive_socle_phaeton.fai_phaeton_coverage
-- group by year,month,day,mode,case when gps_lat is not null then 1 else 0 end,case when gps_radius < 50 then 1 else 0 end

SET hive.auto.convert.join=false;
SET mapred.compress.map.output=true;
SET hive.exec.parallel = true;
SET hive.groupby.skewindata=true;
SET hive.optimize.skewjoin = true;
set hive.execution.engine=tez;
set mapreduce.job.reduces=1000;
set hive.map.aggr=false;

drop table if exists z_lab_drm_hive_temp.dm_ae_daliy;
create table if not exists  z_lab_drm_hive_temp.dm_ae_daliy as 
with a as
(
	select 
		sim_imsi,
		max(gps_lat) as max_gps_lat, 
		min(gps_lat) as min_gps_lat, 
		max(gps_lon) as max_gps_lon,	
		min(gps_lon) as min_gps_lon,
		sum(case when gps_lat is not null then 1 else 0 end) as nb_mesure_total_gps_actif,
		case when LENGTH(gps_tm)=13 then substr(from_unixtime(CAST((gps_tm/1000) AS BIGINT)),1,4) 
									  else substr(from_unixtime(CAST(gps_tm AS BIGINT)),1,4) end as year,
		case when LENGTH(gps_tm)=13 then substr(from_unixtime(CAST((gps_tm/1000) AS BIGINT)),6,2) 
									  else substr(from_unixtime(CAST(gps_tm AS BIGINT)),6,2) end as month,
		case when LENGTH(gps_tm)=13 then substr(from_unixtime(CAST((gps_tm/1000) AS BIGINT)),9,2) 
									  else substr(from_unixtime(CAST(gps_tm AS BIGINT)),9,2) end as day
	FROM z_app_bdf_hive_socle_phaeton.fai_phaeton_coverage
	where
	case when LENGTH(gps_tm)=13 then substr(from_unixtime(CAST((gps_tm/1000) AS BIGINT)),1,4) 
									  else substr(from_unixtime(CAST(gps_tm AS BIGINT)),1,4) end  = ${Y}
	and 
	case when LENGTH(gps_tm)=13 then substr(from_unixtime(CAST((gps_tm/1000) AS BIGINT)),6,2) 
									  else substr(from_unixtime(CAST(gps_tm AS BIGINT)),6,2) end = ${M}
	and 
	case when LENGTH(gps_tm)=13 then substr(from_unixtime(CAST((gps_tm/1000) AS BIGINT)),9,2) 
									  else substr(from_unixtime(CAST(gps_tm AS BIGINT)),9,2) end  = ${D}
	and gps_lat is not null and country_code = 'FR' and gps_radius < 50                              
	group by 
		sim_imsi,
		case when LENGTH(gps_tm)=13 then substr(from_unixtime(CAST((gps_tm/1000) AS BIGINT)),1,4) 
									  else substr(from_unixtime(CAST(gps_tm AS BIGINT)),1,4) end,
		case when LENGTH(gps_tm)=13 then substr(from_unixtime(CAST((gps_tm/1000) AS BIGINT)),6,2) 
									  else substr(from_unixtime(CAST(gps_tm AS BIGINT)),6,2) end,
		case when LENGTH(gps_tm)=13 then substr(from_unixtime(CAST((gps_tm/1000) AS BIGINT)),9,2) 
									  else substr(from_unixtime(CAST(gps_tm AS BIGINT)),9,2) end
)
select
	a.*,
	6371 * acos( sin(radians(min_gps_lat))* sin(radians(max_gps_lat)) + 
				 cos(radians(min_gps_lat))* cos(radians(max_gps_lat))* cos(radians(max_gps_lon)- radians(min_gps_lon))	)
	as distance_diag FROM a;
-----
drop table if exists z_lab_drm_hive_temp.dm_ae_distance_mobile_daliy ;
create table if not exists z_lab_drm_hive_temp.dm_ae_distance_mobile_daliy as
with ae as 
(
	select 
	   a.sim_imsi,a.distance_diag,a.year,a.month,a.day,
	   b.gps_lat,b.gps_lon,zip_code,city,rad_lac,
	   case when LENGTH(b.gps_tm)=13 then cast((b.gps_tm/1000) as bigint) else b.gps_tm end as gps_tm
	FROM z_lab_drm_hive_temp.dm_ae_daliy a
	LEFT JOIN z_app_bdf_hive_socle_phaeton.fai_phaeton_coverage b
	ON  b.sim_imsi = a.sim_imsi and a.year=b.year and a.month = b.month and a.day = b.day 
	WHERE b.gps_lat is not null and b.country_code = 'FR' and b.gps_radius < 50 --and a.distance_diag > 10 
	and
		if (LENGTH(b.gps_tm)=13,substr(from_unixtime(CAST((b.gps_tm/1000) AS BIGINT)),1,4),substr(from_unixtime(CAST(b.gps_tm AS BIGINT)),1,4))= ${Y}
		and
		if (LENGTH(b.gps_tm)=13,substr(from_unixtime(CAST((b.gps_tm/1000) AS BIGINT)),6,2),substr(from_unixtime(CAST(b.gps_tm AS BIGINT)),6,2)) = ${M}
		and
		if (LENGTH(b.gps_tm)=13,substr(from_unixtime(CAST((b.gps_tm/1000) AS BIGINT)),9,2),substr(from_unixtime(CAST(b.gps_tm AS BIGINT)),9,2)) = ${D}
)
select ae.*,row_number() over (partition by sim_imsi,year,month,day order by gps_tm asc) as row_num from ae
order by year,month,day,sim_imsi,gps_tm;

--Investigation--
-- SELECT COUNT(distinct sim_imsi) as nb_client,COUNT(*) as nb_mesure,year,month,day,
		-- case when distance_diag <=10 then '<=10km' else '>10km' end as flag_mobilite from z_lab_drm_hive_temp.dm_ae_distance_mobile_daliy
-- group by year,month,day,
		-- case when distance_diag <=10 then '<=10km' else '>10km' end;
----

drop table if exists z_lab_drm_hive_temp.dm_ae_daliy_route;
create table if not exists z_lab_drm_hive_temp.dm_ae_daliy_route
stored as TEXTFILE 
as select 
	sim_imsi,gps_tm,zip_code,city,rad_lac,gps_lon,gps_lat,row_num,
	CASE WHEN (gps_tm - LAG(gps_tm) over (PARTITION BY sim_imsi,year,month,day ORDER BY row_num)) <= 3600 then  ( gps_tm - LAG(gps_tm) over (PARTITION BY sim_imsi,year,month,day ORDER BY row_num)) else null end as d_tm,
	CASE WHEN (gps_tm - LAG(gps_tm) over (PARTITION BY sim_imsi,year,month,day ORDER BY row_num)) <= 3600 then 
	(
	  6371 * acos( sin(radians(gps_lat))* sin(radians(lag(gps_lat) over (PARTITION BY sim_imsi,year,month,day  ORDER BY row_num) )) + 
				   cos(radians(gps_lat))* cos(radians(lag(gps_lat) over (PARTITION BY sim_imsi,year,month,day  ORDER BY row_num) )) * cos( radians(lag(gps_lon) over (PARTITION BY sim_imsi,year,month,day ORDER BY row_num)) - radians(gps_lon)) )
	 ) else null end as distance,
	CASE WHEN (gps_tm - LAG(gps_tm) over (PARTITION BY sim_imsi,year,month,day ORDER BY row_num)) <= 3600 then 
	COALESCE(
	(6371 * acos(sin(radians(gps_lat))* sin(radians(lag(gps_lat)over (PARTITION BY sim_imsi,year,month,day  ORDER BY row_num) )) + 
				 cos(radians(gps_lat))* cos(radians(lag(gps_lat) over (PARTITION BY sim_imsi,year,month,day ORDER BY row_num)))* cos(radians(lag(gps_lon)over (PARTITION BY sim_imsi,year,month,day ORDER BY row_num))- radians(gps_lon)))/((gps_tm - lag(gps_tm,1,0) over (PARTITION BY sim_imsi,year,month,day ORDER BY row_num))/3600)),0) else null end as vitesse,
	distance_diag,year,month,day
	from z_lab_drm_hive_temp.dm_ae_distance_mobile_daliy
	where 	 
        year = ${Y}
        and 
		month = ${M}  
        and 
		day = ${D}

-- with mobile as 
-- ( select * from  z_lab_drm_hive_temp.dm_ae_daliy_route where (d_tm is not null and distance is not null and vitesse is not null) and d_tm !=0 and distance_diag >=10)

-- with immobile as 
-- ( select * from  z_lab_drm_hive_temp.dm_ae_daliy_route where (d_tm is not null and distance is not null and vitesse is not null) and d_tm !=0 and distance_diag <10)

