set hive.execution.engine=tez;
set mapreduce.job.reduces=1000;

add jar hdfs:/user/a_lab_drm_hdfs/socle/udtf/esri-geometry-api-1.2.1.jar;
add jar hdfs:/user/a_lab_drm_hdfs/socle/udtf/spatial-sdk-hive-1.2.0.jar;
add jar hdfs:/user/a_lab_drm_hdfs/socle/udtf/spatial-sdk-json-1.2.0.jar;

DROP TEMPORARY FUNCTION IF EXISTS ST_Point;
DROP TEMPORARY FUNCTION IF EXISTS ST_Distance;
DROP TEMPORARY FUNCTION IF EXISTS ST_GeomFromText;

CREATE TEMPORARY FUNCTION ST_POINT AS 'com.esri.hadoop.hive.ST_Point';
CREATE TEMPORARY FUNCTION ST_DISTANCE AS 'com.esri.hadoop.hive.ST_Distance';
CREATE TEMPORARY FUNCTION ST_GEOMFROMTEXT AS 'com.esri.hadoop.hive.ST_GeomFromText';

drop table if exists z_lab_drm_hive_temp.dm_autoroutes_jgs;
create table if not exists z_lab_drm_hive_temp.dm_autoroutes_jgs as
with mobile as 
( 
	select * from  z_lab_drm_hive_temp.dm_ae_daliy_route 
	where (d_tm is not null and distance is not null and vitesse is not null) and d_tm !=0 and distance !=0 and  distance_diag >=10 and 
	year=${Y} and month=${M} and day=${D}
)
select
	ae.sim_imsi,
	ae.gps_tm,
	ae.zip_code,
	ae.city,
    ae.rad_lac,
	ae.gps_lat,
	ae.gps_lon,
	ae.vitesse,
	ae.year,
	ae.month,
	ae.day,
	a.num_route,
	ST_DISTANCE(geo_wkt , lon_lat_point) as dist_pt_to_geo_arcep
from   
  (
   select sim_imsi,gps_tm,zip_code,city,rad_lac,gps_lat,gps_lon,vitesse,year,month,day,
		  ST_POINT(cast(gps_lon as String),cast(gps_lat as String) ) as lon_lat_point 
   from mobile
   group by sim_imsi,gps_tm,zip_code,city,rad_lac,gps_lat,gps_lon,vitesse,year,month,day
   sort by sim_imsi
   ) ae
  CROSS JOIN 
  (
   select ST_GeomFromText(wkt) as geo_wkt,num_route
   from z_lab_drm_hive_socle.ref_autoroutes
   ) a;
   
   
   