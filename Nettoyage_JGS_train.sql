set mapreduce.job.reduces=1000;
drop table if exists  z_lab_drm_hive_temp.dm_ae_train_resultat; 
create table if not exists  z_lab_drm_hive_temp.dm_ae_train_resultat as 
 with b as 
        (  select sim_imsi,gps_tm,min(dist_pt_to_geo_train) as dist_pt_to_geo_train
	       from z_lab_drm_hive_temp.dm_train_jgs
	       GROUP BY sim_imsi,gps_tm
        )
,c as 
(
        select 
                b.*,a.vitesse,a.gps_lat,a.gps_lon,a.zip_code,a.city,a.rad_lac,a.year,a.month,a.day,
				case when a.type in ('TGV_LGV','TGV_CLASSIQUE')  then 'TGV' else a.type end as type,
				a.nom_axe,a.troncon,
                row_number() over (partition by b.sim_imsi,b.gps_tm,b.dist_pt_to_geo_train order by b.gps_tm) as idx
        from b
        left join z_lab_drm_hive_temp.dm_train_jgs a
        on b.dist_pt_to_geo_train = a.dist_pt_to_geo_train and
	       a.sim_imsi = b.sim_imsi and
	       a.gps_tm = b.gps_tm
)
select *,row_number() over (partition by sim_imsi order by gps_tm asc) as row_num
from c where c.idx=1 ;
--
INSERT INTO z_lab_drm_hive_temp.dimeng_ae_train_resultat
SELECT * from  z_lab_drm_hive_temp.dm_ae_train_resultat
ORDER BY year,month,day,sim_imsi,gps_tm;