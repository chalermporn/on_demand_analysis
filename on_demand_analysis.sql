-- ## input all mesh files via POSTGIS ##
SELECT gid, key_code, mesh1_id, mesh2_id, mesh3_id, obj_id, geom
	FROM public.mesh05337;

create table mesh_merged as
select * from mesh05337
union 
select * from mesh05338
union
select * from mesh05437
union 
select * from mesh05438;

drop table mesh05337;
drop table mesh05338;
drop table mesh05437;
drop table mesh05438;

CREATE TABLE prepared_201904
(date character varying NOT NULL,  season character varying NOT NULL,  days character varying NOT NULL, hour character varying NOT NULL,  
 ori_code character varying NOT NULL,  des_code character varying NOT NULL,  num_trips integer,  OD_code character varying NOT NULL );
copy prepared_201904
from 'D:/On_demand_bus_planning/dataset/201904_prepared.csv' DELIMITERS ',' CSV; 

CREATE TABLE prepared_201906
(date character varying NOT NULL,  season character varying NOT NULL,  days character varying NOT NULL, hour character varying NOT NULL,  
 ori_code character varying NOT NULL,  des_code character varying NOT NULL,  num_trips integer,  OD_code character varying NOT NULL );
copy prepared_201906
from 'D:/On_demand_bus_planning/dataset/201906_prepared.csv' DELIMITERS ',' CSV; 

CREATE TABLE prepared_201909
(date character varying NOT NULL,  season character varying NOT NULL,  days character varying NOT NULL, hour character varying NOT NULL,  
 ori_code character varying NOT NULL,  des_code character varying NOT NULL,  num_trips integer,  OD_code character varying NOT NULL );
copy prepared_201909
from 'D:/On_demand_bus_planning/dataset/201909_prepared.csv' DELIMITERS ',' CSV; 

CREATE TABLE prepared_202003
(date character varying NOT NULL,  season character varying NOT NULL,  days character varying NOT NULL, hour character varying NOT NULL,  
 ori_code character varying NOT NULL,  des_code character varying NOT NULL,  num_trips integer,  OD_code character varying NOT NULL );
copy prepared_202003
from 'D:/On_demand_bus_planning/dataset/202003_prepared.csv' DELIMITERS ',' CSV; 

CREATE TABLE prepared_202006
(date character varying NOT NULL,  season character varying NOT NULL,  days character varying NOT NULL, hour character varying NOT NULL,  
 ori_code character varying NOT NULL,  des_code character varying NOT NULL,  num_trips integer,  OD_code character varying NOT NULL );
copy prepared_202006
from 'D:/On_demand_bus_planning/dataset/202006_prepared.csv' DELIMITERS ',' CSV; 

CREATE TABLE prepared_202009
(date character varying NOT NULL,  season character varying NOT NULL,  days character varying NOT NULL, hour character varying NOT NULL,  
 ori_code character varying NOT NULL,  des_code character varying NOT NULL,  num_trips integer,  OD_code character varying NOT NULL );
copy prepared_202009
from 'D:/On_demand_bus_planning/dataset/202009_prepared.csv' DELIMITERS ',' CSV; 

CREATE TABLE prepared_202012
(date character varying NOT NULL,  season character varying NOT NULL,  days character varying NOT NULL, hour character varying NOT NULL,  
 ori_code character varying NOT NULL,  des_code character varying NOT NULL,  num_trips integer,  OD_code character varying NOT NULL );
copy prepared_202012
from 'D:/On_demand_bus_planning/dataset/202012_prepared.csv' DELIMITERS ',' CSV; 

CREATE TABLE prepared_201912
(date character varying NOT NULL,  season character varying NOT NULL,  days character varying NOT NULL, hour character varying NOT NULL,  
 ori_code character varying NOT NULL,  des_code character varying NOT NULL,  num_trips integer,  OD_code character varying NOT NULL );
copy prepared_201912
from 'D:/On_demand_bus_planning/dataset/201912_prepared.csv' DELIMITERS ',' CSV; 


create table prepared_2019_2020 as
select * from prepared_201904
union 
select * from prepared_201906
union
select * from prepared_201909
union 
select * from prepared_201912
union
select * from prepared_202003
union 
select * from prepared_202006
union
select * from prepared_202009
union 
select * from prepared_202012;

drop table prepared_201904;
drop table prepared_201906;
drop table prepared_201909;
drop table prepared_201912;
drop table prepared_202003;
drop table prepared_202006;
drop table prepared_202009;
drop table prepared_202012;

--drop table sum_all_des

create table sum_all_ori as 
select * --distinct c.key_code, c.geom, sum (c.num_trips)
from
(select * from mesh_merged a
left join prepared_2019_2020 b on a.key_code = b.ori_code
order by num_trips) c
where c.num_trips is not null;
--group by c.key_code, c.geom;

create table sum_all_des as 
select * --distinct c.key_code, c.geom, sum (c.num_trips)
from
(select * from mesh_merged a
left join prepared_2019_2020 b on a.key_code = b.des_code
order by num_trips) c
where c.num_trips is not null
--group by c.key_code, c.geom;


select sum(num_trips) from sum_all_ori
where key_code = '53377654';

create table sum_all_ori_hour as
select distinct key_code, hour, geom, sum
	from
		(select key_code, hour, od_code, geom, --num_trips,
		sum(num_trips) over (partition by key_code, hour order by num_trips, hour 
						 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
		from sum_all_ori 
		--where key_code = '53377654'
		order by hour) a;

alter table sum_all_ori_hour add column H00 integer; update sum_all_ori_hour set H00 = sum where hour = '0';
alter table sum_all_ori_hour add column H01 integer; update sum_all_ori_hour set H01 = sum where hour = '1';
alter table sum_all_ori_hour add column H02 integer; update sum_all_ori_hour set H02 = sum where hour = '2';
alter table sum_all_ori_hour add column H03 integer; update sum_all_ori_hour set H03 = sum where hour = '3';
alter table sum_all_ori_hour add column  H04 integer; update sum_all_ori_hour set H04 = sum where hour = '4';
alter table sum_all_ori_hour add column  H05 integer; update sum_all_ori_hour set H05 = sum where hour = '5';
alter table sum_all_ori_hour add column  H06 integer; update sum_all_ori_hour set H06 = sum where hour = '6';
alter table sum_all_ori_hour add column  H07 integer; update sum_all_ori_hour set H07 = sum where hour = '7';
alter table sum_all_ori_hour add column  H08 integer; update sum_all_ori_hour set H08 = sum where hour = '8';
alter table sum_all_ori_hour add column  H09 integer; update sum_all_ori_hour set H09 = sum where hour = '9';
alter table sum_all_ori_hour add column  H10 integer; update sum_all_ori_hour set H10 = sum where hour = '10';
alter table sum_all_ori_hour add column H11 integer; update sum_all_ori_hour set H11 = sum where hour = '11';
alter table sum_all_ori_hour add column  H12 integer; update sum_all_ori_hour set H12 = sum where hour = '12';
alter table sum_all_ori_hour add column  H13 integer; update sum_all_ori_hour set H13 = sum where hour = '13';
alter table sum_all_ori_hour add column  H14 integer; update sum_all_ori_hour set H14 = sum where hour = '14';
alter table sum_all_ori_hour add column  H15 integer; update sum_all_ori_hour set H15 = sum where hour = '15';
alter table sum_all_ori_hour add column  H16 integer; update sum_all_ori_hour set H16 = sum where hour = '16';
alter table sum_all_ori_hour add column  H17 integer; update sum_all_ori_hour set H17 = sum where hour = '17';
alter table sum_all_ori_hour add column  H18 integer; update sum_all_ori_hour set H18 = sum where hour = '18';
alter table sum_all_ori_hour add column  H19 integer; update sum_all_ori_hour set H19 = sum where hour = '19';
alter table sum_all_ori_hour add column  H20 integer; update sum_all_ori_hour set H20 = sum where hour = '20';
alter table sum_all_ori_hour add column H21 integer; update sum_all_ori_hour set H21 = sum where hour = '21';
alter table sum_all_ori_hour add column H22 integer; update sum_all_ori_hour set H22 = sum where hour = '22';
alter table sum_all_ori_hour add column H23 integer; update sum_all_ori_hour set H23 = sum where hour = '23';


create table sum_all_ori_hour2 as
select * from
(select yy.*, yz.h23 from
(select yw.*, yx.h22 from
(select yu.*, yv.h21 from
(select ys.*, yt.h20 from
(select yq.*, yr.h19 from
(select yo.*, yp.h18 from
(select ym.*, yn.h17 from
(select yk.*, yl.h16 from
(select yi.*, yj.h15 from
(select ye.*, yf.h14 from
(select y.*, ya.h13 from
(select w.*, x.h12 from
(select u.*, v.h11 from
(select s.*, t.h10 from
(select q.*, r.h09 from
(select o.*, p.h08 from
(select m.*, n.h07 from
(select k.*, l.h06 from
(select i.*, j.h05 from
(select g.*, h.h04 from
(select e.*, f.h03 from
(select c.*, d.h02 from
(select a.*, b.h01 from 
	 (select bz.*, aa.h00 from			
	 	(select  distinct key_code, geom from sum_all_ori) bz 
	full join	(select key_code, geom, h00 from sum_all_ori_hour where h00 is not null) aa on bz.key_code = aa.key_code) a
	full join 	(select key_code, geom, h01 from sum_all_ori_hour where h01 is not null) b on a.key_code = b.key_code) c
	full join 	(select key_code, geom, h02 from sum_all_ori_hour where h02 is not null) d on c.key_code = d.key_code) e 
	full join 	(select key_code, geom, h03 from sum_all_ori_hour where h03 is not null) f on e.key_code = f.key_code) g 
	full join 	(select key_code, geom, h04 from sum_all_ori_hour where h04 is not null) h on g.key_code = h.key_code) i 
	full join 	(select key_code, geom, h05 from sum_all_ori_hour where h05 is not null) j on i.key_code = j.key_code) k 
 	full join 	(select key_code, geom, h06 from sum_all_ori_hour where h06 is not null) l on k.key_code = l.key_code) m
	full join 	(select key_code, geom, h07 from sum_all_ori_hour where h07 is not null) n on m.key_code = n.key_code) o 
	full join 	(select key_code, geom, h08 from sum_all_ori_hour where h08 is not null) p on o.key_code = p.key_code) q 
	full join 	(select key_code, geom, h09 from sum_all_ori_hour where h09 is not null) r on q.key_code = r.key_code) s 
	full join 	(select key_code, geom, h10 from sum_all_ori_hour where h10 is not null) t on s.key_code = t.key_code) u 
	full join 	(select key_code, geom, h11 from sum_all_ori_hour where h11 is not null) v on u.key_code = v.key_code) w 
	full join 	(select key_code, geom, h12 from sum_all_ori_hour where h12 is not null) x on w.key_code = x.key_code) y 
	full join 	(select key_code, geom, h13 from sum_all_ori_hour where h13 is not null) ya on y.key_code = ya.key_code) ye 
	full join 	(select key_code, geom, h14 from sum_all_ori_hour where h14 is not null) yf on ye.key_code = yf.key_code) yi 
	full join 	(select key_code, geom, h15 from sum_all_ori_hour where h15 is not null) yj on yi.key_code = yj.key_code) yk 
 	full join 	(select key_code, geom, h16 from sum_all_ori_hour where h16 is not null) yl on yk.key_code = yl.key_code) ym
	full join 	(select key_code, geom, h17 from sum_all_ori_hour where h17 is not null) yn on ym.key_code = yn.key_code) yo 
	full join 	(select key_code, geom, h18 from sum_all_ori_hour where h18 is not null) yp on yo.key_code = yp.key_code) yq 
	full join 	(select key_code, geom, h19 from sum_all_ori_hour where h19 is not null) yr on yq.key_code = yr.key_code) ys 
	full join 	(select key_code, geom, h20 from sum_all_ori_hour where h20 is not null) yt on ys.key_code = yt.key_code) yu 
	full join 	(select key_code, geom, h21 from sum_all_ori_hour where h21 is not null) yv on yu.key_code = yv.key_code) yw 
	full join 	(select key_code, geom, h22 from sum_all_ori_hour where h22 is not null) yx on yw.key_code = yx.key_code) yy
	full join 	(select key_code, geom, h23 from sum_all_ori_hour where h23 is not null) yz on yy.key_code = yz.key_code ) zz
where key_code is not null;

drop table sum_all_ori_hour;
create table sum_all_ori_hour as select * from sum_all_ori_hour2;
drop table sum_all_ori_hour2;

alter table sum_all_ori_hour add column sum_morning integer; update sum_all_ori_hour set sum_morning = (COALESCE(h06,0)+COALESCE(h07,0)+COALESCE(h08,0)+COALESCE(h09,0));
alter table sum_all_ori_hour add column sum_evening integer; update sum_all_ori_hour set sum_evening = (COALESCE(h16,0)+COALESCE(h17,0)+COALESCE(h18,0)+COALESCE(h19,0));
alter table sum_all_ori_hour add column sum_non_peak integer; update sum_all_ori_hour set sum_non_peak = (COALESCE(h10,0)+COALESCE(h11,0)+COALESCE(h12,0)+COALESCE(h13,0)+COALESCE(h14,0)+COALESCE(h15,0));

--- ####### ----
create table sum_all_des_hour as
select distinct key_code, hour, geom, sum
	from
		(select key_code, hour, od_code, geom, --num_trips,
		sum(num_trips) over (partition by key_code, hour order by num_trips, hour 
						 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
		from sum_all_des 
		--where key_code = '53377654'
		order by hour) a ;

alter table sum_all_des_hour add column H00 integer; update sum_all_des_hour set H00 = sum where hour = '0';
alter table sum_all_des_hour add column H01 integer; update sum_all_des_hour set H01 = sum where hour = '1';
alter table sum_all_des_hour add column H02 integer; update sum_all_des_hour set H02 = sum where hour = '2';
alter table sum_all_des_hour add column H03 integer; update sum_all_des_hour set H03 = sum where hour = '3';
alter table sum_all_des_hour add column  H04 integer; update sum_all_des_hour set H04 = sum where hour = '4';
alter table sum_all_des_hour add column  H05 integer; update sum_all_des_hour set H05 = sum where hour = '5';
alter table sum_all_des_hour add column  H06 integer; update sum_all_des_hour set H06 = sum where hour = '6';
alter table sum_all_des_hour add column  H07 integer; update sum_all_des_hour set H07 = sum where hour = '7';
alter table sum_all_des_hour add column  H08 integer; update sum_all_des_hour set H08 = sum where hour = '8';
alter table sum_all_des_hour add column  H09 integer; update sum_all_des_hour set H09 = sum where hour = '9';
alter table sum_all_des_hour add column  H10 integer; update sum_all_des_hour set H10 = sum where hour = '10';
alter table sum_all_des_hour add column H11 integer; update sum_all_des_hour set H11 = sum where hour = '11';
alter table sum_all_des_hour add column  H12 integer; update sum_all_des_hour set H12 = sum where hour = '12';
alter table sum_all_des_hour add column  H13 integer; update sum_all_des_hour set H13 = sum where hour = '13';
alter table sum_all_des_hour add column  H14 integer; update sum_all_des_hour set H14 = sum where hour = '14';
alter table sum_all_des_hour add column  H15 integer; update sum_all_des_hour set H15 = sum where hour = '15';
alter table sum_all_des_hour add column  H16 integer; update sum_all_des_hour set H16 = sum where hour = '16';
alter table sum_all_des_hour add column  H17 integer; update sum_all_des_hour set H17 = sum where hour = '17';
alter table sum_all_des_hour add column  H18 integer; update sum_all_des_hour set H18 = sum where hour = '18';
alter table sum_all_des_hour add column  H19 integer; update sum_all_des_hour set H19 = sum where hour = '19';
alter table sum_all_des_hour add column  H20 integer; update sum_all_des_hour set H20 = sum where hour = '20';
alter table sum_all_des_hour add column H21 integer; update sum_all_des_hour set H21 = sum where hour = '21';
alter table sum_all_des_hour add column H22 integer; update sum_all_des_hour set H22 = sum where hour = '22';
alter table sum_all_des_hour add column H23 integer; update sum_all_des_hour set H23 = sum where hour = '23';

--select sum (sum) from sum_all_des_hour
select * from sum_all_des_hour;

create table sum_all_des_hour2 as
select * from
(select yy.*, yz.h23 from
(select yw.*, yx.h22 from
(select yu.*, yv.h21 from
(select ys.*, yt.h20 from
(select yq.*, yr.h19 from
(select yo.*, yp.h18 from
(select ym.*, yn.h17 from
(select yk.*, yl.h16 from
(select yi.*, yj.h15 from
(select ye.*, yf.h14 from
(select y.*, ya.h13 from
(select w.*, x.h12 from
(select u.*, v.h11 from
(select s.*, t.h10 from
(select q.*, r.h09 from
(select o.*, p.h08 from
(select m.*, n.h07 from
(select k.*, l.h06 from
(select i.*, j.h05 from
(select g.*, h.h04 from
(select e.*, f.h03 from
(select c.*, d.h02 from
(select a.*, b.h01 from 
	 (select bz.*, aa.h00 from			
	 	(select  distinct key_code, geom from sum_all_des) bz 
	full join	(select key_code, geom, h00 from sum_all_des_hour where h00 is not null) aa on bz.key_code = aa.key_code) a
	full join 	(select key_code, geom, h01 from sum_all_des_hour where h01 is not null) b on a.key_code = b.key_code) c
	full join 	(select key_code, geom, h02 from sum_all_des_hour where h02 is not null) d on c.key_code = d.key_code) e 
	full join 	(select key_code, geom, h03 from sum_all_des_hour where h03 is not null) f on e.key_code = f.key_code) g 
	full join 	(select key_code, geom, h04 from sum_all_des_hour where h04 is not null) h on g.key_code = h.key_code) i 
	full join 	(select key_code, geom, h05 from sum_all_des_hour where h05 is not null) j on i.key_code = j.key_code) k 
 	full join 	(select key_code, geom, h06 from sum_all_des_hour where h06 is not null) l on k.key_code = l.key_code) m
	full join 	(select key_code, geom, h07 from sum_all_des_hour where h07 is not null) n on m.key_code = n.key_code) o 
	full join 	(select key_code, geom, h08 from sum_all_des_hour where h08 is not null) p on o.key_code = p.key_code) q 
	full join 	(select key_code, geom, h09 from sum_all_des_hour where h09 is not null) r on q.key_code = r.key_code) s 
	full join 	(select key_code, geom, h10 from sum_all_des_hour where h10 is not null) t on s.key_code = t.key_code) u 
	full join 	(select key_code, geom, h11 from sum_all_des_hour where h11 is not null) v on u.key_code = v.key_code) w 
	full join 	(select key_code, geom, h12 from sum_all_des_hour where h12 is not null) x on w.key_code = x.key_code) y 
	full join 	(select key_code, geom, h13 from sum_all_des_hour where h13 is not null) ya on y.key_code = ya.key_code) ye 
	full join 	(select key_code, geom, h14 from sum_all_des_hour where h14 is not null) yf on ye.key_code = yf.key_code) yi 
	full join 	(select key_code, geom, h15 from sum_all_des_hour where h15 is not null) yj on yi.key_code = yj.key_code) yk 
 	full join 	(select key_code, geom, h16 from sum_all_des_hour where h16 is not null) yl on yk.key_code = yl.key_code) ym
	full join 	(select key_code, geom, h17 from sum_all_des_hour where h17 is not null) yn on ym.key_code = yn.key_code) yo 
	full join 	(select key_code, geom, h18 from sum_all_des_hour where h18 is not null) yp on yo.key_code = yp.key_code) yq 
	full join 	(select key_code, geom, h19 from sum_all_des_hour where h19 is not null) yr on yq.key_code = yr.key_code) ys 
	full join 	(select key_code, geom, h20 from sum_all_des_hour where h20 is not null) yt on ys.key_code = yt.key_code) yu 
	full join 	(select key_code, geom, h21 from sum_all_des_hour where h21 is not null) yv on yu.key_code = yv.key_code) yw 
	full join 	(select key_code, geom, h22 from sum_all_des_hour where h22 is not null) yx on yw.key_code = yx.key_code) yy
	full join 	(select key_code, geom, h23 from sum_all_des_hour where h23 is not null) yz on yy.key_code = yz.key_code ) zz
where key_code is not null;

drop table sum_all_des_hour;
create table sum_all_des_hour as select * from sum_all_des_hour2;
drop table sum_all_des_hour2;

alter table sum_all_des_hour add column sum_morning integer; update sum_all_des_hour set sum_morning = (COALESCE(h06,0)+COALESCE(h07,0)+COALESCE(h08,0)+COALESCE(h09,0));
alter table sum_all_des_hour add column sum_evening integer; update sum_all_des_hour set sum_evening = (COALESCE(h16,0)+COALESCE(h17,0)+COALESCE(h18,0)+COALESCE(h19,0));
alter table sum_all_des_hour add column sum_non_peak integer; update sum_all_des_hour set sum_non_peak = (COALESCE(h10,0)+COALESCE(h11,0)+COALESCE(h12,0)+COALESCE(h13,0)+COALESCE(h14,0)+COALESCE(h15,0));

select * from sum_all_des_hour;

select * from sum_all_ori 
where ori_code = '54372777'
order by num_trips desc;


CREATE TABLE walkable_grid (key_code character varying);
copy walkable_grid
from 'D:/On_demand_bus_planning/dataset/walkable_grids.csv' DELIMITERS ',' CSV; 


create table sum_all_ori_hour_norail as
select a.* from sum_all_ori_hour a 
left join walkable_grid b on a.key_code = b.key_code
where b.key_code is null;

create table sum_all_des_hour_norail as
select a.* from sum_all_des_hour a 
left join walkable_grid b on a.key_code = b.key_code
where b.key_code is null;

-- ## selected only mesk zone where have demand for travelling
create table mesh_selected as
select distinct key_code, geom from sum_all_ori
union
select distinct key_code, geom from sum_all_des;

-- ## create centroid based on selected mesh
create table mesh_selected_cnt as
select key_code, ST_Centroid(geom)
from mesh_selected;

-- ## inport OD link ## --
-- ## Creare OD Link in QGIS and import to PostGIS
select * from od_link;
alter table od_link drop column entry_cost;
alter table od_link drop column network_co;
alter table od_link drop column exit_cost;
alter table od_link drop column total_cost;

alter table od_link add column od_code character varying;
update od_link set od_code = CONCAT(origin_id, destinatio);

-- ## Data Analysis ## --
create table all_hour as
select hour, sum (num_trips), od_code 
from prepared_2019_2020
group by hour, od_code
order by hour;


alter table all_hour add column H00 integer; update all_hour set H00 = sum where hour = '0';
alter table all_hour add column H01 integer; update all_hour set H01 = sum where hour = '1';
alter table all_hour add column H02 integer; update all_hour set H02 = sum where hour = '2';
alter table all_hour add column H03 integer; update all_hour set H03 = sum where hour = '3';
alter table all_hour add column  H04 integer; update all_hour set H04 = sum where hour = '4';
alter table all_hour add column  H05 integer; update all_hour set H05 = sum where hour = '5';
alter table all_hour add column  H06 integer; update all_hour set H06 = sum where hour = '6';
alter table all_hour add column  H07 integer; update all_hour set H07 = sum where hour = '7';
alter table all_hour add column  H08 integer; update all_hour set H08 = sum where hour = '8';
alter table all_hour add column  H09 integer; update all_hour set H09 = sum where hour = '9';
alter table all_hour add column  H10 integer; update all_hour set H10 = sum where hour = '10';
alter table all_hour add column H11 integer; update all_hour set H11 = sum where hour = '11';
alter table all_hour add column  H12 integer; update all_hour set H12 = sum where hour = '12';
alter table all_hour add column  H13 integer; update all_hour set H13 = sum where hour = '13';
alter table all_hour add column  H14 integer; update all_hour set H14 = sum where hour = '14';
alter table all_hour add column  H15 integer; update all_hour set H15 = sum where hour = '15';
alter table all_hour add column  H16 integer; update all_hour set H16 = sum where hour = '16';
alter table all_hour add column  H17 integer; update all_hour set H17 = sum where hour = '17';
alter table all_hour add column  H18 integer; update all_hour set H18 = sum where hour = '18';
alter table all_hour add column  H19 integer; update all_hour set H19 = sum where hour = '19';
alter table all_hour add column  H20 integer; update all_hour set H20 = sum where hour = '20';
alter table all_hour add column H21 integer; update all_hour set H21 = sum where hour = '21';
alter table all_hour add column H22 integer; update all_hour set H22 = sum where hour = '22';
alter table all_hour add column H23 integer; update all_hour set H23 = sum where hour = '23';

create table all_hour2 as
select * from
(select yy.*, yz.h23 from
(select yw.*, yx.h22 from
(select yu.*, yv.h21 from
(select ys.*, yt.h20 from
(select yq.*, yr.h19 from
(select yo.*, yp.h18 from
(select ym.*, yn.h17 from
(select yk.*, yl.h16 from
(select yi.*, yj.h15 from
(select ye.*, yf.h14 from
(select y.*, ya.h13 from
(select w.*, x.h12 from
(select u.*, v.h11 from
(select s.*, t.h10 from
(select q.*, r.h09 from
(select o.*, p.h08 from
(select m.*, n.h07 from
(select k.*, l.h06 from
(select i.*, j.h05 from
(select g.*, h.h04 from
(select e.*, f.h03 from
(select c.*, d.h02 from
(select a.*, b.h01 from 
	 (select bz.*, aa.h00 from			
	 	(select  distinct od_code from all_hour) bz 
	full join	(select od_code,  h00 from all_hour where h00 is not null) aa on bz.od_code = aa.od_code) a
	full join 	(select od_code,  h01 from all_hour where h01 is not null) b on a.od_code = b.od_code) c
	full join 	(select od_code,  h02 from all_hour where h02 is not null) d on c.od_code = d.od_code) e 
	full join 	(select od_code,  h03 from all_hour where h03 is not null) f on e.od_code = f.od_code) g 
	full join 	(select od_code,  h04 from all_hour where h04 is not null) h on g.od_code = h.od_code) i 
	full join 	(select od_code,  h05 from all_hour where h05 is not null) j on i.od_code = j.od_code) k 
 	full join 	(select od_code,  h06 from all_hour where h06 is not null) l on k.od_code = l.od_code) m
	full join 	(select od_code,  h07 from all_hour where h07 is not null) n on m.od_code = n.od_code) o 
	full join 	(select od_code,  h08 from all_hour where h08 is not null) p on o.od_code = p.od_code) q 
	full join 	(select od_code,  h09 from all_hour where h09 is not null) r on q.od_code = r.od_code) s 
	full join 	(select od_code,  h10 from all_hour where h10 is not null) t on s.od_code = t.od_code) u 
	full join 	(select od_code,  h11 from all_hour where h11 is not null) v on u.od_code = v.od_code) w 
	full join 	(select od_code,  h12 from all_hour where h12 is not null) x on w.od_code = x.od_code) y 
	full join 	(select od_code,  h13 from all_hour where h13 is not null) ya on y.od_code = ya.od_code) ye 
	full join 	(select od_code,  h14 from all_hour where h14 is not null) yf on ye.od_code = yf.od_code) yi 
	full join 	(select od_code,  h15 from all_hour where h15 is not null) yj on yi.od_code = yj.od_code) yk 
 	full join 	(select od_code,  h16 from all_hour where h16 is not null) yl on yk.od_code = yl.od_code) ym
	full join 	(select od_code,  h17 from all_hour where h17 is not null) yn on ym.od_code = yn.od_code) yo 
	full join 	(select od_code,  h18 from all_hour where h18 is not null) yp on yo.od_code = yp.od_code) yq 
	full join 	(select od_code,  h19 from all_hour where h19 is not null) yr on yq.od_code = yr.od_code) ys 
	full join 	(select od_code,  h20 from all_hour where h20 is not null) yt on ys.od_code = yt.od_code) yu 
	full join 	(select od_code,  h21 from all_hour where h21 is not null) yv on yu.od_code = yv.od_code) yw 
	full join 	(select od_code,  h22 from all_hour where h22 is not null) yx on yw.od_code = yx.od_code) yy
	full join 	(select od_code,  h23 from all_hour where h23 is not null) yz on yy.od_code = yz.od_code ) zz
where od_code is not null;
---Query returned successfully in 1 min 36 secs.---

drop table  all_hour;
create table all_hour as select * from all_hour2;
drop table all_hour2;

alter table all_hour add column sum_morning integer; update all_hour set sum_morning = (COALESCE(h06,0)+COALESCE(h07,0)+COALESCE(h08,0)+COALESCE(h09,0));
alter table all_hour add column sum_evening integer; update all_hour set sum_evening = (COALESCE(h16,0)+COALESCE(h17,0)+COALESCE(h18,0)+COALESCE(h19,0));
alter table all_hour add column sum_non_peak integer; update all_hour set sum_non_peak = (COALESCE(h10,0)+COALESCE(h11,0)+COALESCE(h12,0)+COALESCE(h13,0)+COALESCE(h14,0)+COALESCE(h15,0));
alter table all_hour add column sum_night integer; update all_hour set sum_night = (COALESCE(h20,0)+COALESCE(h21,0)+COALESCE(h22,0)+COALESCE(h23,0)+COALESCE(h00,0)
																					+COALESCE(h01,0)+COALESCE(h02,0)+COALESCE(h03,0)+COALESCE(h04,0)+COALESCE(h05,0));


--### 30 April 2021 ###---
--### consider only trips between mesh zones ###

create table od_link_lines as 
select distinct * from
(select (st_distance (geom_ori, geom_des)*100) as distance, st_makeline (geom_ori, geom_des) as geom_line, g.od_code  from
	(select e.*, f.st_centroid as geom_des from 
		(select c.*, d.st_centroid as geom_ori from 
			(select b.ori_code, b.des_code, a.*
			from all_hour a
			inner join prepared_2019_2020 b on a.od_code = b.od_code) c
		inner join mesh_selected_cnt d on c.ori_code = d.key_code) e
	inner join mesh_selected_cnt f on e.des_code = f.key_code) g
	) h
;

create table od_link_volume as
select a.*, b.geom_line from prepared_2019_2020 a 
left join od_link_lines b on a.od_code = b.od_code;


--### consider only trips between mesh zones ###
create table od_link_volumn_btwzone as
select a.date, a.season, a.days, a.hour, a.num_trips, b.* 
from prepared_2019_2020 a
full join od_link_lines b on a.od_code = b.od_code
where ori_code != des_code and num_trips > 0;

--drop table od_link_volumn_btwzone_hour
create table od_link_volumn_btwzone_hour as
select hour, sum (num_trips), geom_line as geom, od_code 
from od_link_volumn_btwzone
group by hour, geom, od_code
order by hour;


alter table od_link_volumn_btwzone_hour add column H00 integer; update od_link_volumn_btwzone_hour set H00 = sum where hour = '0';
alter table od_link_volumn_btwzone_hour add column H01 integer; update od_link_volumn_btwzone_hour set H01 = sum where hour = '1';
alter table od_link_volumn_btwzone_hour add column H02 integer; update od_link_volumn_btwzone_hour set H02 = sum where hour = '2';
alter table od_link_volumn_btwzone_hour add column H03 integer; update od_link_volumn_btwzone_hour set H03 = sum where hour = '3';
alter table od_link_volumn_btwzone_hour add column  H04 integer; update od_link_volumn_btwzone_hour set H04 = sum where hour = '4';
alter table od_link_volumn_btwzone_hour add column  H05 integer; update od_link_volumn_btwzone_hour set H05 = sum where hour = '5';
alter table od_link_volumn_btwzone_hour add column  H06 integer; update od_link_volumn_btwzone_hour set H06 = sum where hour = '6';
alter table od_link_volumn_btwzone_hour add column  H07 integer; update od_link_volumn_btwzone_hour set H07 = sum where hour = '7';
alter table od_link_volumn_btwzone_hour add column  H08 integer; update od_link_volumn_btwzone_hour set H08 = sum where hour = '8';
alter table od_link_volumn_btwzone_hour add column  H09 integer; update od_link_volumn_btwzone_hour set H09 = sum where hour = '9';
alter table od_link_volumn_btwzone_hour add column  H10 integer; update od_link_volumn_btwzone_hour set H10 = sum where hour = '10';
alter table od_link_volumn_btwzone_hour add column H11 integer; update od_link_volumn_btwzone_hour set H11 = sum where hour = '11';
alter table od_link_volumn_btwzone_hour add column  H12 integer; update od_link_volumn_btwzone_hour set H12 = sum where hour = '12';
alter table od_link_volumn_btwzone_hour add column  H13 integer; update od_link_volumn_btwzone_hour set H13 = sum where hour = '13';
alter table od_link_volumn_btwzone_hour add column  H14 integer; update od_link_volumn_btwzone_hour set H14 = sum where hour = '14';
alter table od_link_volumn_btwzone_hour add column  H15 integer; update od_link_volumn_btwzone_hour set H15 = sum where hour = '15';
alter table od_link_volumn_btwzone_hour add column  H16 integer; update od_link_volumn_btwzone_hour set H16 = sum where hour = '16';
alter table od_link_volumn_btwzone_hour add column  H17 integer; update od_link_volumn_btwzone_hour set H17 = sum where hour = '17';
alter table od_link_volumn_btwzone_hour add column  H18 integer; update od_link_volumn_btwzone_hour set H18 = sum where hour = '18';
alter table od_link_volumn_btwzone_hour add column  H19 integer; update od_link_volumn_btwzone_hour set H19 = sum where hour = '19';
alter table od_link_volumn_btwzone_hour add column  H20 integer; update od_link_volumn_btwzone_hour set H20 = sum where hour = '20';
alter table od_link_volumn_btwzone_hour add column H21 integer; update od_link_volumn_btwzone_hour set H21 = sum where hour = '21';
alter table od_link_volumn_btwzone_hour add column H22 integer; update od_link_volumn_btwzone_hour set H22 = sum where hour = '22';
alter table od_link_volumn_btwzone_hour add column H23 integer; update od_link_volumn_btwzone_hour set H23 = sum where hour = '23';


select * from od_link_volumn_btwzone_hour;

create table od_link_volumn_btwzone_hour2 as
select * from
(select yy.*, yz.h23 from
(select yw.*, yx.h22 from
(select yu.*, yv.h21 from
(select ys.*, yt.h20 from
(select yq.*, yr.h19 from
(select yo.*, yp.h18 from
(select ym.*, yn.h17 from
(select yk.*, yl.h16 from
(select yi.*, yj.h15 from
(select ye.*, yf.h14 from
(select y.*, ya.h13 from
(select w.*, x.h12 from
(select u.*, v.h11 from
(select s.*, t.h10 from
(select q.*, r.h09 from
(select o.*, p.h08 from
(select m.*, n.h07 from
(select k.*, l.h06 from
(select i.*, j.h05 from
(select g.*, h.h04 from
(select e.*, f.h03 from
(select c.*, d.h02 from
(select a.*, b.h01 from 
	 (select bz.*, aa.h00 from			
	 	(select  distinct od_code, geom from od_link_volumn_btwzone_hour) bz 
	full join	(select od_code, geom, h00 from od_link_volumn_btwzone_hour where h00 is not null) aa on bz.od_code = aa.od_code) a
	full join 	(select od_code, geom, h01 from od_link_volumn_btwzone_hour where h01 is not null) b on a.od_code = b.od_code) c
	full join 	(select od_code, geom, h02 from od_link_volumn_btwzone_hour where h02 is not null) d on c.od_code = d.od_code) e 
	full join 	(select od_code, geom, h03 from od_link_volumn_btwzone_hour where h03 is not null) f on e.od_code = f.od_code) g 
	full join 	(select od_code, geom, h04 from od_link_volumn_btwzone_hour where h04 is not null) h on g.od_code = h.od_code) i 
	full join 	(select od_code, geom, h05 from od_link_volumn_btwzone_hour where h05 is not null) j on i.od_code = j.od_code) k 
 	full join 	(select od_code, geom, h06 from od_link_volumn_btwzone_hour where h06 is not null) l on k.od_code = l.od_code) m
	full join 	(select od_code, geom, h07 from od_link_volumn_btwzone_hour where h07 is not null) n on m.od_code = n.od_code) o 
	full join 	(select od_code, geom, h08 from od_link_volumn_btwzone_hour where h08 is not null) p on o.od_code = p.od_code) q 
	full join 	(select od_code, geom, h09 from od_link_volumn_btwzone_hour where h09 is not null) r on q.od_code = r.od_code) s 
	full join 	(select od_code, geom, h10 from od_link_volumn_btwzone_hour where h10 is not null) t on s.od_code = t.od_code) u 
	full join 	(select od_code, geom, h11 from od_link_volumn_btwzone_hour where h11 is not null) v on u.od_code = v.od_code) w 
	full join 	(select od_code, geom, h12 from od_link_volumn_btwzone_hour where h12 is not null) x on w.od_code = x.od_code) y 
	full join 	(select od_code, geom, h13 from od_link_volumn_btwzone_hour where h13 is not null) ya on y.od_code = ya.od_code) ye 
	full join 	(select od_code, geom, h14 from od_link_volumn_btwzone_hour where h14 is not null) yf on ye.od_code = yf.od_code) yi 
	full join 	(select od_code, geom, h15 from od_link_volumn_btwzone_hour where h15 is not null) yj on yi.od_code = yj.od_code) yk 
 	full join 	(select od_code, geom, h16 from od_link_volumn_btwzone_hour where h16 is not null) yl on yk.od_code = yl.od_code) ym
	full join 	(select od_code, geom, h17 from od_link_volumn_btwzone_hour where h17 is not null) yn on ym.od_code = yn.od_code) yo 
	full join 	(select od_code, geom, h18 from od_link_volumn_btwzone_hour where h18 is not null) yp on yo.od_code = yp.od_code) yq 
	full join 	(select od_code, geom, h19 from od_link_volumn_btwzone_hour where h19 is not null) yr on yq.od_code = yr.od_code) ys 
	full join 	(select od_code, geom, h20 from od_link_volumn_btwzone_hour where h20 is not null) yt on ys.od_code = yt.od_code) yu 
	full join 	(select od_code, geom, h21 from od_link_volumn_btwzone_hour where h21 is not null) yv on yu.od_code = yv.od_code) yw 
	full join 	(select od_code, geom, h22 from od_link_volumn_btwzone_hour where h22 is not null) yx on yw.od_code = yx.od_code) yy
	full join 	(select od_code, geom, h23 from od_link_volumn_btwzone_hour where h23 is not null) yz on yy.od_code = yz.od_code ) zz
where od_code is not null;

drop table od_link_volumn_btwzone_hour;
create table od_link_volumn_btwzone_hour as select * from od_link_volumn_btwzone_hour2;
drop table od_link_volumn_btwzone_hour2;

alter table od_link_volumn_btwzone_hour add column sum_morning integer; update od_link_volumn_btwzone_hour set sum_morning = (COALESCE(h06,0)+COALESCE(h07,0)+COALESCE(h08,0)+COALESCE(h09,0));
alter table od_link_volumn_btwzone_hour add column sum_evening integer; update od_link_volumn_btwzone_hour set sum_evening = (COALESCE(h16,0)+COALESCE(h17,0)+COALESCE(h18,0)+COALESCE(h19,0));
alter table od_link_volumn_btwzone_hour add column sum_non_peak integer; update od_link_volumn_btwzone_hour set sum_non_peak = (COALESCE(h10,0)+COALESCE(h11,0)+COALESCE(h12,0)+COALESCE(h13,0)+COALESCE(h14,0)+COALESCE(h15,0));
alter table od_link_volumn_btwzone_hour add column sum_night integer; update od_link_volumn_btwzone_hour set sum_night = (COALESCE(h20,0)+COALESCE(h21,0)+COALESCE(h22,0)+COALESCE(h23,0)
																		+COALESCE(h00,0)+COALESCE(h01,0)+COALESCE(h02,0)+COALESCE(h03,0)+COALESCE(h04,0)+COALESCE(h05,0));
alter table od_link_volumn_btwzone_hour add column sum_allday integer; 
update od_link_volumn_btwzone_hour set sum_allday = (COALESCE(sum_morning,0)+COALESCE(sum_evening,0)+COALESCE(sum_non_peak,0)+COALESCE(sum_night,0));


---
create table od_link_volumn_btwzone_hour_dis as
select a.*, b.distance from od_link_volumn_btwzone_hour a
inner join od_link_lines b on a.od_code = b.od_code;

select * from od_link_lines;
select * from od_link_volume; --ori_code, des_code, od_code

create table od_link_volumn_btwzone_hour_dis2 as
select distinct * from 
	(select b.ori_code, b.des_code, a.* from od_link_volumn_btwzone_hour_dis a
	inner join od_link_volume b on a.od_code = b.od_code) C
	 ;

drop table od_link_volumn_btwzone_hour_dis;

create table od_link_volumn_btwzone_hour_dis as
select * from od_link_volumn_btwzone_hour_dis2;

drop table od_link_volumn_btwzone_hour_dis2;


-- ## Focus remote area --
create table Remote_area_morning as
select * from 
(select * from od_link_volumn_btwzone_hour_dis
where od_code like '54372590%' 
	or od_code like '54372591%'
	or od_code like '54371592%'
	or od_code like '54371580%'
	or od_code like '54371479%'
	or od_code like '54371449%'
	or od_code like '54371439%'
	or od_code like '54374729%'
	or od_code like '54383090%'
	or od_code like '54373756%'
	or od_code like '54382078%'
	or od_code like '54382026%'
	or od_code like '54380092%'
	or od_code like '54370608%') a
where ori_code != des_code
and sum_morning > 1
--and od_code != '5437259054372590' 
--and od_code != '5437259054372590'
--and distance > '10'
--order by od_code desc
--limit 5
;


create table Remote_area_evening as
select * from 
(select * from od_link_volumn_btwzone_hour_dis
where od_code like '%54374502' 
	or od_code like '%54374508'
	or od_code like '%54373555'
	or od_code like '%54373515'
	or od_code like '%54373503'
	or od_code like '%54372591'
	or od_code like '%54373501'
	or od_code like '%54372590'
	or od_code like '%54372489'
	or od_code like '%54372479'
	or od_code like '%54371592'
	or od_code like '%54371580'
	or od_code like '%54371479'
	or od_code like '%54371449'
	or od_code like '%54371444'
	or od_code like '%54371434'
	or od_code like '%54371540'
	or od_code like '%54371439'
	or od_code like '%54370556'
	or od_code like '%54370608'
	or od_code like '%54370782'
	or od_code like '%54373756'
	or od_code like '%54373777'
	or od_code like '%54374719'
	or od_code like '%54374729'
	or od_code like '%54383090'
	or od_code like '%54382077'
	or od_code like '%54382078'
	or od_code like '%54382026'
	or od_code like '%54380091'
	or od_code like '%54380092'
	or od_code like '%54380070'
	or od_code like '%54380080') a
where ori_code != des_code
and sum_evening > 1
--and od_code != '5437259054372590'
--and distance > '10'
--order by sum_allday desc
--limit 5
;

--- ## Remote Area ---
select distinct ori_code --*
from Remote_area_morning; --27 rows

select * --*
from Remote_area_evening --54 rows
where ori_code = '54372590';

select * 
from od_link_volume
where od_code like '54382026%';

create table Remote_area_morning_no_transport as
select * from 
(select * from od_link_volumn_btwzone_hour_dis
where od_code like '54374502%' 
	or od_code like '54374508%'
	or od_code like '54373555%'
	or od_code like '54373515%'
	or od_code like '54373503%'
	or od_code like '54372591%'
	or od_code like '54373501%'
	or od_code like '54372489%'
	or od_code like '54371680%'
	or od_code like '54371580%'
	or od_code like '54371479%'
	or od_code like '54371540%'
	or od_code like '54371439%'
	or od_code like '54371449%'
	or od_code like '54371444%'
	or od_code like '54371434%') a
where ori_code != des_code
and sum_morning > 1
--and od_code != '5437259054372590' 
--and od_code != '5437259054372590'
--and distance > '10'
--order by od_code desc
--limit 5
;

create table Remote_area_evening_no_transport as
select * from 
(select * from od_link_volumn_btwzone_hour_dis
where od_code like '%54371580' 
	or od_code like '%54371479'
	or od_code like '%54371449'
	or od_code like '%54381439') a
where ori_code != des_code
and sum_evening > 1
--and od_code != '5437259054372590'
--and distance > '10'
--order by sum_allday desc
--limit 5
;

create table Remote_area_morning_no_transport_54380091 as
select * from 
(select * from od_link_volumn_btwzone_hour_dis
where od_code like '54370767%' 
	or od_code like '54380091%') a
where ori_code != des_code
and sum_morning > 1
--and od_code != '5437259054372590' 
--and od_code != '5437259054372590'
--and distance > '10'
--order by od_code desc
--limit 5
;

create table Remote_area_morning_no_transport_Nmsmt as
select * from 
(select * from od_link_volumn_btwzone_hour_dis
where od_code like '%54374729'
 	or od_code like '%54383090'
	or od_code like '%54373777'
	or od_code like '54374729%'
 	or od_code like '54383090%'
	or od_code like '54373777%') a
where ori_code != des_code
and sum_morning > 1
--and od_code != '5437259054372590' 
--and od_code != '5437259054372590'
--and distance > '10'
--order by od_code desc
--limit 5
;


--### join to see season

create table sum_all_ori_hour_season as
	select distinct key_code, season, hour, geom, sum
		from
			(select key_code, season, hour, od_code, geom, --num_trips,
			sum(num_trips) over (partition by key_code, season, hour order by num_trips, season, hour 
							 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			from sum_all_ori 
			--where key_code = '53377654'
			order by hour) a
			;
	-- ) b

alter table sum_all_ori_hour_season add column H00 integer; update sum_all_ori_hour_season set H00 = sum where hour = '0';
alter table sum_all_ori_hour_season add column H01 integer; update sum_all_ori_hour_season set H01 = sum where hour = '1';
alter table sum_all_ori_hour_season add column H02 integer; update sum_all_ori_hour_season set H02 = sum where hour = '2';
alter table sum_all_ori_hour_season add column H03 integer; update sum_all_ori_hour_season set H03 = sum where hour = '3';
alter table sum_all_ori_hour_season add column  H04 integer; update sum_all_ori_hour_season set H04 = sum where hour = '4';
alter table sum_all_ori_hour_season add column  H05 integer; update sum_all_ori_hour_season set H05 = sum where hour = '5';
alter table sum_all_ori_hour_season add column  H06 integer; update sum_all_ori_hour_season set H06 = sum where hour = '6';
alter table sum_all_ori_hour_season add column  H07 integer; update sum_all_ori_hour_season set H07 = sum where hour = '7';
alter table sum_all_ori_hour_season add column  H08 integer; update sum_all_ori_hour_season set H08 = sum where hour = '8';
alter table sum_all_ori_hour_season add column  H09 integer; update sum_all_ori_hour_season set H09 = sum where hour = '9';
alter table sum_all_ori_hour_season add column  H10 integer; update sum_all_ori_hour_season set H10 = sum where hour = '10';
alter table sum_all_ori_hour_season add column H11 integer; update sum_all_ori_hour_season set H11 = sum where hour = '11';
alter table sum_all_ori_hour_season add column  H12 integer; update sum_all_ori_hour_season set H12 = sum where hour = '12';
alter table sum_all_ori_hour_season add column  H13 integer; update sum_all_ori_hour_season set H13 = sum where hour = '13';
alter table sum_all_ori_hour_season add column  H14 integer; update sum_all_ori_hour_season set H14 = sum where hour = '14';
alter table sum_all_ori_hour_season add column  H15 integer; update sum_all_ori_hour_season set H15 = sum where hour = '15';
alter table sum_all_ori_hour_season add column  H16 integer; update sum_all_ori_hour_season set H16 = sum where hour = '16';
alter table sum_all_ori_hour_season add column  H17 integer; update sum_all_ori_hour_season set H17 = sum where hour = '17';
alter table sum_all_ori_hour_season add column  H18 integer; update sum_all_ori_hour_season set H18 = sum where hour = '18';
alter table sum_all_ori_hour_season add column  H19 integer; update sum_all_ori_hour_season set H19 = sum where hour = '19';
alter table sum_all_ori_hour_season add column  H20 integer; update sum_all_ori_hour_season set H20 = sum where hour = '20';
alter table sum_all_ori_hour_season add column H21 integer; update sum_all_ori_hour_season set H21 = sum where hour = '21';
alter table sum_all_ori_hour_season add column H22 integer; update sum_all_ori_hour_season set H22 = sum where hour = '22';
alter table sum_all_ori_hour_season add column H23 integer; update sum_all_ori_hour_season set H23 = sum where hour = '23';


create table sum_all_ori_hour_season2 as
select * from
(select yy.*, yz.h23 from
(select yw.*, yx.h22 from
(select yu.*, yv.h21 from
(select ys.*, yt.h20 from
(select yq.*, yr.h19 from
(select yo.*, yp.h18 from
(select ym.*, yn.h17 from
(select yk.*, yl.h16 from
(select yi.*, yj.h15 from
(select ye.*, yf.h14 from
(select y.*, ya.h13 from
(select w.*, x.h12 from
(select u.*, v.h11 from
(select s.*, t.h10 from
(select q.*, r.h09 from
(select o.*, p.h08 from
(select m.*, n.h07 from
(select k.*, l.h06 from
(select i.*, j.h05 from
(select g.*, h.h04 from
(select e.*, f.h03 from
(select c.*, d.h02 from
(select a.*, b.h01 from 
	 (select bz.*, aa.h00 from			
	 	(select  distinct key_code, geom from sum_all_ori) bz 
	full join	(select key_code, geom, h00 from sum_all_ori_hour_season where h00 is not null) aa on bz.key_code = aa.key_code) a
	full join 	(select key_code, geom, h01 from sum_all_ori_hour_season where h01 is not null) b on a.key_code = b.key_code) c
	full join 	(select key_code, geom, h02 from sum_all_ori_hour_season where h02 is not null) d on c.key_code = d.key_code) e 
	full join 	(select key_code, geom, h03 from sum_all_ori_hour_season where h03 is not null) f on e.key_code = f.key_code) g 
	full join 	(select key_code, geom, h04 from sum_all_ori_hour_season where h04 is not null) h on g.key_code = h.key_code) i 
	full join 	(select key_code, geom, h05 from sum_all_ori_hour_season where h05 is not null) j on i.key_code = j.key_code) k 
 	full join 	(select key_code, geom, h06 from sum_all_ori_hour_season where h06 is not null) l on k.key_code = l.key_code) m
	full join 	(select key_code, geom, h07 from sum_all_ori_hour_season where h07 is not null) n on m.key_code = n.key_code) o 
	full join 	(select key_code, geom, h08 from sum_all_ori_hour_season where h08 is not null) p on o.key_code = p.key_code) q 
	full join 	(select key_code, geom, h09 from sum_all_ori_hour_season where h09 is not null) r on q.key_code = r.key_code) s 
	full join 	(select key_code, geom, h10 from sum_all_ori_hour_season where h10 is not null) t on s.key_code = t.key_code) u 
	full join 	(select key_code, geom, h11 from sum_all_ori_hour_season where h11 is not null) v on u.key_code = v.key_code) w 
	full join 	(select key_code, geom, h12 from sum_all_ori_hour_season where h12 is not null) x on w.key_code = x.key_code) y 
	full join 	(select key_code, geom, h13 from sum_all_ori_hour_season where h13 is not null) ya on y.key_code = ya.key_code) ye 
	full join 	(select key_code, geom, h14 from sum_all_ori_hour_season where h14 is not null) yf on ye.key_code = yf.key_code) yi 
	full join 	(select key_code, geom, h15 from sum_all_ori_hour_season where h15 is not null) yj on yi.key_code = yj.key_code) yk 
 	full join 	(select key_code, geom, h16 from sum_all_ori_hour_season where h16 is not null) yl on yk.key_code = yl.key_code) ym
	full join 	(select key_code, geom, h17 from sum_all_ori_hour_season where h17 is not null) yn on ym.key_code = yn.key_code) yo 
	full join 	(select key_code, geom, h18 from sum_all_ori_hour_season where h18 is not null) yp on yo.key_code = yp.key_code) yq 
	full join 	(select key_code, geom, h19 from sum_all_ori_hour_season where h19 is not null) yr on yq.key_code = yr.key_code) ys 
	full join 	(select key_code, geom, h20 from sum_all_ori_hour_season where h20 is not null) yt on ys.key_code = yt.key_code) yu 
	full join 	(select key_code, geom, h21 from sum_all_ori_hour_season where h21 is not null) yv on yu.key_code = yv.key_code) yw 
	full join 	(select key_code, geom, h22 from sum_all_ori_hour_season where h22 is not null) yx on yw.key_code = yx.key_code) yy
	full join 	(select key_code, geom, h23 from sum_all_ori_hour_season where h23 is not null) yz on yy.key_code = yz.key_code ) zz
where key_code is not null;

drop table sum_all_ori_hour_season;
create table sum_all_ori_hour_season as select * from sum_all_ori_hour_season2;
drop table sum_all_ori_hour_season2;


create table sum_all_des_hour_season as
	select distinct key_code, season, hour, geom, sum
		from
			(select key_code, season, hour, od_code, geom, --num_trips,
			sum(num_trips) over (partition by key_code, season, hour order by num_trips, season, hour 
							 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			from sum_all_des 
			--where key_code = '53377654'
			order by hour) a
			;
	-- ) b

alter table sum_all_des_hour_season add column H00 integer; update sum_all_des_hour_season set H00 = sum where hour = '0';
alter table sum_all_des_hour_season add column H01 integer; update sum_all_des_hour_season set H01 = sum where hour = '1';
alter table sum_all_des_hour_season add column H02 integer; update sum_all_des_hour_season set H02 = sum where hour = '2';
alter table sum_all_des_hour_season add column H03 integer; update sum_all_des_hour_season set H03 = sum where hour = '3';
alter table sum_all_des_hour_season add column  H04 integer; update sum_all_des_hour_season set H04 = sum where hour = '4';
alter table sum_all_des_hour_season add column  H05 integer; update sum_all_des_hour_season set H05 = sum where hour = '5';
alter table sum_all_des_hour_season add column  H06 integer; update sum_all_des_hour_season set H06 = sum where hour = '6';
alter table sum_all_des_hour_season add column  H07 integer; update sum_all_des_hour_season set H07 = sum where hour = '7';
alter table sum_all_des_hour_season add column  H08 integer; update sum_all_des_hour_season set H08 = sum where hour = '8';
alter table sum_all_des_hour_season add column  H09 integer; update sum_all_des_hour_season set H09 = sum where hour = '9';
alter table sum_all_des_hour_season add column  H10 integer; update sum_all_des_hour_season set H10 = sum where hour = '10';
alter table sum_all_des_hour_season add column H11 integer; update sum_all_des_hour_season set H11 = sum where hour = '11';
alter table sum_all_des_hour_season add column  H12 integer; update sum_all_des_hour_season set H12 = sum where hour = '12';
alter table sum_all_des_hour_season add column  H13 integer; update sum_all_des_hour_season set H13 = sum where hour = '13';
alter table sum_all_des_hour_season add column  H14 integer; update sum_all_des_hour_season set H14 = sum where hour = '14';
alter table sum_all_des_hour_season add column  H15 integer; update sum_all_des_hour_season set H15 = sum where hour = '15';
alter table sum_all_des_hour_season add column  H16 integer; update sum_all_des_hour_season set H16 = sum where hour = '16';
alter table sum_all_des_hour_season add column  H17 integer; update sum_all_des_hour_season set H17 = sum where hour = '17';
alter table sum_all_des_hour_season add column  H18 integer; update sum_all_des_hour_season set H18 = sum where hour = '18';
alter table sum_all_des_hour_season add column  H19 integer; update sum_all_des_hour_season set H19 = sum where hour = '19';
alter table sum_all_des_hour_season add column  H20 integer; update sum_all_des_hour_season set H20 = sum where hour = '20';
alter table sum_all_des_hour_season add column H21 integer; update sum_all_des_hour_season set H21 = sum where hour = '21';
alter table sum_all_des_hour_season add column H22 integer; update sum_all_des_hour_season set H22 = sum where hour = '22';
alter table sum_all_des_hour_season add column H23 integer; update sum_all_des_hour_season set H23 = sum where hour = '23';


create table sum_all_des_hour_season2 as
select * from
(select yy.*, yz.h23 from
(select yw.*, yx.h22 from
(select yu.*, yv.h21 from
(select ys.*, yt.h20 from
(select yq.*, yr.h19 from
(select yo.*, yp.h18 from
(select ym.*, yn.h17 from
(select yk.*, yl.h16 from
(select yi.*, yj.h15 from
(select ye.*, yf.h14 from
(select y.*, ya.h13 from
(select w.*, x.h12 from
(select u.*, v.h11 from
(select s.*, t.h10 from
(select q.*, r.h09 from
(select o.*, p.h08 from
(select m.*, n.h07 from
(select k.*, l.h06 from
(select i.*, j.h05 from
(select g.*, h.h04 from
(select e.*, f.h03 from
(select c.*, d.h02 from
(select a.*, b.h01 from 
	 (select bz.*, aa.h00 from			
	 	(select  distinct key_code, geom from sum_all_des) bz 
	full join	(select key_code, geom, h00 from sum_all_des_hour_season where h00 is not null) aa on bz.key_code = aa.key_code) a
	full join 	(select key_code, geom, h01 from sum_all_des_hour_season where h01 is not null) b on a.key_code = b.key_code) c
	full join 	(select key_code, geom, h02 from sum_all_des_hour_season where h02 is not null) d on c.key_code = d.key_code) e 
	full join 	(select key_code, geom, h03 from sum_all_des_hour_season where h03 is not null) f on e.key_code = f.key_code) g 
	full join 	(select key_code, geom, h04 from sum_all_des_hour_season where h04 is not null) h on g.key_code = h.key_code) i 
	full join 	(select key_code, geom, h05 from sum_all_des_hour_season where h05 is not null) j on i.key_code = j.key_code) k 
 	full join 	(select key_code, geom, h06 from sum_all_des_hour_season where h06 is not null) l on k.key_code = l.key_code) m
	full join 	(select key_code, geom, h07 from sum_all_des_hour_season where h07 is not null) n on m.key_code = n.key_code) o 
	full join 	(select key_code, geom, h08 from sum_all_des_hour_season where h08 is not null) p on o.key_code = p.key_code) q 
	full join 	(select key_code, geom, h09 from sum_all_des_hour_season where h09 is not null) r on q.key_code = r.key_code) s 
	full join 	(select key_code, geom, h10 from sum_all_des_hour_season where h10 is not null) t on s.key_code = t.key_code) u 
	full join 	(select key_code, geom, h11 from sum_all_des_hour_season where h11 is not null) v on u.key_code = v.key_code) w 
	full join 	(select key_code, geom, h12 from sum_all_des_hour_season where h12 is not null) x on w.key_code = x.key_code) y 
	full join 	(select key_code, geom, h13 from sum_all_des_hour_season where h13 is not null) ya on y.key_code = ya.key_code) ye 
	full join 	(select key_code, geom, h14 from sum_all_des_hour_season where h14 is not null) yf on ye.key_code = yf.key_code) yi 
	full join 	(select key_code, geom, h15 from sum_all_des_hour_season where h15 is not null) yj on yi.key_code = yj.key_code) yk 
 	full join 	(select key_code, geom, h16 from sum_all_des_hour_season where h16 is not null) yl on yk.key_code = yl.key_code) ym
	full join 	(select key_code, geom, h17 from sum_all_des_hour_season where h17 is not null) yn on ym.key_code = yn.key_code) yo 
	full join 	(select key_code, geom, h18 from sum_all_des_hour_season where h18 is not null) yp on yo.key_code = yp.key_code) yq 
	full join 	(select key_code, geom, h19 from sum_all_des_hour_season where h19 is not null) yr on yq.key_code = yr.key_code) ys 
	full join 	(select key_code, geom, h20 from sum_all_des_hour_season where h20 is not null) yt on ys.key_code = yt.key_code) yu 
	full join 	(select key_code, geom, h21 from sum_all_des_hour_season where h21 is not null) yv on yu.key_code = yv.key_code) yw 
	full join 	(select key_code, geom, h22 from sum_all_des_hour_season where h22 is not null) yx on yw.key_code = yx.key_code) yy
	full join 	(select key_code, geom, h23 from sum_all_des_hour_season where h23 is not null) yz on yy.key_code = yz.key_code ) zz
where key_code is not null;

drop table sum_all_des_hour_season;
create table sum_all_des_hour_season as select * from sum_all_des_hour_season2;
drop table sum_all_des_hour_season2;


create table sum_all_ori_hour_season_notransport as
	select b.* from sum_all_ori_hour_no_transport a
	left join sum_all_ori_hour_season b on a.key_code = b.key_code;

create table sum_all_des_hour_season_notransport as
	select b.* from sum_all_des_hour_no_transport a
	left join sum_all_des_hour_season b on a.key_code = b.key_code;


alter table sum_all_ori_hour_season_notransport add column sum_morning integer; 
update sum_all_ori_hour_season_notransport set sum_morning = (COALESCE(h06,0)+COALESCE(h07,0)+COALESCE(h08,0)+COALESCE(h09,0));

alter table sum_all_ori_hour_season_notransport add column sum_evening integer; 
update sum_all_ori_hour_season_notransport set sum_evening = (COALESCE(h16,0)+COALESCE(h17,0)+COALESCE(h18,0)+COALESCE(h19,0));

alter table sum_all_ori_hour_season_notransport add column sum_non_peak integer; 
update sum_all_ori_hour_season_notransport set sum_non_peak = (COALESCE(h10,0)+COALESCE(h11,0)+COALESCE(h12,0)+COALESCE(h13,0)+COALESCE(h14,0)+COALESCE(h15,0));

alter table sum_all_ori_hour_season_notransport add column sum_night integer; 
update sum_all_ori_hour_season_notransport set sum_night = (COALESCE(h20,0)+COALESCE(h21,0)+COALESCE(h22,0)+COALESCE(h23,0)
													+COALESCE(h00,0)+COALESCE(h01,0)+COALESCE(h02,0)+COALESCE(h03,0)+COALESCE(h04,0)+COALESCE(h05,0));


alter table sum_all_des_hour_season_notransport add column sum_morning integer; 
update sum_all_des_hour_season_notransport set sum_morning = (COALESCE(h06,0)+COALESCE(h07,0)+COALESCE(h08,0)+COALESCE(h09,0));

alter table sum_all_des_hour_season_notransport add column sum_evening integer; 
update sum_all_des_hour_season_notransport set sum_evening = (COALESCE(h16,0)+COALESCE(h17,0)+COALESCE(h18,0)+COALESCE(h19,0));

alter table sum_all_des_hour_season_notransport add column sum_non_peak integer; 
update sum_all_des_hour_season_notransport set sum_non_peak = (COALESCE(h10,0)+COALESCE(h11,0)+COALESCE(h12,0)+COALESCE(h13,0)+COALESCE(h14,0)+COALESCE(h15,0));

alter table sum_all_des_hour_season_notransport add column sum_night integer; 
update sum_all_des_hour_season_notransport set sum_night = (COALESCE(h20,0)+COALESCE(h21,0)+COALESCE(h22,0)+COALESCE(h23,0)
													+COALESCE(h00,0)+COALESCE(h01,0)+COALESCE(h02,0)+COALESCE(h03,0)+COALESCE(h04,0)+COALESCE(h05,0));


create table sum_ori_hour_notrsansport_spring as
select key_code, season, geom, sum(sum), sum(sum_morning) as sum_morning, sum(sum_evening) as sum_evening, sum(sum_non_peak) as sum_non_peak, sum(sum_night) as sum_night,
sum (h00) as H00, sum (h01) as H01, sum (h02) as H02, sum (h03) as H03, sum (h04) as H04, sum (h05) as H05, sum (h06) as H06, sum (h07) as H07, sum (h08) as H08, 
sum (h09) as H09, sum (h10) as H10, sum (h11) as H11, sum (h12) as H12, sum (h13) as H13, sum (h14) as H14, sum (h15) as H15, sum (h16) as H16, sum (h17) as H17, 
sum (h18) as H18, sum (h19) as H19, sum (h20) as H20, sum (h21) as H21, sum (h22) as H22, sum (h23) as H23
from sum_all_ori_hour_season_notransport
where season = '1'
group by key_code, season, geom;


create table sum_ori_hour_notrsansport_summer as
select key_code, season, geom, sum(sum), sum(sum_morning) as sum_morning, sum(sum_evening) as sum_evening, sum(sum_non_peak) as sum_non_peak, sum(sum_night) as sum_night,
sum (h00) as H00, sum (h01) as H01, sum (h02) as H02, sum (h03) as H03, sum (h04) as H04, sum (h05) as H05, sum (h06) as H06, sum (h07) as H07, sum (h08) as H08, 
sum (h09) as H09, sum (h10) as H10, sum (h11) as H11, sum (h12) as H12, sum (h13) as H13, sum (h14) as H14, sum (h15) as H15, sum (h16) as H16, sum (h17) as H17, 
sum (h18) as H18, sum (h19) as H19, sum (h20) as H20, sum (h21) as H21, sum (h22) as H22, sum (h23) as H23
from sum_all_ori_hour_season_notransport
where season = '2'
group by key_code, season, geom;


create table sum_ori_hour_notrsansport_autumn as
select key_code, season, geom, sum(sum), sum(sum_morning) as sum_morning, sum(sum_evening) as sum_evening, sum(sum_non_peak) as sum_non_peak, sum(sum_night) as sum_night,
sum (h00) as H00, sum (h01) as H01, sum (h02) as H02, sum (h03) as H03, sum (h04) as H04, sum (h05) as H05, sum (h06) as H06, sum (h07) as H07, sum (h08) as H08, 
sum (h09) as H09, sum (h10) as H10, sum (h11) as H11, sum (h12) as H12, sum (h13) as H13, sum (h14) as H14, sum (h15) as H15, sum (h16) as H16, sum (h17) as H17, 
sum (h18) as H18, sum (h19) as H19, sum (h20) as H20, sum (h21) as H21, sum (h22) as H22, sum (h23) as H23
from sum_all_ori_hour_season_notransport
where season = '3'
group by key_code, season, geom;

create table sum_ori_hour_notrsansport_winter as
select key_code, season, geom, sum(sum), sum(sum_morning) as sum_morning, sum(sum_evening) as sum_evening, sum(sum_non_peak) as sum_non_peak, sum(sum_night) as sum_night,
sum (h00) as H00, sum (h01) as H01, sum (h02) as H02, sum (h03) as H03, sum (h04) as H04, sum (h05) as H05, sum (h06) as H06, sum (h07) as H07, sum (h08) as H08, 
sum (h09) as H09, sum (h10) as H10, sum (h11) as H11, sum (h12) as H12, sum (h13) as H13, sum (h14) as H14, sum (h15) as H15, sum (h16) as H16, sum (h17) as H17, 
sum (h18) as H18, sum (h19) as H19, sum (h20) as H20, sum (h21) as H21, sum (h22) as H22, sum (h23) as H23
from sum_all_ori_hour_season_notransport
where season = '4'
group by key_code, season, geom;


create table sum_des_hour_notrsansport_spring as
select key_code, season, geom, sum(sum), sum(sum_morning) as sum_morning, sum(sum_evening) as sum_evening, sum(sum_non_peak) as sum_non_peak, sum(sum_night) as sum_night,
sum (h00) as H00, sum (h01) as H01, sum (h02) as H02, sum (h03) as H03, sum (h04) as H04, sum (h05) as H05, sum (h06) as H06, sum (h07) as H07, sum (h08) as H08, 
sum (h09) as H09, sum (h10) as H10, sum (h11) as H11, sum (h12) as H12, sum (h13) as H13, sum (h14) as H14, sum (h15) as H15, sum (h16) as H16, sum (h17) as H17, 
sum (h18) as H18, sum (h19) as H19, sum (h20) as H20, sum (h21) as H21, sum (h22) as H22, sum (h23) as H23
from sum_all_des_hour_season_notransport
where season = '1'
group by key_code, season, geom;


create table sum_des_hour_notrsansport_summer as
select key_code, season, geom, sum(sum), sum(sum_morning) as sum_morning, sum(sum_evening) as sum_evening, sum(sum_non_peak) as sum_non_peak, sum(sum_night) as sum_night,
sum (h00) as H00, sum (h01) as H01, sum (h02) as H02, sum (h03) as H03, sum (h04) as H04, sum (h05) as H05, sum (h06) as H06, sum (h07) as H07, sum (h08) as H08, 
sum (h09) as H09, sum (h10) as H10, sum (h11) as H11, sum (h12) as H12, sum (h13) as H13, sum (h14) as H14, sum (h15) as H15, sum (h16) as H16, sum (h17) as H17, 
sum (h18) as H18, sum (h19) as H19, sum (h20) as H20, sum (h21) as H21, sum (h22) as H22, sum (h23) as H23
from sum_all_des_hour_season_notransport
where season = '2'
group by key_code, season, geom;


create table sum_des_hour_notrsansport_autumn as
select key_code, season, geom, sum(sum), sum(sum_morning) as sum_morning, sum(sum_evening) as sum_evening, sum(sum_non_peak) as sum_non_peak, sum(sum_night) as sum_night,
sum (h00) as H00, sum (h01) as H01, sum (h02) as H02, sum (h03) as H03, sum (h04) as H04, sum (h05) as H05, sum (h06) as H06, sum (h07) as H07, sum (h08) as H08, 
sum (h09) as H09, sum (h10) as H10, sum (h11) as H11, sum (h12) as H12, sum (h13) as H13, sum (h14) as H14, sum (h15) as H15, sum (h16) as H16, sum (h17) as H17, 
sum (h18) as H18, sum (h19) as H19, sum (h20) as H20, sum (h21) as H21, sum (h22) as H22, sum (h23) as H23
from sum_all_des_hour_season_notransport
where season = '3'
group by key_code, season, geom;

create table sum_des_hour_notrsansport_winter as
select key_code, season, geom, sum(sum), sum(sum_morning) as sum_morning, sum(sum_evening) as sum_evening, sum(sum_non_peak) as sum_non_peak, sum(sum_night) as sum_night,
sum (h00) as H00, sum (h01) as H01, sum (h02) as H02, sum (h03) as H03, sum (h04) as H04, sum (h05) as H05, sum (h06) as H06, sum (h07) as H07, sum (h08) as H08, 
sum (h09) as H09, sum (h10) as H10, sum (h11) as H11, sum (h12) as H12, sum (h13) as H13, sum (h14) as H14, sum (h15) as H15, sum (h16) as H16, sum (h17) as H17, 
sum (h18) as H18, sum (h19) as H19, sum (h20) as H20, sum (h21) as H21, sum (h22) as H22, sum (h23) as H23
from sum_all_des_hour_season_notransport
where season = '4'
group by key_code, season, geom;


create table od_link_hour_notransport_inzone as
select distinct * from 
	(select a.* from od_link_volumn_btwzone_hour_dis a
	inner join sum_all_des_hour_season_notransport b on a.des_code = b.key_code) c;

create table od_link_hour_notransport_btwzone as
select distinct * from 
	(select a.* from od_link_hour_notransport_inzone a
	inner join sum_all_ori_hour_season_notransport b on a.ori_code = b.key_code) c;



