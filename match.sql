-----step1: creating master table where we eph_id in all tables
CREATE OR REPLACE TABLE`datavant_ent_dev.master_lob`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select * from anbc-dev.datavant_ent_dev.clean_room_rtl_phmcy_digital_2309_table where not (token_4  is null  and token_12  is null  and token_17  is null   and token_9 is null   and token_30  is null   and token_29  is null)
union all
select * from anbc-dev.datavant_ent_dev.clean_room_hcb_digital_2309_table where not (token_4  is null  and token_12  is null  and token_17  is null   and token_9 is null   and token_30  is null   and token_29  is null)
union all
select * from anbc-dev.datavant_ent_dev.clean_room_pbm_digital_2309_table where not (token_4  is null  and token_12  is null  and token_17  is null   and token_9 is null   and token_30  is null   and token_29  is null)
union all
select * from anbc-dev.datavant_ent_dev.clean_room_mc_digital_2309_table where not (token_4  is null  and token_12  is null  and token_17  is null   and token_9 is null   and token_30  is null   and token_29  is null)
union all
select * from anbc-dev.datavant_ent_dev.clean_room_spc_digital_2309_table where not (token_4  is null  and token_12  is null  and token_17  is null   and token_9 is null   and token_30  is null   and token_29  is null);
-----------------------------------
------- step 2 : creating fs_master_table where all record's eph_id is null
CREATE OR REPLACE TABLE`datavant_ent_dev.clean_room_fs_digital_overlap_digital_1`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select * from anbc-dev.datavant_ent_dev.clean_room_fs_digital_2309_table where not (token_4  is null  and token_12  is null  and token_17  is null   and token_9 is null   and token_30  is null   and token_29  is null);
---------------------------------------
------ step 3:
update `datavant_ent_dev.master_lob`
set token_4 = concat(token_4,'_',EPH_id) where token_4 in (select token_4
from `datavant_ent_dev.master_lob` group by token_4
having count(distinct eph_id)>1);
-----------------------------------------------------------
----- step 4:
update `datavant_ent_dev.master_lob`
set token_12 = concat(token_12,'_',EPH_id) where token_12 in (select token_12
from `datavant_ent_dev.master_lob` group by token_12
having count(distinct eph_id)>1
);
-----------------------------------------------------------
---- step 5: 
update `datavant_ent_dev.master_lob`
set token_17 = concat(token_17,'_',EPH_id) where token_17 in (select token_17
from `datavant_ent_dev.master_lob` group by token_17
having count(distinct eph_id)>1
);
-----------------------------------------------------------
update `datavant_ent_dev.master_lob`
set token_9 = concat(token_9,'_',EPH_id) where token_9 in (select token_9
from `datavant_ent_dev.master_lob` group by token_9
having count(distinct eph_id)>1
);
-----------------------------------------------------------
update `datavant_ent_dev.master_lob`
set token_30 = concat(token_30,'_',EPH_id) where token_30 in (select token_30
from `datavant_ent_dev.master_lob` group by token_30
having count(distinct eph_id)>1
);
-----------------------------------------------------------
update `datavant_ent_dev.master_lob`
set token_29 = concat(token_29,'_',EPH_id) where token_29 in (select token_29
from `datavant_ent_dev.master_lob` group by token_29
having count(distinct eph_id)>1
);

-----------------------------------------------------------
-- Step 1: Create a table to store the distinct matched Token_4 values
CREATE OR REPLACE TABLE `datavant_ent_dev.tmp_token_4_distinct`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
SELECT DISTINCT
       a.EPH_id,
       b.Site_name,
       b.Token_4 ,
       b.Token_12 ,
       b.Token_17,
       b.Token_9 ,
       b.Token_30,
       b.Token_29
FROM  `datavant_ent_dev.master_lob` AS a
JOIN `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` AS b
ON a.Token_4 = b.Token_4;
 
----------step 2: creating table to insert the token_4 matched where site_name ='cvs_retail_fs'
CREATE OR REPLACE TABLE `datavant_ent_dev.dv_match_master1_token_4`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
SELECT * FROM  `datavant_ent_dev.master_lob`
UNION ALL
SELECT * FROM `datavant_ent_dev.tmp_token_4_distinct`;

-- ---- step 3:
DELETE FROM `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1`
WHERE Token_4 IN (
    SELECT Token_4 FROM `datavant_ent_dev.tmp_token_4_distinct`
) ;
 
---for token_12:
-- Step 1: Create a table to store the distinct matched Token_12 values
CREATE OR REPLACE TABLE `datavant_ent_dev.tmp_token_12_distinct`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
SELECT DISTINCT
       a.EPH_id,
       b.Site_name,
       b.Token_4 ,
       b.Token_12 ,
       b.Token_17,
       b.Token_9 ,
       b.Token_30,
       b.Token_29
FROM `datavant_ent_dev.dv_match_master1_token_4` AS a
JOIN `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` AS b
ON a.Token_12 = b.Token_12
WHERE b.TOKEN_4 is null;
----------step 2: creating table to insert the token_4 matched where site_name ='cvs_retail_fs'
CREATE OR REPLACE TABLE `datavant_ent_dev.dv_match_master1_token_4_token_12`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
SELECT * FROM `datavant_ent_dev.dv_match_master1_token_4`
UNION ALL
SELECT * FROM `datavant_ent_dev.tmp_token_12_distinct` ;


---- step 3:
DELETE FROM `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1`
WHERE  Token_12 IN (
    SELECT Token_12 FROM `datavant_ent_dev.tmp_token_12_distinct`
);
 
---for token_17:
-- Step 1: Create a table to store the distinct matched Token_17 values
CREATE OR REPLACE TABLE `datavant_ent_dev.tmp_token_17_distinct`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
SELECT DISTINCT
       a.EPH_id,
       b.Site_name,
       b.Token_4 ,
       b.Token_12 ,
       b.Token_17,
       b.Token_9 ,
       b.Token_30,
       b.Token_29
FROM `datavant_ent_dev.dv_match_master1_token_4_token_12` AS a
JOIN `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` AS b
ON a.Token_17 = b.Token_17
WHERE  b.Token_4 is null and  b.Token_12 is null;

----------step 2: creating table to insert the token_4 matched where site_name ='cvs_retail_fs'
CREATE OR REPLACE TABLE `datavant_ent_dev.dv_match_master1_token_4_token_12_token_17`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
SELECT * FROM `datavant_ent_dev.dv_match_master1_token_4_token_12`
UNION ALL
SELECT * FROM `datavant_ent_dev.tmp_token_17_distinct`;

---- step 3:
DELETE FROM `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1`
WHERE token_4 is null and token_12 is null and token_17 IN (
    SELECT Token_17 FROM `datavant_ent_dev.tmp_token_17_distinct`
);

 
---for token_9:
-- Step 1: Create a table to store the distinct matched Token_9 values
CREATE OR REPLACE TABLE `datavant_ent_dev.tmp_token_9_distinct`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
SELECT DISTINCT
       a.EPH_id,
       b.Site_name,
       b.Token_4 ,
       b.Token_12 ,
       b.Token_17,
       b.Token_9 ,
       b.Token_30,
       b.Token_29
FROM `datavant_ent_dev.dv_match_master1_token_4_token_12_token_17` AS a
JOIN `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` AS b
ON a.Token_9 = b.Token_9
WHERE  b.Token_4 is null and b.Token_12 is null and b.Token_17 is null;
 
----------step 2: creating table to insert the token_4 matched where site_name ='cvs_retail_fs'
CREATE OR REPLACE TABLE `datavant_ent_dev.dv_match_master1_token_4_token_12_token_17_token_9`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
SELECT * FROM `datavant_ent_dev.dv_match_master1_token_4_token_12_token_17`
UNION ALL
SELECT * FROM `datavant_ent_dev.tmp_token_9_distinct`;

 
---- step 3:this one
DELETE FROM `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1`
WHERE  token_4 is null and token_12 is null and token_17 is null and token_9 IN
(  SELECT Token_9 FROM `datavant_ent_dev.tmp_token_9_distinct`
);
 
---for token_30:
-- Step 1: Create a table to store the distinct matched Token_9 values
CREATE OR REPLACE TABLE `datavant_ent_dev.tmp_token_30_distinct`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
SELECT DISTINCT
       a.EPH_id,
       b.Site_name,
       b.Token_4 ,
       b.Token_12 ,
       b.Token_17,
       b.Token_9 ,
       b.Token_30,
       b.Token_29
FROM `datavant_ent_dev.dv_match_master1_token_4_token_12_token_17_token_9` AS a
JOIN `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` AS b
ON a.Token_30 = b.Token_30
WHERE  b.TOKEN_4 is null and b.TOKEN_12 is null  and b.TOKEN_17 is null and b.TOKEN_9 is null;
 
----------step 2: creating table to insert the token_4 matched where site_name ='cvs_retail_fs'
CREATE OR REPLACE TABLE `datavant_ent_dev.dv_match_master1_token_4_token_12_token_17_token_9_token_30`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
SELECT * FROM `datavant_ent_dev.dv_match_master1_token_4_token_12_token_17_token_9`
UNION ALL
SELECT * FROM `datavant_ent_dev.tmp_token_30_distinct`;
---- step 3:
DELETE FROM `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1`
WHERE  token_4 is null and token_12 is null and token_17 is null and token_9 is null and  token_30 IN (
    SELECT Token_30 FROM  `datavant_ent_dev.tmp_token_30_distinct`
);

---for token_29:
-- Step 1: Create a table to store the distinct matched Token_29 values
CREATE OR REPLACE TABLE `datavant_ent_dev.tmp_token_29_distinct`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
SELECT DISTINCT
       a.EPH_id,
       b.Site_name,
       b.Token_4 ,
       b.Token_12 ,
       b.Token_17,
       b.Token_9 ,
       b.Token_30,
       b.Token_29
FROM `datavant_ent_dev.dv_match_master1_token_4_token_12_token_17_token_9_token_30` AS a
JOIN `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` AS b
ON a.Token_29 = b.Token_29
WHERE  b.TOKEN_4 is null and b.TOKEN_12 is null and b.TOKEN_17 is null and b.TOKEN_9 is null and b.TOKEN_30 is null ;

----------step 2: creating table to insert the token_4 matched where site_name ='cvs_retail_fs'
CREATE OR REPLACE TABLE `datavant_ent_dev.dv_match_master1_token_4_token_12_token_17_token_9_token_30_token_29`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
SELECT * FROM `datavant_ent_dev.dv_match_master1_token_4_token_12_token_17_token_9_token_30`
UNION ALL
SELECT * FROM `datavant_ent_dev.tmp_token_29_distinct`;

---- step 3:
DELETE FROM `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1`
WHERE token_4 is null and token_12 is null and token_17 is null and token_9 is null and token_30 is null and token_29 IN (
    SELECT Token_29 FROM `datavant_ent_dev.tmp_token_29_distinct`
);

--------------------------------------
 ---- whatever matched from fs_master to master given eph_id and deleted from fs_master and whatever left are being matched in fs_table it self
CREATE OR REPLACE TABLE `datavant_ent_dev.token_4_from_fs`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select * from  `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` where token_4  is not null;

--------step 1:
CREATE OR REPLACE TABLE `datavant_ent_dev.token_4_from_fs`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select * from  `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` where token_4  is not null;
-- select * from  {PROJECT_ID}.{DATASET_ID}.{FS_MASTER_TABLE}  where token_4  is not null;
------ step 2:
delete from  `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` where token_4  is not null;

-- delete from  {PROJECT_ID}.{DATASET_ID}.{FS_MASTER_TABLE}  where token_4  is not null;

------for token_12
----- step 3:
CREATE OR REPLACE TABLE `datavant_ent_dev.token_12_not_null`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select * from `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` where token_12 in (select token_12 from `datavant_ent_dev.token_4_from_fs`) ;

------- step4:
update `datavant_ent_dev.token_4_from_fs`
set token_4 = null
where token_12 in (select  token_12
from `datavant_ent_dev.token_12_not_null` );


------ step 5:
delete  from `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` where token_12 in (select token_12 from  `datavant_ent_dev.token_12_not_null`) ;

 

-----step6:
CREATE OR REPLACE TABLE `datavant_ent_dev.token_4_12_from_fs`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select * from `datavant_ent_dev.token_4_from_fs`
union all
select * from `datavant_ent_dev.token_12_not_null`;

------ for token_17
----- step 3
CREATE OR REPLACE TABLE `datavant_ent_dev.token_17_not_null`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select * from `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` where token_17 in (select token_17 from `datavant_ent_dev.token_4_12_from_fs`)  and token_12 is null;

------- step4:
update `datavant_ent_dev.token_4_from_fs`
set token_4 = null, token_12 = null
where token_17 in (select  token_17
from `datavant_ent_dev.token_17_not_null` );
 
------ step 5:
delete  from `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` where token_12 in (select token_12 from  `datavant_ent_dev.token_17_not_null`) ;
-----step6:
CREATE OR REPLACE TABLE `datavant_ent_dev.token_4_12_from_fs`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select * from `datavant_ent_dev.token_4_from_fs`
union all
select * from `datavant_ent_dev.token_12_not_null`;

------ for token_9
----- step 3:
CREATE OR REPLACE TABLE `datavant_ent_dev.token_9_not_null`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select * from `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` where token_9 in (select token_9 from`datavant_ent_dev.token_4_12_from_fs`)  and token_12 is null and token_17 is null;

 
------- step4:
update `datavant_ent_dev.token_4_12_from_fs`
set token_4 = null, token_12=null,token_17=null
where token_9 in (select  token_9
from `datavant_ent_dev.token_9_not_null` );

------ step 5:
delete  from `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` where token_9 in (select token_9 from  `datavant_ent_dev.token_9_not_null`) ;

-----step6:
CREATE OR REPLACE TABLE `datavant_ent_dev.token_4_12_17_9_from_fs`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select * from `datavant_ent_dev.token_4_12_from_fs`
union all
select * from `datavant_ent_dev.token_9_not_null`;
 
------ for token_30
----- step 3:
CREATE OR REPLACE TABLE `datavant_ent_dev.token_30_not_null`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select * from `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` where token_30 in (select token_30 from  `datavant_ent_dev.token_4_12_17_9_from_fs`)  and token_12 is null and token_17 is null and token_9 is null;
 
------- step4:
update  `datavant_ent_dev.token_4_12_17_9_from_fs`
set token_4 = null, token_12=null,token_17=null, token_9 = null
where token_30 in (select  token_30
from `datavant_ent_dev.token_30_not_null` );

------ step 5:
delete  from `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` where token_30 in (select token_30 from  `datavant_ent_dev.token_30_not_null`) ;

 
-----step6:
CREATE OR REPLACE TABLE `datavant_ent_dev.token_4_12_17_9_30_from_fs`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select * from  `datavant_ent_dev.token_4_12_17_9_from_fs`
union all
select * from `datavant_ent_dev.token_30_not_null`;

 
------ for token_29
----- step 3:
CREATE OR REPLACE TABLE `datavant_ent_dev.token_29_not_null`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select * from `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` where token_29 in (select token_29 from `datavant_ent_dev.token_4_12_17_9_30_from_fs`)  and token_12 is null and token_17 is null and token_9 is null and token_30 is null;

 
------- step4:
update  `datavant_ent_dev.token_4_12_17_9_30_from_fs`
set token_4 = null, token_12=null,token_17=null, token_9 = null,token_30=null
where token_29 in (select  token_29
from `datavant_ent_dev.token_29_not_null` );
 
------ step 5:
delete  from `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1` where token_29 in (select token_29 from  `datavant_ent_dev.token_29_not_null`) ;
-----step6:
CREATE OR REPLACE TABLE `datavant_ent_dev.token_4_12_17_9_30__29_from_fs`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select * from `datavant_ent_dev.token_4_12_17_9_30_from_fs`
union all
select * from `datavant_ent_dev.token_29_not_null`;

 
------------------------------
CREATE OR REPLACE TABLE `datavant_ent_dev.fs_to_master`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select * from  `datavant_ent_dev.token_4_12_17_9_30__29_from_fs`
union all
select * from `datavant_ent_dev.clean_room_fs_digital_overlap_digital_1`;

update `datavant_ent_dev.fs_to_master`
set eph_id = coalesce(token_4,token_12,token_17,token_9,token_30,token_29)
where 1 = 1;


CREATE OR REPLACE TABLE `datavant_ent_dev.final_fs_12_11`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select * from `datavant_ent_dev.dv_match_master1_token_4_token_12_token_17_token_9_token_30_token_29`
union all
select * from `datavant_ent_dev.fs_to_master`;
 
----------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `datavant_ent_dev.dv_match_12_11`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select dense_rank() over (order by eph_id) as dv_match_id, eph_id,site_name,token_4,token_12,token_17,token_9,token_30,token_29  from  `datavant_ent_dev.final_fs_12_11`; 

 --
select max(dv_match_id ) from `datavant_ent_dev.dv_match_12_11`;
-- or3bxGndlEr9u------------
select column_name as Tokens, round(countif(column_value is not null )/count(*),3) as fill_rate
from   `datavant_ent_dev.final_fs_12_11`,
unnest
([
    struct('token_4' as column_name, token_4 as column_value),
    struct('token_12',token_12),
    struct('token_17',token_17),
    struct('token_9',token_9),
    struct('token_30',token_30),
    struct('token_29',token_29)
])
group by column_name order by column_name asc;
 
--- FOR NUM INDIVIDUALS-------------------------------------------------
select max(dv_match_id ) as num_individuals from `datavant_ent_dev.datavant_final_2309`;
----------------------------------------------------------------------------------------------------------------------------
-----step 19:
CREATE OR REPLACE TABLE `datavant_ent_dev.2309_final_tmp_dv_match`
OPTIONS (
    labels = [("owner", "myakalar_aetna_com")]
) AS
select dense_rank() over (order by eph_id) as dv_match_id, eph_id,site_name,token_4,token_12,token_17,token_9,token_30,token_29  from `datavant_ent_dev.dv_match_master1_token_4_token_12_token_17_token_9_token_30_token_29`;
------------------------------------------
 
---- step 20 :
select max(dv_match_id) from `datavant_ent_dev.2309_final_tmp_11_30`;

 
--- "dv_final":
Select individual_frequency,count(*) as individuals FROM
(select eph_id , count(*) as individual_frequency from   `datavant_ent_dev.dv_final_null`  
group by eph_id) as subquery
where individual_frequency between 1 and 50000
group by individual_frequency order by individual_frequency;


 