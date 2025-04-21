--用例1

create table YDS19_DDL_RENAME.test1(id int,id1 char(10));
create table YDS19_DDL_RENAME_MAP.test1(id int,id1 char(10));

--改名已存在表名
--直接改名(不含映射)
alter table YDS19_DDL_RENAME.test1 rename to test2;
insert into YDS19_DDL_RENAME.test2 values(1,'a');

--改名增量阶段建表
create table YDS19_DDL_RENAME.test4(id int,id1 char(10));
create table YDS19_DDL_RENAME.test3(id int,id1 char(10));

--改名含有映射规则
alter table YDS19_DDL_RENAME_MAP.test1 rename to test2;
insert into YDS19_DDL_RENAME_MAP.test2 values(1,'a');
--YDS19_DDL_RENAME_MAP————>YDS19_MAP
--test1——>T_TEST1

--改名回原名
alter table YDS19_DDL_RENAME.test3 rename to test33;
alter table YDS19_DDL_RENAME.test33 rename to test3;

--含有映射规则回原名
alter table YDS19_DDL_RENAME_MAP.test2 rename to test1;
--改名有中文
--改名有特殊符号
alter table YDS19_DDL_RENAME.test4 rename to "$#%^&*tab中文_ yep.'.,/n/r/t'“‘";


--用例2
--truncate

create table YDS19_DDL_DROP.test1(id int,id1 char(10),id2 clob);
insert into YDS19_DDL_DROP.test1 values(1,'a','b');
insert into YDS19_DDL_DROP.test1 values(2,'c','b');
create table YDS19_DDL_DROP_MAP.test1(id int,id1 char(10));
insert into YDS19_DDL_DROP_MAP.test1 values(1,'a');
insert into YDS19_DDL_DROP_MAP.test1 values(2,'c');

--0.改名——>插入——>清除——>删表
alter table YDS19_DDL_DROP.test1 rename to test2;
insert into YDS19_DDL_DROP.test2 values(3,'c','aa');
insert into YDS19_DDL_DROP.test2 values(4,'d','bb');
--1.基础 清除 
-- For run in exact time，SQL execute in case
truncate table YDS19_DDL_DROP.test2;

--2.含有映射规则 清除
insert into YDS19_DDL_DROP_MAP.test1 values(3,'c');
insert into YDS19_DDL_DROP_MAP.test1 values(4,'d');
truncate table YDS19_DDL_DROP_MAP.test1;
--YDS19_DDL_DROP_MAP————>YDS19_DROP_MAP
--test1——>T_TEST1

--3.基础 删表
drop table YDS19_DDL_DROP.test2;
--4.含有映射规则 删表
drop table YDS19_DDL_DROP_MAP.test1;


--默认值
ALTER TABLE YDS18_DDL_COL.test ADD (col1 varchar(8000)  default '3aa测试1aD#$d',
                        COL2 INT DEFAULT 0, col3 INTERVAL DAY TO SECOND default null, 
  col4 INTERVAL YEAR TO MONTH default null,
  col5 RAW(50) DEFAULT 'FF',
  col6 float default 3.402823E38,
  col7 double default 1.7976931348623157E+125, 
  col8 number default 9999999999.9999999999, 
  col9 nvarchar(200) default '测1aD#$d~!@#$%^&*()"\`<>?😀😃😄😁😆', 
  col10 char(2000) default 'b', 
  col13 time default null, 
  col15 INTERVAL YEAR(9) TO MONTH default '178000000-00',
  col16 INTERVAL DAY TO SECOND default '+03 01:20:10.222222', 
  col17 INTERVAL YEAR TO MONTH default '+20-03',
  col18 time default '00:00:00.000000', 
  col20 INTERVAL YEAR(9) TO MONTH default '-178000000-00',
  col21 blob default 'abc1234567890',  col22 clob default 'add 20232229',
  col23 INTERVAL DAY(9) TO SECOND(9) default '100000000 00:00:00.000000', 
  col24 INTERVAL DAY(9) TO SECOND(9) default '-100000000 00:00:00.000000', 
  col25 date default ('2021-10-29'));
  

INSERT INTO YDS18_DDL_COL.test(ID) VALUES(1);

-- 创建表空间
create tablespace test_yds53;
-- 列的
create table YDS53_INCR_DDL_WITH_PK_CHANGE.test (id int primary key) tablespace test_yds53;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test values(1);
update YDS53_INCR_DDL_WITH_PK_CHANGE.test set id=2 where id=1;

create table YDS53_INCR_DDL_WITH_PK_CHANGE.test1 (id int, primary key(id)) tablespace test_yds53;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test1 values(1);
update YDS53_INCR_DDL_WITH_PK_CHANGE.test1 set id=2 where id=1;

create table YDS53_INCR_DDL_WITH_PK_CHANGE.test2 (id int primary key,col1 char(10)) tablespace test_yds53;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test2 values(1,'aaa');
update YDS53_INCR_DDL_WITH_PK_CHANGE.test2 set id=2 where id=1;

create table YDS53_INCR_DDL_WITH_PK_CHANGE.test3 (id int ,col1 char(10),primary key(col1)) tablespace test_yds53;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test3 values(1,'aaa');
update YDS53_INCR_DDL_WITH_PK_CHANGE.test3 set id=2 where id=1;

create table YDS53_INCR_DDL_WITH_PK_CHANGE.test4 (id int ,col1 char(10),col2 varchar(2),constraint test4_pk primary key(col2))tablespace test_yds53;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test4 values(1,'aaa','aa');
update YDS53_INCR_DDL_WITH_PK_CHANGE.test4 set id=2 where id=1;
update YDS53_INCR_DDL_WITH_PK_CHANGE.test4 set col2='bb';

create table YDS53_INCR_DDL_WITH_PK_CHANGE.test5 (id int ,col1 char(10),col2 number,primary key(col2)) tablespace test_yds53;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test5 values(1,'aaa',999);
update YDS53_INCR_DDL_WITH_PK_CHANGE.test5 set id=998 where col2=999;
update YDS53_INCR_DDL_WITH_PK_CHANGE.test5 set col1='bb';

create table YDS53_INCR_DDL_WITH_PK_CHANGE.test6 (id int ,col1 char(10),col2 float(5,2),primary key(col2)) tablespace test_yds53;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test6 values(1,'aaa',999);
update YDS53_INCR_DDL_WITH_PK_CHANGE.test6 set id=998 where col2=999;
update YDS53_INCR_DDL_WITH_PK_CHANGE.test6 set col1='bb';

create table YDS53_INCR_DDL_WITH_PK_CHANGE.test7 (id int ,col1 char(10),col2 bit,primary key(col2)) tablespace test_yds53;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test7 values(1,'aaa',1);
update YDS53_INCR_DDL_WITH_PK_CHANGE.test7 set col1='bb';

create table YDS53_INCR_DDL_WITH_PK_CHANGE.test8 (id int ,col1 char(10),col2 date,primary key(col2)) tablespace test_yds53;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test8 values(1,'aaa','2020-02-02');
update YDS53_INCR_DDL_WITH_PK_CHANGE.test8 set col1='bb';

create table YDS53_INCR_DDL_WITH_PK_CHANGE.test9 (id int ,col1 char(10),col2 time,primary key(col2)) tablespace test_yds53;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test9 values(1,'aaa','23:59:59.999999');
update YDS53_INCR_DDL_WITH_PK_CHANGE.test9 set col1='bb';

create table YDS53_INCR_DDL_WITH_PK_CHANGE.test10 (id int ,col1 char(10),col2 timestamp ,primary key(col2)) tablespace test_yds53;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test10 values(1,'aaa','9999-12-31 23:59:59.999999');
update YDS53_INCR_DDL_WITH_PK_CHANGE.test10 set col1='bb';

create table YDS53_INCR_DDL_WITH_PK_CHANGE.test11 (id int ,col1 char(10),col2 interval year to month, primary key(col2)) tablespace test_yds53;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test11 values(1,'aaa',INTERVAL '11' YEAR);
update YDS53_INCR_DDL_WITH_PK_CHANGE.test11 set col1='bb';

create table YDS53_INCR_DDL_WITH_PK_CHANGE.test12 (id int ,col1 char(10),col2 interval day to second , primary key(col2)) tablespace test_yds53;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test12 values(1,'aaa','50 00:00:00.000000');
update YDS53_INCR_DDL_WITH_PK_CHANGE.test12 set col1='bb';
-- add drop pk
create table YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type(COL1 date,COL2 tinyint,COL3 smallint,COL4 int,COL5 bigint,COL6 float,COL7 binary_float,COL8 real, COL9 double,
COL10 binary_double,COL11 nchar(1000),COL12 nvarchar(1600),COL13 bit,COL14 boolean,COL15 timestamp,COL16 timestamp(9),col17 INTERVAL YEAR TO MONTH,
col18 INTERVAL DAY TO SECOND) tablespace test_yds53;

-- 主键长度不能超6000
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type add primary key(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col13,COL14);

insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type values('2022-02-05',2,3,4,5,655,7666,8,999,1000,'11aaa0','12dshsh',1,1,'2022-02-01 10:50:59.999999','2022-02-01 10:50:59.999999',INTERVAL '11' MONTH,'50 00:00:00.000000');

update YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type set col1='9999-12-31';
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type drop primary key;
delete from YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type where col1='9999-12-31';

alter table YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type add constraint test_all_type_pk primary key(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col13,col14,col15);
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type values('2022-02-01',11,3,4,5,655,7666,8,999,1000,'11aaa0','12dshsh',1,1,'2022-02-01 10:50:59.999999','2022-02-01 10:50:59.999999',INTERVAL '11' MONTH,'50 00:00:00.000000');

update YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type set col2=111 where col2=11;
alter table  YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type drop constraint test_all_type_pk;

-- 无法使用modify多列主键
-- alter table YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type modify (COL1 date,COL2 int,COL7 binary_float, COL14 boolean,primary key(COL1,COL2,COL7,COL14));
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type modify (COL11 nchar(1000) primary key);
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type drop column  COL1;

create table YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type1(COL1 date,COL2 tinyint,COL3 smallint,COL4 int,COL5 bigint,COL6 float,COL7 binary_float,COL8 real,COL9 double,
COL10 binary_double,COL11 nchar(1000),COL12 nvarchar(1600),COL13 bit,COL14 boolean,COL15 timestamp,COL16 timestamp(9),col17 INTERVAL YEAR(4) TO MONTH,
col18 INTERVAL DAY(9) TO SECOND(6),primary key(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col13,COL14,COL15,COL16,col17,col18)) tablespace test_yds53;
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type1 drop primary key;;

-- 增加主键，与上次顺序不一样
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type1 add constraint test_all_type_pk_1 primary key(col7 ,col8,col1 ,col3 ,col2 ,col5 ,col6,col13, col4,col14,col10,col9,col11,col15);
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type1 values('2022-02-01',11,3,4,5,655,7666,8,999,1000,'11aaa0','12dshsh',1,1,'2022-02-01 10:50:59.999999','2022-02-01 10:50:59.999999',INTERVAL '11' MONTH,'50 00:00:00.000000');
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test_all_type1 values('2022-02-02',11,3,4,5,655,7666,8,999,1000,'11aaa0','12dshsh',1,1,'2022-02-01 10:50:59.999999','2022-02-01 10:50:59.999999',INTERVAL '11' MONTH,'50 00:00:00.000000');

-- add drop column with pk -single pk
create table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK(id int ,col1 int) tablespace test_yds53;
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK add column age int primary key ;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK values(1,2,3);
update YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK set age=10;
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK drop primary key;

alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK add column supplier_name int;
delete from YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK values(1,2,3,4);
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK  add primary key(supplier_name);
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK  drop primary key ;

-- 改为date类型需要该列为空
delete from YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK;
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK modify (col1 date primary key);
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK values(2,'2022-12-10',3,4);
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK drop column col1;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK values(2,4,4);
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK modify (AGE int primary key);

-- add column with pk -multi pk
create table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK1(id int ,col1 int) tablespace test_yds53;
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK1 add column (col2 INT, col3 INT, PRIMARY KEY (col2, col3));
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK1  drop primary key ;
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK1 add column (col4 INT, col5 INT, PRIMARY KEY (id,col4,col5));
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK1  drop primary key ;
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK1  add column (col6 date, PRIMARY KEY (id,col1,col2,col3,col4,col5,col6));
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK1 values(1,2,3,4,5,6,'2020-10-22');

-- 主键+默认值 及重命名主键
create table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT(id NUMBER ,col1 NUMBER) tablespace test_yds53;
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT modify (id date default '2020-02-02' primary key);
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT(col1) values(1);
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT  drop primary key ;
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT values('2020-02-02',1);
delete from YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT;
-- 变更主键类型，需要清空原有数据
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT modify (id NUMBER default 200 primary key);
insert into YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT(col1) values(1);
-- 删除主键 默认值依然存在
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT  drop primary key ;
-- 再在默认值基础上添加主键
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT  add primary key(id);
-- 重名名主键
alter table  YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT rename id to id_new;
-- 增加主键 默认值类型 字符
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT  drop primary key ;
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT  add column (col2 varchar(4000) default '~!@#$', PRIMARY KEY (col2));
-- 增加主键 默认值类型 浮点型
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT  drop primary key ;
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT  add column (col3  float default -3.402823E38, PRIMARY KEY (col3));
-- 修改col2长度，复合主键长度超过6000无法创建
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT  drop primary key ;
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT  modify col2 varchar(100);
-- 创建复合主键，复合主键列均有默认值
alter table YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT  add column (
col4 BINARY_DOUBLE DEFAULT -1.7976931348623157E+125,
col5 number default 3.141E-9,
col6 nvarchar(2000) default '中华',
col7 char(15) default '00:00:00.000000',
col8 INTERVAL DAY(9) TO SECOND(9) default '-100000000 00:00:00.000000',
col9 INTERVAL YEAR(9) TO MONTH default '-178000000-00',
PRIMARY KEY (id_new,col2,col3,col4,col5,col6,col7,col8,col9));

delete from YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT;
alter table  YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT modify ID_NEW NUMBER;
alter table  YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT modify COL1 NUMBER;
alter table  YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT modify COL7 CHAR(15);

insert into YDS53_INCR_DDL_WITH_PK_CHANGE.TEST_PK_WITH_DEFAULT(id_new) values(2);


-- 删除列比增加列多，删除的一部分为已有列
CREATE TABLE YDS53_INCR_DDL_WITH_PK_CHANGE.test_col_add2drop (
id int NOT NULL,
supplier_id double NULL,
supplier_name char(2) DEFAULT NULL primary key,
contact_name varchar(50) DEFAULT NULL
)  tablespace test_yds53;

--alter table YDS53_INCR_DDL_WITH_PK_CHANGE.test_col_add2drop add column id int,add column supplier_name1 int primary key,drop column id,drop column supplier_id,drop column supplier_name,drop column contact_name;
--alter table YDS53_INCR_DDL_WITH_PK_CHANGE.test_col_add2drop add column id int,add column supplier_name1 int,drop column id,drop column supplier_id,drop column supplier_name,drop column contact_name;
--alter table YDS53_INCR_DDL_WITH_PK_CHANGE.test_col_add2drop primary key supplier_name1;

insert into YDS53_INCR_DDL_WITH_PK_CHANGE.test_col_add2drop values(1,2,'a','nn');

create table YDS53_INCR_DDL_WITH_PK_CHANGE.table_end1(id int);
