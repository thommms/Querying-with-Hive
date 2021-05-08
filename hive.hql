/* 
Create a partitioned table using HIVE
The table must contain the following types of fields:string, MAP, ARRAY, STRUCT 
*/

--creating a database using hive
CREATE DATABASE profile_database LOCATION '/user/home/profile_data';

/*create a table to load our data from file personal_data.txt*/
CREATE TABLE profile_table(name string, classes array<string>, 
			mobile_info MAP<string,bigint>,
			age int,
			other_info STRUCT<company:string, pin:int, married:string, salary:int>)
		row format delimited
		fields terminated by '\t' /* fields are seperated by tab*/
		collection items terminated by ',' -- the arrays are seperated by comma
		MAP keys terminated by ':'	-- the mapping are seperated by colon ':'
		lines terminated by '\n';	--this represents next line

--describe the table to see how the table looks like
DESCRIBE profile_table;

--=======================================================================================================================
--load the txt file into the table 'profile_table'
LOAD data local inpath '/home/tokonkw/personal_data.txt' INTO TABLE profile_table;

--create a partitioned table and used age as the partition
CREATE table profile_partitioned_table(name string, classes array<string>, 
			mobile_info MAP<string,bigint>,
			age int,
			other_info struct<company:string, pin:int, married:string, salary:int>)
			PARTITIONED BY (age int)
		ROW format delimited
		FIELDS terminated by '\t'
		COLLECTION items terminated by ','
		MAP keys terminated by ':'
		LINES terminated by '\n';

--=======================================================================================================================
-- insert all data into the partitioned table created from the profile_table
INSERT OVERWRITE TABLE profile_partitioned_table PARTITION (age) SELECT * FROM profile_table;

--describe to see if the table is really partitioned
DESCRIBE profile_partitioned_table;

-- set the partition mode to be nonstrict
 SET hive.exec.dynamic.partition.mode=nonstrict

--=======================================================================================================================
--query the string field from the profile table e.g name is a string field
SELECT * from profile_partitioned_table where name='Thomas';
SELECT * from profile_partitioned_table where name='Fikayo';

--query the MAP using SELECT statement
SELECT * from profile_partitioned_table where mobile_info['personal']=343234324;

--Querying an array in hive
 SELECT name, mobile_info, classes[1] from profile_partitioned_table;

--Querying a struct in hive
SELECT * from profile_partitioned_table where other_info.married ='single';
SELECT * from profile_partitioned_table where other_info.company='OSU';


--=======================================================================================================================
--a. A query to manipulate columns using function calls
/* calling the inbuilt function, upper, to a column in the table. This function converts 
all letters to upper case. */
SELECT upper(name), mobile_info from profile_partitioned_table;

--b. A query to manipulate columns using arithmetric expression
--calculating the annual salary from the table
SELECT upper(name), mobile_info, round(other_info.salary/12) from profile_partitioned_table;


--calculate the average age using aggregate function.
SELECT avg(age) from profile_partitioned_table;


--example of nested SELECT statement in hive
SELECT avg(age) from profile_partitioned_table where other_info.salary< (SELECT avg(other_info.salary from profile_partitioned_table);


--writing a SELECT operation using group by statement
SELECT name, count(*) from profile_partitioned_table group by name;

--=======================================================================================================================
/*
Create another table and write a query to do a JOIN
*/
--created second table for the join from the second_table.txt file
CREATE table second_table(name string, hobby string, nationality string)
						row format delimited fields terminated by '\t'
						lines terminated by '\n';

--run a join command between profile_partitioned_table and second_table using the Left outer join
SELECT 
	first_table_j.name, first_table_j.mobile_info, first_table_j.age,second_table_j.hobby, second_table_j.nationality 
from profile_partitioned_table first_table_j LEFT OUTER JOIN second_table second_table_j
ON (first_table_j.name = second_table_j.name);
