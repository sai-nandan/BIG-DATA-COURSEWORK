-- Databricks notebook source
CREATE EXTERNAL TABLE clinic21
( Id STRING ,
Sponsor STRING ,
Status STRING ,
Started STRING ,
Completion STRING,
Type STRING,
Submission STRING,
Conditions STRING,
Interventions STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LOCATION '/FileStore/new21/';

-- COMMAND ----------

CREATE EXTERNAL TABLE mesh1
( Term STRING ,
Tree STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/FileStore/mesh/';

-- COMMAND ----------

CREATE EXTERNAL TABLE pharma1
( Company STRING ,
Parent_Company STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/FileStore/pharma/';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #dbutils.fs.cp("FileStore/tables/pharma.csv", "FileStore/pharma")
-- MAGIC #dbutils.fs.cp("FileStore/tables/mesh.csv", "FileStore/mesh")
-- MAGIC #dbutils.fs.cp("FileStore/tables/clinicaltrial_2021.csv", "FileStore/new21")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC IMPLEMENTATION OF QUESTION 1

-- COMMAND ----------

select count(distinct(id))-1 as Distinct_Studies from clinic21;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC IMPLEMENTATION OF QUESTION 2

-- COMMAND ----------

select Type,count(*)as cnt from clinic21 where Type!= "Type"  GROUP BY Type ORDER BY cnt desc limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC IMPLEMENTATION OF QUESTION 3

-- COMMAND ----------

create or replace view q4 as 
select explode(split(Conditions,",")) from clinic21 where Conditions !=""

-- COMMAND ----------

select col, count(*)as cnt 
from q4 group by col order by cnt desc LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC IMPLEMENTATION OF QUESTION 4

-- COMMAND ----------

create or replace view q14 as
select Term, substr(Tree, 1,3)as codes from mesh1

-- COMMAND ----------

create or replace view q144 as
select c.col, m.Term, m.codes
from q4 c
join q14 m on (c.col = m.Term)

-- COMMAND ----------

select codes, count(*) as cn from q144 group by codes order by cn desc limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC IMPLEMENTATION OF QUESTION 5

-- COMMAND ----------


select Sponsor, count(*) as cnt from clinic21 s left outer join pharma1 p on(s.Sponsor = replace(p.Parent_Company, '"','')) 
where p.Parent_Company is NULL group by Sponsor order by cnt desc limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC IMPLEMENTATION OF QUESTION 6

-- COMMAND ----------

create or replace view q6 as
select Status, substr(Completion, 1,4) as month , substr(Completion, 4,8) as year from clinic21;

-- COMMAND ----------

select month, count(*) as cnt from q6 where Status = "Completed" and year =2021 group by month order by from_unixtime(unix_timestamp(replace(month,' ',''), "MMM"))

-- COMMAND ----------


