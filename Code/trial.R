# apache spark and R interface
library(sparklyr)
# move, filter etc  data from and to database,dataframe .
library(dplyr)
# for SQL requests
library(DBI)


conf <- spark_config()
conf$spark.executor.memory <- "4GB"
conf$spark.memory.fraction <- 0.2
conf$spark.executor.cores <- 2
conf$spark.dynamicAllocation.enabled <- "false"

c <- spark_connect(master= "local" , 
                   version = "2.3.0",
                   config = conf,
                   spark_home = "/home/adam/spark/spark-2.3.0-bin-hadoop2.7")


# copy imported dataset from R environment to Spark , check Connections tab in RStudio IDE
table1 <- spark_read_csv(c, name = "database", path = "/home/adam/Letöltések/EXCEL_filteringOK.csv", header = TRUE, delimiter = ";")

# dplyr like req
table %>% filter(diagnosis == 0)
#spark sql request example: get the first 10 rows from database datatable
tabletrial <- dbGetQuery(c, "SELECT * FROM database ORDER BY height DESC LIMIT 10")

tableheight <-dbGetQuery(c,"SELECT weight, height  FROM database WHERE height > 1.8 AND weight > 90")

tablediag <- dbGetQuery(c, "SELECT * FROM database WHERE diagnosis = 0 ")
# get DB info using DBI package








dbGetQuery(c, "show databases")
dbGetQuery(c, "show tables in default")
dbGetQuery(c, "show tables in userdb")
dbGetQuery(c, "describe userdb.students")

### Create a new database, a new table, and insert data
dbGetQuery(c, "create database newdb")
dbGetQuery(c, "drop table if exists newdb.pageviews")
dbGetQuery(c, "create table newdb.pageviews (userid varchar(64), link string, came_from string)")
dbGetQuery(c, "insert into table newdb.pageviews values ('jsmith', 'mail.com', 'sports.com'), ('jdoe', 'mail.com', null)")

### This query does not work from R but works from the command prompt
dbGetQuery(c, "CREATE TABLE students (name VARCHAR(64), age INT, gpa DECIMAL(3, 2)) CLUSTERED BY (age) INTO 2 BUCKETS STORED AS ORC")

dbGetQuery(c, "use newdb")
dbGetQuery(c, "show tables in newdb")



