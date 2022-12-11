https://www.docker.com/products/docker-desktop/
https://desktop.docker.com/win/main/amd64/Docker%20Desktop%20Installer.exe?utm_source=docker&utm_medium=webreferral&utm_campaign=dd-smartbutton&utm_location=header

git clone https://github.com/big-data-europe/docker-hive.git
cd docker-hive
docker-compose up -d
docker-compose exec hive-server bash

-- CONNECT BY DBeaver: jdbc:hive2://localhost:10000
-- docker-compose down


curl -L https://github.com/oycii/hiveQL-example/archive/refs/heads/main.zip > main.zip
apt update
apt install unzip
unzip main.zip

--Dowload airport data:
hdfs dfs -mkdir /user/hive/warehouse/data-airports
hdfs dfs -mkdir /user/hive/warehouse/data-airports/airports
hdfs dfs -put hiveQL-example-main/data/airports.csv /user/hive/warehouse/data-airports/airports
--hdfs dfs -rm -R /user/hive/warehouse/data-airports/airports.csv
hdfs dfs -mkdir /user/hive/warehouse/data-airports/flights
hdfs dfs -put hiveQL-example-main/data/flights.csv /user/hive/warehouse/data-airports/flights
hdfs dfs -mkdir /user/hive/warehouse/data-airports/raw-flight-data
hdfs dfs -put hiveQL-example-main/data/raw-flight-data.csv /user/hive/warehouse/data-airports/raw-flight-data

--Check that data exist:
hdfs dfs -ls /user/hive/warehouse/data-airports

--Run with hive console:
hive

create database airports;

DROP TABLE IF EXISTS airports.airports;

--Create external Hive table:
CREATE EXTERNAL TABLE airports.airports (
    airport_id STRING,
    city STRING,
    state STRING,
    name STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/user/hive/warehouse/data-airports/airports'
    tblproperties("skip.header.line.count"="1");

CREATE EXTERNAL TABLE airports.flights (
    DayofMonth STRING,
    DayOfWeek STRING,
    Carrier STRING,
    OriginAirportID STRING,
    DestAirportID STRING,
    DepDelay STRING,
    ArrDelay STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/user/hive/warehouse/data-airports/flights'
    tblproperties("skip.header.line.count"="1");

CREATE EXTERNAL TABLE airports.raw_flight_data (
    DayofMonth STRING,
    DayOfWeek STRING,
    Carrier STRING,
    OriginAirportID STRING,
    DestAirportID STRING,
    DepDelay STRING,
    ArrDelay STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/user/hive/warehouse/data-airports/raw-flight-data'
    tblproperties("skip.header.line.count"="1");


-- Самый популярный аэропорт прилётов
with tops as (
	select destairportid, count(destairportid) as aircount
	from airports.flights
	group by destairportid
	order by aircount desc
),
air as (
	select airport_id, city, state, name
	from airports.airports
)
select a.name, a.city, t.aircount
from tops t left join air a on t.destairportid = a.airport_id;

-- Самый популярный аэропорт вылетов
with tops as (
	select originairportid, count(originairportid) as aircount
	from airports.flights
	group by destairportid
	order by aircount desc
),
air as (
	select airport_id, city, state, name
	from airports.airports
)
select a.name, a.city, t.aircount
from tops t left join air a on t.originairportid = a.airport_id;

-- Объеденённая информация вылетов и прилётов, которая позволяет увидеть некую анамалию у аэропортов Seattle и Minneapolis
with tops_in as (
	select destairportid, count(destairportid) as aircount
	from airports.flights
	group by destairportid
	order by aircount desc
),
air_in as (
	select airport_id, city, state, name
	from airports.airports
),
tops_out as (
	select originairportid, count(originairportid) as aircount
	from airports.flights
	group by originairportid
	order by aircount desc
),
air_out as (
	select airport_id, city, state, name
	from airports.airports
)
select ia.name, ia.city, it.aircount as aircount, "in" as direction, it.destairportid
from tops_in it left join air_in ia on it.destairportid = ia.airport_id
union all
select oa.name, oa.city, ot.aircount as aircount, "out" as direction, ot.originairportid
from tops_out ot left join air_out oa on ot.originairportid = oa.airport_id
order by aircount desc;

-- Самые популярные дни вылета по всем аэропортам
select
case
	when f.dayofweek = 1 then "Воскресение"
	when f.dayofweek = 2 then "Понедельник"
	when f.dayofweek = 3 then "Вторник"
	when f.dayofweek = 4 then "Среда"
	when f.dayofweek = 5 then "Четверг"
	when f.dayofweek = 6 then "Пятница"
	when f.dayofweek = 7 then "Суббота"
end as dayofweek,
count(f.dayofweek) as cnt
from airports.flights f
group by f.dayofweek
order by cnt desc;

-- Самые популярные дни вылетов для самого топового аэропорта Atlanta
select
case
	when f.dayofweek = 1 then "Воскресение"
	when f.dayofweek = 2 then "Понедельник"
	when f.dayofweek = 3 then "Вторник"
	when f.dayofweek = 4 then "Среда"
	when f.dayofweek = 5 then "Четверг"
	when f.dayofweek = 6 then "Пятница"
	when f.dayofweek = 7 then "Суббота"
end as dayofweek,
count(f.dayofweek) as cnt,
f.originairportid
from airports.flights f
where f.originairportid = "11433"
group by f.dayofweek, f.originairportid
order by cnt desc;

-- Распределение вылетов по дням месяца с информацией распределения количества вылетов по дням недели по всем аэропортам
with cday as (
	select
		f.dayofmonth,
		count(f.dayofmonth) as cnt,
		f.dayofweek,
		count(f.dayofweek) OVER(PARTITION BY f.originairportid ORDER BY f.dayofweek) as dayofweek_count,
		f.originairportid
	from airports.flights f
	group by f.dayofmonth, f.dayofweek, f.originairportid
	order by f.originairportid, cnt desc),
air as (
	select airport_id, city, state, name
	from airports.airports
)
select *
from cday c join air a on c.originairportid = a.airport_id
