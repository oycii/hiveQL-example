https://www.docker.com/products/docker-desktop/
https://desktop.docker.com/win/main/amd64/Docker%20Desktop%20Installer.exe?utm_source=docker&utm_medium=webreferral&utm_campaign=dd-smartbutton&utm_location=header

git clone https://github.com/big-data-europe/docker-hive.git
cd docker-hive
docker-compose up -d
docker-compose exec hive-server bash

CONNECT BY DBeaver: jdbc:hive2://localhost:10000

docker-compose down

--Dowload airport data:
curl -L https://datahub.io/core/airport-codes/r/0.csv > airports.csv

