Dependencies needed to run - 

Task 1 

Task 2
.
├─ docker-compose.yml
├─ initdb/
│  └─ schema.sql
├─ data/
│  ├─ clean_customers.csv
│  ├─ clean_listings.csv
│  └─ clean_orders.csv
└─ loader/
   ├─ Dockerfile
   └─ load_to_mysql.py

#how to use
cd mysql-docker 
docker compose up -d 
docker exec -it mysql mysql -u root -ppassword mysql


#how it works 
schema.sql is under the initdb
