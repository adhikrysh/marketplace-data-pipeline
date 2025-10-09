
-> running the MySQL 8 image 
docker run -d --name mysql8 \
  -e MYSQL_ROOT_PASSWORD=secret \
  -e MYSQL_DATABASE=marketplace \
  -p 3306:3306 \
  -v "$PWD/mysql-data":/var/lib/mysql \
  mysql:8.0

-> run schema.sql 
docker exec -i mysql8 mysql -uroot -psecret marketplace < schema.sql

-> .env
The script uses environment variables so you don’t hard-code secrets:
MYSQL_HOST (default 127.0.0.1)
MYSQL_PORT (default 3306)
MYSQL_USER (default root)
MYSQL_PASSWORD (e.g., secret)
MYSQL_DB (default marketplace)

If you ran Docker exactly as above, the defaults work with MYSQL_PASSWORD=secret

-> # (optional) install deps
pip install pandas mysql-connector-python

# point to the CSVs
export MYSQL_PASSWORD=secret  # adapt if different

python load_to_mysql.py \
  --customers /path/to/clean_customers.csv \
  --listings  /path/to/clean_listings.csv \
  --orders    /path/to/clean_orders.csv
