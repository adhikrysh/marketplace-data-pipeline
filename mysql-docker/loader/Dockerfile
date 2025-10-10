FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir mysql-connector-python pandas

COPY load_to_mysql.py /app/load_to_mysql.py

ENV DATA_DIR=/data
ENV DB_HOST=db
ENV DB_PORT=3306
ENV DB_USER=user
ENV DB_PASS=password
ENV DB_NAME=marketplace

ENTRYPOINT ["python", "/app/load_to_mysql.py"]