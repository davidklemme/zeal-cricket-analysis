FROM python:3.8-slim-buster
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
ADD app/sql/db_schema.sql sql/db_schema.sql
COPY . .
CMD ["python3", "./app/app.py"]


