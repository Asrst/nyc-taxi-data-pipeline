FROM python:3.10.6

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2-binary==2.9.5

WORKDIR /app
COPY ingest_data.py ingest_data.py 

ENTRYPOINT [ "python"]