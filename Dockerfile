FROM python:3.10.6

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_trips.py ingest_trips.py 
COPY ingest_zones.py ingest_zones.py 

ENTRYPOINT [ "python"]