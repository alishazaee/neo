FROM python:3.10
# Fix python printing
ARG DIR_PATH
ENV PYTHONUNBUFFERED 1
RUN mkdir /app && mkdir -p $DIR_PATH
# Installing all python dependencies
ADD Ingestor /app
RUN pip install -r /app/requirements.txt

