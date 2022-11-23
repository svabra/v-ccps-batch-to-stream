FROM python:3.9.13-slim

WORKDIR /app

# dont write pyc files
ENV PYTHONDONTWRITEBYTECODE 1
# dont buffer to stdout/stderr
ENV PYTHONUNBUFFERED 1

COPY ./requirements.txt /app/requirements.txt
RUN mkdir /app/storage
RUN mkdir /app/logs

# dependencies
#RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY ./src /app/src

WORKDIR /app

#CAUTION, the --reset flag will automatically read again from the beginning for this consumer group. Remove it for production.
#CMD ["python", "src/main.py", "storage/config.ini"]
CMD ["python", "src/main.py", "storage/config.ini", "--reset"]


