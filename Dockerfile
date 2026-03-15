FROM python:3.11-alpine AS base

WORKDIR /usr/src/app

COPY ./main.py .
COPY ./requirements.txt .

RUN pip install -r requirements.txt

FROM base AS final

ENV RABBIT_HOST=rabbit
ENV RABBIT_PORT=5672

ENV REDIS_HOST=redis
ENV REDIS_PORT=6379

ENTRYPOINT [ "python3", "main.py" ]
