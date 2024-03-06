FROM python:3.11 AS builder

WORKDIR /app

COPY requirements.txt /app

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

COPY . /app

ENTRYPOINT ["python"]
CMD ["app.py"]

