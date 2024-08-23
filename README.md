# kedro-deltalake-demo

## Getting started

Spin up MinIO:

```
$ docker compose up -d
```

Create a `data` bucket:

```
$ mc alias set myminio http://127.0.0.1:9010 minioadmin minioadmin
$ mc mb myminio/data
```

Install Python dependencies:

```
$ uv sync
```
