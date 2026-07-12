# Movies ETL demo
This project demonstrates a production-inspired data engineering
pipeline. Data is exposed through a REST API, extracted by a Python ETL
application, transformed, and loaded into PostgreSQL.

## Disclaimer
This project is intended for educational and portfolio purposes. Commercial use is not the intended goal.

## Context
This project was first created as a technical challenge for a senior data engineer hiring process. The challenge was as follow:
> You have access to a data source that exposes the following API: 
>
> * POST - /auth
> * GET - /art/v3/genres
> * GET - /art/v3/genres/{genreId}/movies
> * GET - /art/v3/movies/{movieId}
> * GET - /art/v3/movies/{movieId}/ratings
>
> Inferring the information each endpoint contains:
> 1. Create a Postgres table that will contain the main information from these endpoints;
> 2. Write an ETL in Python that will populate this schema.

## Architecture

``` text
Movies API
    |
Python ETL
    |
PostgreSQL
```

## Project Structure

``` text
.
├── api/
├── etl/
├── docker-compose.yml
├── .env.template
└── README.md
```

## Technology Stack

-   Python
-   Flask
-   PostgreSQL
-   SQLAlchemy
-   Docker Compose
-   uv

## Pipeline Overview

1.  Authenticate with the API.
2.  Download movie data.
3.  Transform the data.
4.  Load it into PostgreSQL.

## Getting Started

### Prerequisites

-   Docker
-   Docker Compose

### Run

``` bash
cp .env.template .env
docker compose up --build
```

## Running the Pipeline

``` bash
docker compose run --rm etl
```

## Adding New Code

-   Add new API endpoints in `api/`.
-   Add extraction and transformation logic in `etl/`.
-   Add new database tables and loaders as needed.
-   Store configuration in `.env`.

## Next Steps

### Code Quality

-   Unit tests
-   Integration tests
-   Better logging
-   Improved error handling

### CI/CD

-   Automated testing
-   Docker image builds
-   Deployment pipeline

### Orchestration

-   Airflow DAG
-   Scheduling
-   Retries
-   Monitoring

### Cloud

-   S3 data lake
-   dbt transformations
-   Trino or BigQuery

### Performance

-   Parallel API requests
-   Batch inserts
-   Incremental loading

## Acknowledgements

-   Dataset source
-   API inspiration
-   Useful references