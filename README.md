
# Air Quality Monitoring Platform


## Project Overview

This platform is designed to monitor and analyze air quality data, utilizing a scalable data processing pipeline that integrates Python, Docker, Databricks, Spark, and BigQuery. It features data collection from the OpenWeather API, processing with Databricks and Spark, storage in PostgreSQL, and deployment using Docker on the Google Cloud Platform.

## Key Features

- **Data Ingestion:** Fetch air pollution data for specified cities using the OpenWeather API.
- **Data Processing:** Utilize Spark for processing and analyzing air quality data.
- **Data Storage:** Store processed data in PostgreSQL for persistent storage and easy retrieval.
- **Metadata Addition:** Enrich air quality data with metadata such as timestamps and city information.
- **Cloud Integration:** Deploy the application on Google Cloud Platform using Docker, orchestrated with GitHub Actions for continuous integration and deployment.

## Technologies Used

- Python for scripting and backend logic.
- Docker for creating isolated environments and ensuring consistency across development, testing, and production.
- Databricks and Spark for scalable data processing and analytics.
- GitHub Actions for automating workflows and CI/CD pipeline.
- BigQuery for handling large-scale data analysis.

## Configuration

### `config.yaml`

```yaml
cities:
  - name: Warsaw
    country: PL
  - name: Krakow
    country: PL
  - name: Wroclaw
    country: PL
  - name: Gdansk
    country: PL

date_range:
  start_date: '2023-01-01'
  end_date: '2023-12-31'

output:
  metadata: true
  file_format: json

logging:
  level: INFO
```

This configuration file specifies the cities for which air quality data will be fetched, the date range for the data, and output preferences.

## Installation and Setup

1. **Clone the repository:**

```bash
git clone https://github.com/kurekhombre/AirPollution.git
cd airpollution
```

2. **Build Docker Image:**

```bash
docker build -t airpollution .
```

3. **Run the Docker container:**

```bash
docker run -d --name aqm-container airpollution
```

4. **Set up environmental variables:**

Ensure that the `OPENWEATHER_API_KEY` is set in your environment to authenticate API requests.

## Usage

Deploy the platform on the Google Cloud Platform using the provided Docker configuration. The system will automatically start fetching and processing data according to the schedule defined in `config.yaml`.

