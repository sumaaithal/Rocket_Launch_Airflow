# Rocket_Launch_Airflow

An Apache Airflow DAG that downloads upcoming rocket launch data and associated images from The Space Devs API.

## Overview

This project automates the process of fetching upcoming rocket launch information and downloading their promotional images. It uses Airflow to orchestrate three main tasks:

1. **Download Launches** - Fetches upcoming launch data from The Space Devs API
2. **Get Pictures** - Downloads all launch images locally
3. **Notify** - Reports the total number of images downloaded

## Prerequisites

- Python 3.8+
- Apache Airflow
- requests library
- pendulum library

## Installation

1. Clone the repository:
```bash
git clone https://github.com/sumaaithal/Rocket_Launch_Airflow.git
cd Rocket_Launch_Airflow
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate
```

3. Install dependencies:
```bash
pip install apache-airflow requests pendulum
```

## Usage

1. Place `rocket_launch.py` in your Airflow DAGs folder (default: `~/airflow/dags/`)

2. Start the Airflow webserver:
```bash
airflow webserver
```

3. In another terminal, start the scheduler:
```bash
airflow scheduler
```

4. Access the Airflow UI at `http://localhost:8080`

5. Enable the `download_rocket_launches` DAG from the UI

## DAG Details

- **DAG ID**: `download_rocket_launches`
- **Schedule**: Manual trigger (no schedule)
- **Start Date**: 14 days ago (UTC)

### Tasks

- **download_launches**: Downloads launch data to `/tmp/launches.json`
- **get_pictures**: Extracts image URLs and downloads them to `/tmp/images/`
- **notify**: Displays the total count of downloaded images

## Data Source

Data is fetched from [The Space Devs API](https://thespacedevs.com/) - a free, open-source API for space data.

## Output

- Launch data: `/tmp/launches.json`
- Images: `/tmp/images/`

## License

MIT

