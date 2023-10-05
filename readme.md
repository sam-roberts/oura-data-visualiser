# Oura sleep data visualiser

This is a Python project that allows you to pull your data from the Oura API using a personal access token, save it to a local PostgreSQL database, then use tools like Grafana to revisualise it. Currently the data being scraped is:

- Sleep score including score breakdown (e.g. efficiency, latency, etc)
- Sleep durations for:
  - Total time in bed duration
  - Total sleep duration
  - REM sleep duration
  - Deep sleep duration
 

Although last change was made approx 3 months ago - script should be working as of this edit, October 2023. 

![Example of visualising your sleep data with this tool](/app-preview.jpg)

## Limitations and warnings

This tool was created for PERSONAL USE and such is not intended as a production-quality community tool. Use at your own risk. In my configuration, all necessary programs are running under a Rapsberry Pi running . The tool should work on other platforms such as MacOS, Windows, other Linux distriubtions. Support for other platforms will not be provided at this stage.

## Installation

### Prerequisites

- Python 3.x: The project requires Python 3.x to be installed. If Python is not installed, you can download it from the official Python website: <https://www.python.org/downloads/>
- PostGreSQL: The project attempts to connect to a Postgres database to store its data. <https://www.postgresql.org/download/>

### Install Postgres (linux) - Other database types now supported, but not tested

Installing Postgres is recommended first.
For proper documentation, follow instructions for your platform at: <https://www.postgresql.org/download/> (or consider using something like brew on macOS)
or
General instructions:

1. Update your package list and install postgres

    ```bash
    sudo apt update && sudo apt install postgresql
    ```

2. Switch to the postgres user using

    ```bash
    sudo -i -u postgres
    ```

3. Access the PostgreSQL terminal by running:

    ```bash
    psql
    ```

4. Create the database you will use to host the data  (by default this tool calls it 'sleepdb')

    ```sql
    CREATE DATABASE sleepdb;
    ```

### Set up a Virtual Environment (Optional)

Setting up a virtual environment is recommended to keep the project dependencies isolated. Follow these steps to set up a virtual environment. Note on MacOS you may need to follow some of the steps described in <https://stackoverflow.com/questions/34304833/failed-building-wheel-for-psycopg2-macosx-using-virtualenv-and-pip>

1. Change to the project directory:

    ```bash
    cd your-repository
    ```

2. Create a virtual environment:

    ```bash
    python3 -m venv venv
    ```

3. Activate the virtual environment:

    ```bash
    source venv/bin/activate
    ```

### Install Dependencies

1. Ensure that the virtual environment is activated (if you set up one).
2. Install the project dependencies by running the following command:

    ```bash
    pip install -r requirements.txt
    ```

### Configuration

1. Open the `config-template.ini` file in a text editor.
2. Fill out all configurations in the db and user sections
3. Save the file.
4. Set an environment variable to tell your operating system where to find this file. (Add to ~/.bashrc to not need to do this every time program is run)

```bash
export OURA_SLEEP_CONFIG_PATH="e.g. PATH/TO/YOUR/CONFIG-FILE.ini"
```

### Set up

As long as a database exists as defined in your config file, the program will create the necessary table for you.

```bash
python3 sleep-data.py
```

### Installing Grafana

Grafana allows your to visualise data from a range of sources. Follow their guide at: <https://grafana.com/docs/grafana/latest/setup-grafana/installation/>

### Connecting Grafana to your Postgres database

Follow this guide for connecting grafana to your postgres server
<https://grafana.com/docs/grafana/latest/datasources/postgres/>

### Example of Query to show scores in Grafana

Create a new Visualization on your main Grafana dashboard. Set the data source to PostgreSQL.
Under the 'code' query builder, you can now pull out the fields you need.

```sql
SELECT score as "Sleep Score", date FROM sleep_sessions WHERE $__timeFilter(date)
```

## Usage

1. Ensure that you have followed setup instructions (above)
2. Run the Python script by executing the following command:

```bash
python3 sleep-data.py
```

## Major changes released 19/7/23

1. Migrated from psycopg2 to sqlalchemy to allow for more generic db handling
2. Added support for pulling in sleep durations
3. Various code refactoring and readability improvements

## Setting up the script to run automatically

You can use a tool like cron to run the script every day.

## Upcoming improvements

- Add installation shell script to speed up installation.
- Look at Docker container version of tool.
- Pull in more information from Oura. At the moment only sleep data is pulled.

## Known issues

- checkConfig() fails if no password is set, which is a valid configuration on postgres.
