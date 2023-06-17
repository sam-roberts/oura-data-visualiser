# Oura data visualiser
This is a Python project that allows you to pull your data from the Oura API using a personal access token, save it to a local PostgreSQL database, then use tools like Grafana to revisualise it.

## Limitations and warnings
This tool was created for PERSONAL USE and such is not intended as a production-quality community tool. Use at your own risk. In my configuration, all necessary programs are running under a Rapsberry Pi running . The tool should work on other platforms such as MacOS, Windows, other Linux distriubtions. Support for other platforms will not be provided at this stage.

## Installation

### Prerequisites

- Python 3.x: The project requires Python 3.x to be installed. If Python is not installed, you can download it from the official Python website: https://www.python.org/downloads/
- PostGreSQL: The project attempts to connect to a Postgres database to store its data. https://www.postgresql.org/download/


### Clone the Repository

1. Open a terminal.
2. Change to the directory where you want to clone the repository.
3. Run the following command to clone the repository:

    ```
    git clone https://github.com/sam-roberts/Oura-data-visualiser.git
    ```

### Set up a Virtual Environment (Optional)

Setting up a virtual environment is recommended to keep the project dependencies isolated. Follow these steps to set up a virtual environment:

1. Change to the project directory:

    ```
    cd your-repository
    ```

2. Create a virtual environment:

    ```
    python3 -m venv venv
    ```

3. Activate the virtual environment:

    ```
    source venv/bin/activate
    ```

### Install Dependencies

1. Ensure that the virtual environment is activated (if you set up one).
2. Install the project dependencies by running the following command:

    ```
    pip install -r requirements.txt
    ```

### Install Postgres (linux)
For proper documentation, follow instructions for your platform at: https://www.postgresql.org/download/
or
General instructions:
1. Update your package list and install postgres
    ```
    sudo apt update && sudo apt install postgresql
    ```

2. Switch to the postgres user using 
    ```
    sudo -i -u postgres
    ```
3. Access the PostgreSQL terminal by running:
    ```
    psql
    ```
4. Create the database you will use to host the data  (by default this tool calls it 'sleepdb')

    ```
    CREATE DATABASE sleepdb;
    ```

### Configuration

1. Open the `sleep-data-config.json` file in a text editor.
2. Modify the configuration settings as required (e.g., database credentials, API keys).
3. Save the file.

### Run the script to populate the database
    ```
    python3 sleep-data.py
    ```
### Set up 
The main python file will create a table for you in your specified database, and set up the fields for it automatically. 
    ```
    python3 sleep-data.py
    ```

### Installing Grafana
Grafana allows your to visualise data from a range of sources. Follow their guide at: https://grafana.com/docs/grafana/latest/setup-grafana/installation/

### Connecting Grafana to your Postgres database
Follow this guide for connecting grafana to your postgres server
https://grafana.com/docs/grafana/latest/datasources/postgres/

### Example of Query to show scores in Grafana
Create a new Visualization on your main Grafana dashboard. Set the data source to PostgreSQL. 
Under the 'code' query builder, you can now pull out the fields you need.

    ```
    SELECT score as "Sleep Score", date FROM sleep_sessions WHERE $__timeFilter(date)
    ```

## Usage
1. Ensure that you have followed setup instructions (above)
2. Run the Python script by executing the following command:

    ```
    python3 sleep-data.py
    ```

## Setting up the script to run automatically
You can use a tool like cron to run the script every day.

## Upcoming improvements
- Add installation shell script to speed up installation.
- Look at Docker container version of tool.
- Pull in more information from Oura. At the moment only sleep data is pulled. 