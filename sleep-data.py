# built-in libraries
from datetime import date, datetime, timedelta
from http import HTTPStatus
from typing import Optional, Dict, List
import pprint
import configparser

# third-party libraries
import psycopg2
import requests
import sqlalchemy


sleepTableFields = """
        date DATE PRIMARY KEY,
        score INT,
        deep_sleep INT,
        efficiency INT,
        latency INT,
        rem_sleep INT,
        restfulness INT,
        timing INT,
        total_sleep INT,
        total_sleep_duration INT,
        rem_sleep_duration INT,
        time_in_bed INT,
        deep_sleep_duration INT
        """


def getResponseFromAPI(API_URl: str, PERSONAL_TOKEN: str, myParams: Dict) -> Optional[Dict]:
    # Optional: Define headers or authentication tokens if required by the API
    headers = {
        "Authorization": f"Bearer {PERSONAL_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(API_URl, headers=headers, params=myParams)

        # Check the response status code
        if response.status_code == HTTPStatus.OK:
            data = response.json()  # Get the response data in JSON format
            # Process and work with the response data as needed
            return data
        else:
            print(f"Request failed with status code: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None


def createDbConnection(dbhost, dbname, dbusername, dbpassword) -> Optional[psycopg2.extensions.connection]:
    try:
        # Connect to the PostgreSQL server
        connection = psycopg2.connect(
            host=dbhost,
            database=dbname,
            user=dbusername,
            password=dbpassword
        )
        return connection
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)
    return None


def checkConfig(config) -> bool:
    # Check if valid configuration file
    # TODO
    return True


def getSleepDataOnDate(sleepData, compareDate) -> List[Dict]:
    # support multiple sets of data on a day
    results = [item for item in sleepData if item["day"] == str(compareDate)]
    return results

def getSleepDataSum(additionalDayData, config: configparser.ConfigParser):
    # pprint.pprint(additionalDayData)
    combinedData = {
        "total_sleep_duration": 0,
        "rem_sleep_duration": 0,
        "time_in_bed": 0,
        "deep_sleep_duration": 0,
    }

    for item in additionalDayData:
        if config['user'].getboolean('include_naps'):
            combinedData["total_sleep_duration"] += item["total_sleep_duration"]
            combinedData["rem_sleep_duration"] += item["rem_sleep_duration"]
            combinedData["time_in_bed"] += item["time_in_bed"]
            combinedData["deep_sleep_duration"] += item["deep_sleep_duration"]
        else:
            if item["type"] == "long_sleep":
                combinedData["total_sleep_duration"] = item["total_sleep_duration"]
                combinedData["rem_sleep_duration"] = item["rem_sleep_duration"]
                combinedData["time_in_bed"] = item["time_in_bed"]
                combinedData["deep_sleep_duration"] = item["deep_sleep_duration"]
                return combinedData
    
    return combinedData


def populateDbSleep(sleepData, moreSleepData, connection, toDate, config: configparser.ConfigParser) -> None:
    # Create a cursor object to interact with the database
    cursor = connection.cursor()

    if config['dev'].getboolean('debug_mode') is True:
        # Get column headings
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (config['db']['tablename'],))
        columnHeadings = cursor.fetchall()
        print(f"Columns in {config['db']['tablename']}:")
        pprint.pprint(columnHeadings)
        # Print the database names
        for heading in columnHeadings:
            print(heading[0], end=", ")
        print(f"{len(columnHeadings)} columns total")

    # Find the range of dates to loop over
    dateFormat = "%Y-%m-%d"
    firstDate = datetime.strptime(config['user']['start_date'], dateFormat).date()
    lastDate = datetime.strptime(toDate, dateFormat).date()
    dateDelta = (lastDate - firstDate).days

    # List of all the days between the range
    allDays = [firstDate + timedelta(days=i) for i in range(dateDelta + 1)]
    if config['dev'].getboolean('debug_mode'):
        print("First day:", firstDate, "last day", lastDate)
        print("There should be ", len(allDays), "of data")

    print("......Filling any missing days of data")

    # Loop over all the days, and if there's no data for that day, just fill it with zeroes
    successCount = 0
    missingDays = 0
    for calendarDay in allDays:
        # Grab the data from our API response for that particular day
        dayData = getSleepDataOnDate(sleepData, calendarDay)
        additionalDayData = getSleepDataOnDate(moreSleepData, calendarDay)

        # Days can have multiple sleep sessions (e.g. naps), so we need to deal with those
        if len(additionalDayData) > 1 and config['dev'].getboolean('debug_mode'):
            print(calendarDay, "had", len(additionalDayData), "sleep sessions")
        aggregateDayData = getSleepDataSum(additionalDayData, config)

        # assume no value
        values = (calendarDay.isoformat(), "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")

        # if we do have data then get correct values
        # We can assume the first elmenet of DayData because length should only be 1
        if len(dayData) > 0 and additionalDayData is not None:
            values = (
                str(dayData[0].get("day")),
                dayData[0]["score"],
                dayData[0]["contributors"]["deep_sleep"],
                dayData[0]["contributors"]["efficiency"],
                dayData[0]["contributors"]["latency"],
                dayData[0]["contributors"]["rem_sleep"],
                dayData[0]["contributors"]["restfulness"],
                dayData[0]["contributors"]["timing"],
                dayData[0]["contributors"]["total_sleep"],
                aggregateDayData["total_sleep_duration"],
                aggregateDayData["rem_sleep_duration"],
                aggregateDayData["time_in_bed"],
                aggregateDayData["deep_sleep_duration"]
            )
        else:
            print("..." * 3, "Oura wasn't worn (or there is no data) on", calendarDay)
            missingDays += 1

        # Loop over the values and construct the INSERT statement
        query = f"INSERT INTO {config['db']['tablename']} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"

        try:
            cursor.execute(query, (values))
            successCount += 1
        except psycopg2.Error as e:
            print(f"Error executing query: {e}")

    connection.commit()
    cursor.close()
    print(f"...Successfully added (or ignored existing) {successCount} rows to database. ({missingDays}) day(s) of data was missing")


def checkTableExists(connection, dbtable):
    cursor = connection.cursor()
    query = """
        SELECT EXISTS (
            SELECT 1
            FROM pg_tables
            WHERE tablename = %s
        );
    """
    cursor.execute(query, (dbtable,))
    result = cursor.fetchone()[0]
    cursor.close()
    return result


def createDbTable(connection, dbtable, dbfields):
    cursor = connection.cursor()
    create_table_query = f"""
    CREATE TABLE {dbtable} (
        {dbfields}
    );
    """
    try:
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        return True
    except psycopg2.Error as e:
        print(f"Error executing query: {e}")
        cursor.close()
        return False

def dropTable(connection, tableName) -> None:
    cursor = connection.cursor()
    cursor.execute(f"DROP TABLE {tableName}")
    cursor.close()

def setupDb(connection, config: configparser.ConfigParser):
    if config['dev'].getboolean('clear_tables'):
        dropTable(connection, config['db']['tablename'])
        print("...Deleting table [start fresh is true]")
    # Set up table in DB if needed
    hasTable = checkTableExists(connection, config['db']['tablename'])
    if hasTable is False:
        print("...Table", config['db']['tablename'], "doesn't exist. Attempting to create...")
        isCreateSuccessful = createDbTable(connection,
                                           config['db']['tablename'],
                                           sleepTableFields)
        if isCreateSuccessful is False:
            print("Unable to create table", config['db']['tablename'], ". Exiting")
            return

    print(f"...Applying Oura data to database (name={config['db']['dbname']}, table={config['db']['tablename']})")

def main():
    # Setup configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')
    if not checkConfig(config):
        print("Configuration file is invalid, check file and try again. Exiting")
        return

    # Fetch data
    todayDate = date.today().strftime("%Y-%m-%d")

    # Define start and end date we want data for
    myParams = {
        "start_date": config['user']['start_date'],
        "end_date": todayDate
    }

    # Get results from sleep api (e.g. overall score)
    sleepData = getResponseFromAPI(config['oura']['sleep_api_url'], config['user']['personal_token'], myParams)
    sleepData = sleepData.get("data")

    # Get additional sleep data (e.g. rem time, deep time, in bed duration, etc)
    moreSleepData = getResponseFromAPI(config['oura']['sleep_routes_api_url'], config['user']['personal_token'], myParams)
    moreSleepData = moreSleepData.get("data")

    # Connect to DB
    connection = createDbConnection(config['db']['host'],
                                    config['db']['dbname'],
                                    config['db']['username'],
                                    config['db']['password'])
    if connection is None:
        print("Unable to create a connection to database, exiting")
        return
    print(f"...Successfully connected to database: (host={config['db']['host']}, user={config['db']['username']})")

    # Clear the table
    setupDb(connection, config)

    # Put API data to DB
    populateDbSleep(sleepData, moreSleepData, connection, todayDate, config)

    # Close the connection and wrap up
    connection.close()


if __name__ == "__main__":
    main()
