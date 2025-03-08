# built-in libraries
import configparser
import logging
import os
from datetime import date, datetime, timedelta
from http import HTTPStatus
from typing import Dict, List, Optional

# third-party libraries
import requests
from sqlalchemy import Column, Date, Engine, Integer, MetaData, Table, create_engine


def getResponseFromAPI(
    API_URl: str, PERSONAL_TOKEN: str, myParams: Dict
) -> Optional[Dict]:
    # Optional: Define headers or authentication tokens if required by the API
    headers = {
        "Authorization": f"Bearer {PERSONAL_TOKEN}",
        "Content-Type": "application/json",
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


def checkConfig(config) -> bool:
    # Check if valid configuration file
    # TODO MAKE SURE THE USER PROVIDES A VALID CONFIG FILE
    return True


def getSleepDataOnDate(sleepData, compareDate) -> List[Dict]:
    # support multiple sets of data on a day
    results = [item for item in sleepData if item["day"] == str(compareDate)]
    return results


def getSleepDataSum(additionalDayData, config: configparser.ConfigParser):
    combinedData = {
        "total_sleep_duration": 0,
        "rem_sleep_duration": 0,
        "time_in_bed": 0,
        "deep_sleep_duration": 0,
    }

    for item in additionalDayData:
        # If in config we include naps then add all sleep sessions for the day up, otherwise only take the long sleep value
        if config["user"].getboolean("include_naps"):
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


def clearAndCreateTable(engine: Engine, meta: MetaData, table: Table, config):
    with engine.connect() as connection:
        # Check if the table already exists
        if connection.dialect.has_table(connection, config["db"]["tablename"]):
            # Drop the table if it exists
            table.drop(engine)
            print(f"{config['db']['tablename']} table has been dropped.")
    table.create(engine)


def populateDb(
    engine: Engine,
    meta: MetaData,
    tableSleep: Table,
    config: configparser.ConfigParser,
    sleepData: Dict,
    moreSleepData: Dict,
    todayDate: str,
):
    # Find the range of dates to loop over
    dateFormat = "%Y-%m-%d"
    firstDate = datetime.strptime(config["user"]["start_date"], dateFormat).date()
    lastDate = datetime.strptime(todayDate, dateFormat).date()
    dateDelta = (lastDate - firstDate).days

    # List of all the days between the range
    allDays = [firstDate + timedelta(days=i) for i in range(dateDelta + 1)]
    if config["dev"].getboolean("debug_mode"):
        print("First day:", firstDate, "last day", lastDate)
        print("There should be ", len(allDays), "of data")

    # Loop over all the days, and if there's no data for that day, just fill it with zeroes
    missingDays = 0
    with engine.connect() as connection:
        for calendarDay in allDays:
            # Grab the data from our API response for that particular day
            dayData = getSleepDataOnDate(sleepData, calendarDay)
            additionalDayData = getSleepDataOnDate(moreSleepData, calendarDay)

            # Days can have multiple sleep sessions (e.g. naps), so we need to deal with those
            if len(additionalDayData) > 1 and config["dev"].getboolean("debug_mode"):
                print(calendarDay, "had", len(additionalDayData), "sleep sessions")
            aggregateDayData = getSleepDataSum(additionalDayData, config)

            # if we do have data then get correct values
            # We can assume the first elmenet of DayData because length should only be 1
            if len(dayData) > 0 and additionalDayData is not None:
                inStatment = tableSleep.insert().values(
                    date=dayData[0].get("day"),
                    score=dayData[0]["score"],
                    deep_sleep=dayData[0]["contributors"]["deep_sleep"],
                    efficiency=dayData[0]["contributors"]["efficiency"],
                    latency=dayData[0]["contributors"]["latency"],
                    rem_sleep=dayData[0]["contributors"]["rem_sleep"],
                    restfulness=dayData[0]["contributors"]["restfulness"],
                    timing=dayData[0]["contributors"]["timing"],
                    total_sleep=dayData[0]["contributors"]["total_sleep"],
                    total_sleep_duration=aggregateDayData["total_sleep_duration"],
                    rem_sleep_duration=aggregateDayData["rem_sleep_duration"],
                    time_in_bed=aggregateDayData["time_in_bed"],
                    deep_sleep_duration=aggregateDayData["deep_sleep_duration"],
                )
            else:
                inStatment = tableSleep.insert().values(date=calendarDay.isoformat())
                print(
                    "..." * 3, "Oura wasn't worn (or there is no data) on", calendarDay
                )
                missingDays += 1
            connection.execute(inStatment)
        connection.commit()


def getTable(config, meta):
    return Table(
        config["db"]["tablename"],
        meta,
        Column("date", Date, primary_key=True),
        Column("score", Integer),
        Column("deep_sleep", Integer),
        Column("efficiency", Integer),
        Column("latency", Integer),
        Column("rem_sleep", Integer),
        Column("restfulness", Integer),
        Column("timing", Integer),
        Column("total_sleep", Integer),
        Column("total_sleep_duration", Integer),
        Column("rem_sleep_duration", Integer),
        Column("time_in_bed", Integer),
        Column("deep_sleep_duration", Integer),
    )


def getSleepData(config, myParams):
    sleepData = getResponseFromAPI(
        config["oura"]["sleep_api_url"], config["user"]["personal_token"], myParams
    )
    sleepData = sleepData.get("data")
    return sleepData


def getMoreSleepData(config, myParams):
    moreSleepData = getResponseFromAPI(
        config["oura"]["sleep_routes_api_url"],
        config["user"]["personal_token"],
        myParams,
    )
    moreSleepData = moreSleepData.get("data")
    return moreSleepData


def setupLogging():
    logging.basicConfig(level=logging.ERROR)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.ERROR)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.ERROR)


def main():
    # Setup configuration file
    config_path = os.environ.get("OURA_SLEEP_CONFIG_PATH", "config.ini")
    config = configparser.ConfigParser()
    config.read(config_path)
    if not checkConfig(config):
        print("Configuration file is invalid, check file and try again. Exiting")
        return

    # Fetch data
    today_date = date.today().strftime("%Y-%m-%d")

    # Define start and end date we want data for
    my_params = {"start_date": config["user"]["start_date"], "end_date": today_date}

    # Get results from sleep api (e.g. overall score)
    sleep_data = getSleepData(config, my_params)

    # Get additional sleep data (e.g. rem time, deep time, in bed duration, etc)
    more_sleep_data = getMoreSleepData(config, my_params)

    # Connect to database
    engine = create_engine(
        f"{config['db']['dbtype']}://{config['db']['username']}:{config['db']['password']}@{config['db']['host']}/{config['db']['dbname']}"
    )
    meta = MetaData()
    setupLogging()

    # Create table object for sleep data
    table = getTable(config, meta)

    # Create the table in the database
    clearAndCreateTable(engine, meta, table, config)
    populateDb(engine, meta, table, config, sleep_data, more_sleep_data, today_date)


if __name__ == "__main__":
    main()
