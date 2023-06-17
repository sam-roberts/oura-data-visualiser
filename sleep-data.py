import requests
from datetime import date, timedelta,datetime
import psycopg2
import json

debug_mode = False
github_mode = True


sleepTableFields= """
        date DATE PRIMARY KEY,
        score INT,
        deep_sleep INT,
        efficiency INT,
        latency INT,
        rem_sleep INT,
        restfulness INT,
        timing INT,
        total_sleep INT
        """

def loadConfig():
    path='sleep-data-config.json'
    if github_mode is False:
        path='sleep-data-config-private.json'
    with open(path) as config_file:
        config = json.load(config_file)
    return config


def generateConfigVariables(configJson):
    return {
        "DBHOST": configJson.get("db-host"),
        "DBNAME": configJson.get("db-dbname"),
        "DBUSERNAME":configJson.get("db-username"),
        "DBPASSWORD":configJson.get("db-password"),
        "DBTABLENAME":configJson.get("db-tablename"),
        #Oura configuration
        "OURA_PERSONAL_TOKEN":configJson.get("oura-token"),
        "OURA_SLEEP_API_URL":"https://api.ouraring.com/v2/usercollection/daily_sleep",
        "OURA_FROM_DATE":configJson.get("oura-from-date")
    }


def getSleepDataFromOura(API_URl, PERSONAL_TOKEN, fromDate,toDate):
    # Optional: Define headers or authentication tokens if required by the API
    headers = {
        "Authorization": f"Bearer {PERSONAL_TOKEN}",
        "Content-Type": "application/json"
    }
    params = {
        "start_date": fromDate,
        "end_date": toDate
    }

    try:
        response = requests.get(API_URl, headers=headers, params=params)  # Make a GET request, replace with the appropriate HTTP method

        # Check the response status code
        if response.status_code == 200:  # Replace 200 with the expected status code for a successful response
            data = response.json()  # Get the response data in JSON format
            # Process and work with the response data as needed

            sleepData = data["data"]
            print ("...Gathered",len(data["data"]), "nights of sleep data from Oura API")
            return sleepData
        else:
            print(f"Request failed with status code: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None


def createDbConnection(dbhost, dbname, dbusername, dbpassword):
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

def checkConfig(config):
    # Check if any value is null or empty
    if any(value is None or value == "" for value in config.values()):
        return False
    return True


def getSleepDataOnDate(sleepData, compareDate):
    for day in sleepData:
        if str(day["day"]) == str(compareDate):
            return day
    return None

def populateDbSleep(sleepData, connection, dbtable, fromDate,toDate):
        # Create a cursor object to interact with the database
    cursor = connection.cursor()

    if debug_mode is True:
        # Get column headings
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (dbtable,))
        columnHeadings = cursor.fetchall()
        print(f"Columns in {dbtable}:")
        # Print the database names
        for heading in columnHeadings:
            print(heading[0], end=", ")
        print(f"{len(columnHeadings)} columns total")

    dateFormat = "%Y-%m-%d"
    firstDate = datetime.strptime(fromDate,dateFormat).date()
    lastDate = datetime.strptime(toDate,dateFormat).date()
    dateDelta = (lastDate - firstDate).days

    allDays = [firstDate + timedelta(days=i) for i in range(dateDelta + 1)]
    if debug_mode is True:
        print("first day:", firstDate, "last day",lastDate)
        print("There should be ",len(allDays),"of data")

    print("......Filling any missing days of data")

    successCount=0
    missingDays=0
    for calendarDay in allDays:
        dayData = getSleepDataOnDate(sleepData,calendarDay)
       
        #assume no value
        values = (calendarDay,0,0,0,0,0,0,0,0)
        
        #if we do have data then get correct values
        if dayData is not None:
            values = (
                dayData["day"], 
                dayData["score"], 
                dayData["contributors"]["deep_sleep"], 
                dayData["contributors"]["efficiency"],
                dayData["contributors"]["latency"], 
                dayData["contributors"]["rem_sleep"], 
                dayData["contributors"]["restfulness"], 
                dayData["contributors"]["timing"], 
                dayData["contributors"]["total_sleep"]
            )
        else:
            print("..."*3,"Oura wasn't worn (or there is no data) on",calendarDay)
            missingDays += 1

        # Loop over the values and construct the INSERT statement
        query = f"INSERT INTO {dbtable} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"
        
        try:
            cursor.execute(query,(values))
            successCount +=1
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

def main():
    configJson = loadConfig()
    if not checkConfig(configJson):
        print("Configuration file is missing a value, check file and try again. Exiting")
        return
    config = generateConfigVariables(configJson)
    print("...Loaded config successfully")

    todayDate = date.today().strftime("%Y-%m-%d")
    sleepData = getSleepDataFromOura(config['OURA_SLEEP_API_URL'], config['OURA_PERSONAL_TOKEN'], config['OURA_FROM_DATE'],todayDate)

    connection = createDbConnection(config['DBHOST'], config['DBNAME'],config['DBUSERNAME'],config['DBPASSWORD'])
    if connection is None:
        print("Unable to create a connection to database, exiting")
        return
    print(f"...Successfully connected to database: (host={config['DBHOST']}, user={config['DBUSERNAME']})")

    hasTable = checkTableExists(connection, config['DBTABLENAME'])
    if hasTable is False:
        print("...Table",config['DBTABLENAME'],"doesn't exist. Attempting to create...")
        isCreateSuccessful = createDbTable(connection, config['DBTABLENAME'],sleepTableFields)
        if isCreateSuccessful is False:
            print("Unable to create table",config['DBTABLENAME'],". Exiting")
            return
    print(f"...Applying Oura data to database (name={config['DBNAME']}, table={config['DBTABLENAME']})")
    populateDbSleep(sleepData, connection, config['DBTABLENAME'], config['OURA_FROM_DATE'], todayDate)
    connection.close()

    
if __name__ == "__main__":
    main()


