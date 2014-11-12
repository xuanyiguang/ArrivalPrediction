import argparse
import psycopg2 as pg
import pandas.io.sql as psql
import sys
import os


def query_dbus_data_postgres(username, password, query_string):
    """
    Query postgres database with the specified query. Login information is also needed as input.
    """
    # postgres database connection setting
    host = 'dbus.cueepsqael4s.us-west-2.rds.amazonaws.com'
    database = 'dbus'
    port = 5432

    connection = None
    try:
        connection = pg.connect(host=host, database=database, port=port, user=username, password=password)
        dataframe = psql.read_sql(query_string, connection)
        return dataframe
    except pg.DatabaseError, e:
        print 'Error: %s' % e
        sys.exit(1)
    finally:
        if connection:
            connection.close()


def prompt_for_yes_no(message, default="no"):
    """
    Prompt for yes or no, return True for yes, False for no
    """
    lookup_table = {"yes": True, "y": True, "true": True,
                    "no": False, "n": False, "false": False}

    if default == "yes":
        message_to_display = message + " [Y/n]: "
    elif default == "no":
        message_to_display = message + " [y/N]: "
    else:
        message_to_display = message + " [y/n]: "

    while True:
        user_input = raw_input(message_to_display)
        if user_input in lookup_table:
            return lookup_table[user_input]


if __name__ == "__main__":

    # config argument parser, obtain username and password from command line input
    parser = argparse.ArgumentParser(description="Query Dbus data from postgres")
    parser.add_argument("-u", "--username", dest="username", help="Username for DB", required=True)
    parser.add_argument("-p", "--password", dest="password", help="Password for DB", required=True)
    parser.add_argument("-d", "--days", dest="days", help="Number of days to download", default=1)
    args = parser.parse_args()

    number_of_days = args.days
    pathname = "../dbusdata/"
    csv_filename = "dbus_proj_location_{}days.csv".format(number_of_days)

    if os.path.isfile(pathname + csv_filename):
        flag_overwrite = prompt_for_yes_no("File already exists. Overwrite?")

    if flag_overwrite:
        # epoch time 1404079200000 (ms) ==> 6/30/2014 12am (GMT+2)
        query_string = 'SELECT device_id, "time", latitude, longitude, speed, bearing, accuracy, ' \
                       'postmile, trip_id, driver_id, bus_id, dt, delay ' \
                       'FROM projected_location ' \
                       'WHERE time >= 1404079200000::bigint and time < 1404079200000::bigint + {}*24*3600000::bigint ' \
                       'ORDER BY device_id, time '.format(number_of_days)

        df = query_dbus_data_postgres(args.username, args.password, query_string)
        print "Query returns {} entries of data".format(len(df))

        df.to_csv(pathname + csv_filename, index=False)
