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


def file_does_not_exist(filename):
    return not os.path.isfile(filename)


def overwrite_file(filename):
    return prompt_for_yes_no("File {} already exists. Overwrite?".format(filename))


def construct_query_proj_location(starttime_ms, number_of_days):
    """
    Construct query string for DBus projected location data
    given start time (in milliseconds) and number of days
    """
    query_string = "SELECT p.device_id, p.time, p.latitude, p.longitude, p.speed, p.bearing, p.accuracy, " \
                   "p.postmile, p.trip_id, t.shape_id, r.route_short_name, p.driver_id, p.bus_id, p.dt, p.delay " \
                   "FROM projected_location p " \
                   "JOIN gtfs_trips_history t on t.trip_id = p.trip_id " \
                   "JOIN gtfs_routes_history r on t.route_id = r.route_id " \
                   "WHERE p.time >= {starttime_ms}::bigint " \
                   "and p.time < {starttime_ms}::bigint + {days}*24*3600000::bigint " \
                   "and r.t_range @> to_timestamp('{starttime_s}') " \
                   "and t.t_range @> to_timestamp('{starttime_s}') " \
                   "ORDER BY p.trip_id, p.time " \
                   .format(starttime_ms=starttime_ms, starttime_s=starttime_ms/1000, days=number_of_days)
    return query_string


def construct_query_stop_sequence(starttime_ms):
    """
    Construct query string for DBus stop sequence data
    given start time (in milliseconds)
    """
    query_string = "SELECT DISTINCT t.shape_id, st.stop_id, st.stop_sequence, st.shape_dist_traveled " \
                   "FROM gtfs_stop_times_history st " \
                   "JOIN gtfs_trips_history t ON t.trip_id = st.trip_id " \
                   "WHERE st.t_range @> to_timestamp('{starttime_s}') " \
                   "and t.t_range @> to_timestamp('{starttime_s}') " \
                   "ORDER BY t.shape_id, st.stop_sequence " \
                   .format(starttime_s=starttime_ms/1000)
    return query_string


if __name__ == "__main__":

    # config argument parser, obtain username and password from command line input
    parser = argparse.ArgumentParser(description="Query Dbus data from postgres")
    parser.add_argument("-u", "--username", dest="username", help="Username for DB", required=True)
    parser.add_argument("-p", "--password", dest="password", help="Password for DB", required=True)
    parser.add_argument("-d", "--days", dest="days", type=int, help="Number of days to download", default=35)
    parser.add_argument("-st", "--starttime", dest="starttime_ms", type=long,
                        help="Epoch time (milliseconds) to start from", default=1404079200000)
    # default epoch time 1404079200000 (ms) ==> 6/30/2014 Monday 12am (GMT+2)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-pl", "--projected_location", action="store_true", help="Download projected location data")
    group.add_argument("-ss", "--stop_sequence", action="store_true", help="Download stop sequence data")
    group.add_argument("-e", "--event", action="store_true", help="Download event data")

    args = parser.parse_args()

    starttime_ms = args.starttime_ms
    number_of_days = args.days
    pathname = "../dbusdata/"
    # set filename
    if args.projected_location:
        csv_filename = "dbus_proj_location_{starttime_s}_{days}days.csv"\
            .format(starttime_s=starttime_ms/1000, days=number_of_days)
    elif args.stop_sequence:
        csv_filename = "dbus_stop_sequence_{starttime_s}.csv"\
            .format(starttime_s=starttime_ms/1000)
    elif args.event:
        csv_filename = "dbus_event_{starttime_s}_{days}days.csv"\
            .format(starttime_s=starttime_ms/1000, days=number_of_days)

    if file_does_not_exist(pathname + csv_filename) or overwrite_file(pathname + csv_filename):
        # prepare query_string
        if args.projected_location:
            query_string = construct_query_proj_location(starttime_ms, number_of_days)
        elif args.stop_sequence:
            query_string = construct_query_stop_sequence(starttime_ms)
        elif args.event:
            pass

        # query
        df = query_dbus_data_postgres(args.username, args.password, query_string)
        print "Query returns {} entries of data".format(len(df))

        # save
        df.to_csv(pathname + csv_filename, index=False)
