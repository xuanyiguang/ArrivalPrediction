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
    """
    Return True if file does not exist, False if file exists
    """
    return not os.path.isfile(filename)


def overwrite_file(filename):
    """
    Return True if user indicate to overwrite the existing file, False otherwise
    """
    return prompt_for_yes_no("File {} already exists. Overwrite?".format(filename))


def construct_query_proj_locations(starttime_ms, number_of_days):
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


def construct_query_stops(starttime_ms):
    """
    Construct query string for DBus stops data
    given start time (in milliseconds)
    """
    query_string = "SELECT DISTINCT t.shape_id, st.stop_id, st.stop_sequence, st.shape_dist_traveled, " \
                   "s.stop_lat, s.stop_lon " \
                   "FROM gtfs_stop_times_history st " \
                   "JOIN gtfs_trips_history t ON t.trip_id = st.trip_id " \
                   "JOIN gtfs_stops_history s ON s.stop_id = st.stop_id " \
                   "WHERE st.t_range @> to_timestamp('{starttime_s}') " \
                   "and t.t_range @> to_timestamp('{starttime_s}') " \
                   "and s.t_range @> to_timestamp('{starttime_s}') " \
                   "ORDER BY t.shape_id, st.stop_sequence " \
                   .format(starttime_s=starttime_ms/1000)
    return query_string


def construct_query_events(starttime_ms, number_of_days):
    """
    Construct query string for DBus event data
    given start time (in milliseconds) and number of days
    """
    query_string = "SELECT e.type, e.stop_id, e.stop_sequence, e.stop_postmile, e.time, e.trip_id, t.shape_id " \
                   "FROM event e " \
                   "JOIN gtfs_trips_history t ON t.trip_id = e.trip_id " \
                   "JOIN gtfs_routes_history r on t.route_id = r.route_id " \
                   "WHERE e.time >= {starttime_ms}::bigint " \
                   "and e.time < {starttime_ms}::bigint + {days}*24*3600000::bigint " \
                   "and t.t_range @> to_timestamp('{starttime_s}') " \
                   "and  r.t_range @> to_timestamp('{starttime_s}') " \
                   "ORDER BY trip_id, time " \
                   .format(starttime_ms=starttime_ms, starttime_s=starttime_ms/1000, days=number_of_days)
    return query_string


def construct_query_stop_times(starttime_ms):
    """
    Construct query string for DBus stop times data
    given start time (in milliseconds)
    """
    query_string = "SELECT trip_id, stop_id, stop_sequence, arrival_time " \
                   "FROM gtfs_stop_times_history " \
                   "WHERE t_range @> to_timestamp('{starttime_s}') " \
                   "ORDER BY trip_id, stop_sequence " \
                   .format(starttime_s=starttime_ms/1000)
    return query_string


def construct_filename(datatype, starttime_ms=1404079200000, number_of_days=35):
    """
    Unified file naming
    """
    pathname = "../dbusdata/"
    if datatype == "proj_locations" or datatype == "events":
        return "{pathname}dbus_{datatype}_{starttime_s}_{days}days.csv"\
            .format(pathname=pathname, datatype=datatype, starttime_s=starttime_ms/1000, days=number_of_days)
    elif datatype == "stops" or datatype == "stop_times":
        return "{pathname}dbus_{datatype}_{starttime_s}.csv"\
            .format(pathname=pathname, datatype=datatype, starttime_s=starttime_ms/1000)


if __name__ == "__main__":

    # config argument parser, obtain username and password from command line input
    parser = argparse.ArgumentParser(description="Query Dbus data from postgres")
    parser.add_argument("-u", "--username", dest="username", help="Username for DB", required=True)
    parser.add_argument("-p", "--password", dest="password", help="Password for DB", required=True)
    parser.add_argument("-t", "--datatype", choices=["proj_locations", "stops", "events", "stop_times"],
                        help="The type of data to download")
    parser.add_argument("-d", "--days", dest="days", type=int, help="Number of days to download", default=1)
    parser.add_argument("-st", "--starttime", dest="starttime_ms", type=long,
                        help="Epoch time (milliseconds) to start from", default=1404079200000)
    # default epoch time 1404079200000 (ms) ==> 6/30/2014 Monday 12am (GMT+2)

    args = parser.parse_args()

    starttime_ms = args.starttime_ms
    number_of_days = args.days
    csv_filename = construct_filename(datatype=args.datatype, starttime_ms=starttime_ms, number_of_days=number_of_days)

    if file_does_not_exist(csv_filename) or overwrite_file(csv_filename):
        # prepare query_string
        if args.datatype == "proj_locations":
            query_string = construct_query_proj_locations(starttime_ms=starttime_ms, number_of_days=number_of_days)
        elif args.datatype == "stops":
            query_string = construct_query_stops(starttime_ms=starttime_ms)
        elif args.datatype == "events":
            query_string = construct_query_events(starttime_ms=starttime_ms, number_of_days=number_of_days)
        elif args.datatype == "stop_times":
            query_string = construct_query_stop_times(starttime_ms=starttime_ms)
        else:
            print "Should not happen because of argument parser setting"

        # query
        df = query_dbus_data_postgres(args.username, args.password, query_string)
        print "Query returns {} entries of data".format(len(df))

        # save
        df.to_csv(csv_filename, index=False)
