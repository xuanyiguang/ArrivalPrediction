import pandas as pd
import matplotlib.pyplot as plt
from src.query_dbus_data_postgres import construct_filename

# TODO: add plots for stop location
def plot_by_shape_id(proj_locations):
    unique_shape_id = proj_locations['shape_id'].unique()
    for shape_id in unique_shape_id:
        plt.figure()
        proj_locations_this_shape_id = proj_locations[proj_locations['shape_id'] == shape_id]
        unique_trip_id = proj_locations_this_shape_id['trip_id'].unique()
        for trip_id in unique_trip_id:
            d = proj_locations_this_shape_id[proj_locations_this_shape_id['trip_id'] == trip_id]
            plt.subplot(1, 2, 1)
            plt.title("Postmile vs. Time for shape_id: {}".format(shape_id))
            plt.plot(d['time'], d['postmile'])
            plt.subplot(1, 2, 2)
            plt.title("Longitude vs. Latitude for shape_id: {}".format(shape_id))
            plt.plot(d['longitude'], d['latitude'], 'bo-')
    plt.show()


def load_proj_location_data(starttime_ms, number_of_days):
    """
    Load projected location data and check input
    """
    proj_location_filename = construct_filename(datatype="proj_location",
                                                starttime_ms=starttime_ms, number_of_days=number_of_days)
    proj_locations = pd.read_csv(proj_location_filename)
    print "Total number of rows: {}".format(len(proj_locations))

    # Input check: eliminate duplication in data (only using columns time and trip_id)
    if proj_locations.duplicated(['time', 'trip_id']).any():
        proj_locations.drop_duplicates(['time', 'trip_id'], inplace=True)
        print "Duplicates removed, now {} rows".format(len(proj_locations))

    # # plot for different shape_id
    # plot_by_shape_id(proj_locations)

    return proj_locations


def match_location_pairs(proj_locations):
    """
    Match consecutive location pairs to get travel time and distance
    """
    # copy a subset of fields for future use
    matched_locations = proj_locations[['time', 'postmile', 'trip_id', 'shape_id', 'route_short_name']]

    # use diff to calculate travel_time and distance between adjacent rows
    matched_locations['travel_time'] = matched_locations['time'].diff()
    matched_locations['distance'] = matched_locations['postmile'].diff()

    # Not all adjacent rows correspond to location pairs
    # Filtering criteria:
    # - same trip_id
    #   - travel_time < 1 hour, to avoid pairing trip_id from different days
    #       (typically each trip_id is used at most once every day)
    # TODO: trip_id is treated as a number (OK for DBus data), but this is not generally true
    row_valid = (matched_locations['travel_time'] < 1 * 3600 * 1000) & (matched_locations['trip_id'].diff() == 0)
    matched_locations = matched_locations[row_valid]
    return matched_locations


def load_stop_sequence_data(starttime_ms):
    """
    Get postmiles of stops sorted by shape_id
    """
    stop_sequence_filename = construct_filename(datatype="stop_sequence", starttime_ms=starttime_ms)
    stop_sequences = pd.read_csv(stop_sequence_filename)
    print "Total number of rows: {}".format(len(stop_sequences))

    # input check
    unique_shape_id = stop_sequences['shape_id'].unique()
    for shape_id in unique_shape_id:
        stop_sequences_this_shape_id = stop_sequences[stop_sequences['shape_id'] == shape_id]
        if (stop_sequences_this_shape_id['stop_sequence'].diff() < 0).any():
            print "Stop sequences of shape {} are not in order!".format(shape_id)
        if (stop_sequences_this_shape_id['shape_dist_traveled'].diff() < 0).any():
            print "Stop postmiles of shape {} are not in order!".format(shape_id)

    return stop_sequences


def load_event_data(starttime_ms, number_of_days):
    event_filename = construct_filename(datatype="event", starttime_ms=starttime_ms, number_of_days=number_of_days)
    events = pd.read_csv(event_filename)
    print "Total number of rows: {}".format(len(events))

    # input check
    unique_trip_id = events['trip_id'].unique()
    for trip_id in unique_trip_id:
        events_this_trip_id = events[events['trip_id'] == trip_id]
        if (events_this_trip_id['stop_sequence'].diff() < 0).any():
            print "Events of trip {} are not in order!".format(trip_id)

    # TODO: data filtering
    # When events data are sorted by trip_id and time, sometimes stop_sequence may not be in order
    # For trips 06140005000208000401 and 06140005000209090102, there is an extra arrival at the first stop
    # For trip 06140005000209550102, there are some duplicate events with the same trip_id but very different time
    # To solve this, all events after arriving at the end of line should be removed

    return events


if __name__ == "__main__":
    number_of_days = 1
    starttime_ms = 1404079200000

    proj_locations = load_proj_location_data(starttime_ms=starttime_ms, number_of_days=number_of_days)
    matched_locations = match_location_pairs(proj_locations)

    stop_sequences = load_stop_sequence_data(starttime_ms=starttime_ms)

    events = load_event_data(starttime_ms=starttime_ms, number_of_days=number_of_days)
