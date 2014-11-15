import pandas as pd
import matplotlib.pyplot as plt
from src.query_dbus_data_postgres import construct_filename


def plot_by_shape_id(proj_locations, stop_sequences=None):
    """
    Plot the following by shape_id, using information in proj_locations:
        - postmile vs. time
        - longitude vs. latitude
    If stop_sequences is further provided:
        - stop postmiles are added to the first subplot
        - stop locations are added to the second subplot
    """
    unique_shape_id = proj_locations['shape_id'].unique()
    for shape_id in unique_shape_id:
        plt.figure()

        proj_locations_this_shape_id = proj_locations[proj_locations['shape_id'] == shape_id]
        unique_trip_id = proj_locations_this_shape_id['trip_id'].unique()
        for trip_id in unique_trip_id:
            proj_locations_this_trip_id = proj_locations_this_shape_id[proj_locations_this_shape_id['trip_id'] == trip_id]
            plt.subplot(1, 2, 1)
            plt.title("Postmile vs. Time for shape_id: {}".format(shape_id))
            plt.plot(proj_locations_this_trip_id['time'], proj_locations_this_trip_id['postmile'])
            plt.subplot(1, 2, 2)
            plt.title("Longitude vs. Latitude for shape_id: {}".format(shape_id))
            plt.plot(proj_locations_this_trip_id['longitude'], proj_locations_this_trip_id['latitude'], 'bo-')

        if stop_sequences is not None:
            stop_sequences_this_shape_id = stop_sequences[stop_sequences['shape_id'] == shape_id]
            plt.subplot(1, 2, 1)
            for postmile in stop_sequences_this_shape_id['shape_dist_traveled']:
                plt.axhline(y=postmile, linestyle=":")
            plt.subplot(1, 2, 2)
            plt.plot(stop_sequences_this_shape_id['stop_lon'], stop_sequences_this_shape_id['stop_lat'], 'rs')

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
    #   - same trip_id
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
    # matched_locations = match_location_pairs(proj_locations)

    stop_sequences = load_stop_sequence_data(starttime_ms=starttime_ms)

    events = load_event_data(starttime_ms=starttime_ms, number_of_days=number_of_days)

    # plot_by_shape_id(proj_locations=proj_locations, stop_sequences=stop_sequences)


    # Training (with training data)
    #   - Benchmark: propagate delay downstream (no training required)
    #   - Algorithm 1: assume uniform speed between locations
    #   - Algorithm 2: assume travel times are independent distributions, use Expectation Maximization ...

    # Predicting (with validation data)
    #   - Prediction is carried out when new location is received, for all the downstream stops
    #   - It is possible to carry out prediction on a regular interval, but it is only a matter of extrapolation
    #       with no new information. Will do this later.

    # Sort proj_locations in the order of time
    proj_locations.sort(columns='time', inplace=True)

    # for each new location that becomes available, do prediction
    for location_index in range(len(proj_locations)):
        location_shape_id = proj_locations.ix[location_index, 'shape_id']
        location_postmile = proj_locations.ix[location_index, 'postmile']

        # Find all stops of the same shape_id and larger postmile
        selected_stop_index = (stop_sequences['shape_id'] == location_shape_id) & \
                     (stop_sequences['shape_dist_traveled'] >= location_postmile)
        # TODO: rename stop_sequences to stops or sequenced_stops
        selected_stop_sequences = stop_sequences[selected_stop_index]

        # for each selected stop, predict arrival time
        for stop_index in range(len(selected_stop_sequences)):
            # TODO: query DB for arrival time (in local time with no date) and adjust to epoch time
            # SELECT arrival_time, stop_id, stop_sequence
            # FROM gtfs_stop_times_history
            # WHERE trip_id = '06140005000107090401'
            # and t_range @> to_timestamp('1404079200')
            # --and stop_id = '240'
            # TODO: then plus the delay to get predicted arrival time
            # TODO: need to understand the sign of delay (+ for late and - for early?)
            pass
