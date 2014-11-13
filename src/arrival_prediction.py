import pandas as pd
import matplotlib.pyplot as plt


def plot_postmile_vs_time_by_shape_id(proj_locations):
    unique_shape_id = proj_locations['shape_id'].unique()
    for shape_id in unique_shape_id:
        plt.figure()
        proj_locations_this_shape_id = proj_locations[proj_locations['shape_id'] == shape_id]
        unique_trip_id = proj_locations_this_shape_id['trip_id'].unique()
        for trip_id in unique_trip_id:
            d = proj_locations_this_shape_id[proj_locations_this_shape_id['trip_id'] == trip_id]
            plt.subplot(1,2,1)
            plt.title("Postmile vs. Time for shape_id: {}".format(shape_id))
            plt.plot(d['time'], d['postmile'])
            plt.subplot(1,2,2)
            plt.title("Longitude vs. Latitude for shape_id: {}".format(shape_id))
            plt.plot(d['longitude'], d['latitude'], 'bo-')
    plt.show()


def match_location_pairs(proj_locations):
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
    row_valid = (matched_locations['travel_time'] < 1*3600*1000) & (matched_locations['trip_id'].diff() == 0)
    matched_locations = matched_locations[row_valid]
    return matched_locations

if __name__ == "__main__":
    number_of_days = 7
    starttime_s = 1404079200
    pathname = "../dbusdata/"
    csv_filename = "dbus_proj_location_{starttime_s}_{days}days.csv"\
        .format(starttime_s=starttime_s, days=number_of_days)
    proj_locations = pd.read_csv(pathname + csv_filename)
    print "Total number of rows: {}".format(len(proj_locations))

    # Input check: eliminate duplication in data (only using columns time and trip_id)
    if proj_locations.duplicated(['time', 'trip_id']).any():
        proj_locations.drop_duplicates(['time', 'trip_id'], inplace=True)
        print "Duplicates removed, now {} rows".format(len(proj_locations))

    # # plot postmile vs. time for different shape_id
    # plot_postmile_vs_time_by_shape_id(proj_location)

    # matching consecutive locations to get travel time
    matched_locations = match_location_pairs(proj_locations)

    # TODO: get postmiles of stops for given shape_id
