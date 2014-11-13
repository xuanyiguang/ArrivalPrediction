import pandas as pd
import matplotlib.pyplot as plt


def plot_postmile_vs_time_by_shape_id(proj_location):
    unique_shape_id = proj_location['shape_id'].unique()
    for shape_id in unique_shape_id:
        plt.figure()
        plt.title("Shape id: {}".format(shape_id))
        proj_location_this_shape_id = proj_location[proj_location['shape_id'] == shape_id]
        unique_trip_id = proj_location_this_shape_id['trip_id'].unique()
        for trip_id in unique_trip_id:
            d = proj_location_this_shape_id[proj_location_this_shape_id['trip_id'] == trip_id]
            plt.plot(d['time'], d['postmile'])
    plt.show()


if __name__ == "__main__":
    number_of_days = 1
    starttime_s = 1404079200
    pathname = "../dbusdata/"
    csv_filename = "dbus_proj_location_{starttime_s}_{days}days.csv"\
        .format(starttime_s=starttime_s, days=number_of_days)
    proj_location = pd.read_csv(pathname + csv_filename)
    print len(proj_location)

    # eliminate duplication in data (only using columns time and trip_id)
    if proj_location.duplicated(['time', 'trip_id']).any():
        proj_location.drop_duplicates(['time', 'trip_id'], inplace=True)
        print len(proj_location)

    # # plot postmile vs. time for different shape_id
    # plot_postmile_vs_time_by_shape_id(proj_location)

    # TODO: matching consecutive locations to get travel time

    # TODO: get postmiles of stops for given shape_id
