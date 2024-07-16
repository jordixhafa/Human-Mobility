
from libraries import *


def stop_locations(df, nj, distance, stay, file):
    start_time = time.time()
    
    roaming_distance = meters2degrees(distance) # meters converted to degrees
    minimum_stay = stay # minutes
    number_jobs = nj # number of parallel jobs
    
    # Set index
    df = df.reset_index().set_index(["user_id", "timestamp"])
    
    # Call helper-function to process entire df in one go
    print(" STOP LOCATIONS ")
    print(" ")
    df_stops = process_data(df=df, 
                        roam_dist=roaming_distance, 
                        min_stay=minimum_stay, 
                        n_jobs=number_jobs)
    
    df_stops = pd.concat(df_stops)
    
    # Only keep users with more than one stop
    df_stops = (df_stops
    .groupby("user_id").filter(lambda x: len(x) > 1)
    .set_index(["user_id"]))
    
    # Preview data
    print("Number of stop locations: {}".format(df_stops.shape[0]))
    df_stops.iloc[:3,:]
    
    
    fig_stops = plot_stops(df_stops.reset_index())
    #fig_stops.show()
    plot(fig_stops, filename=file)
    
    print(" ")
    print("Tiempo Stop Locations (minutos):  ", (time.time() - start_time)/60)
    print(" ")
    return df_stops
