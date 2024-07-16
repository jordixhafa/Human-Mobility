
from libraries import *


def destinations(df_stops, clustering_distance, n_top, file):
    start_time_Dest = time.time()
    
    # Clustering parameters
    linkage_method = 'centroid'
    distance = meters2degrees(clustering_distance)
    
    # Cluster stoplocations on a per user basis
    df_clusters_all = (cluster_stoplocations(df_stops, 'centroid', distance)
                   .reset_index())
    
    # Preview results
    df_clusters_all.iloc[:3,:]
    
    
    # Get medoid of each destination (cluster)
    df_clustermedoids_all = (get_clustermedoids(df_clusters_all)
                         .reset_index(drop=True))
    
    # Compute stop counts at each destination
    df_clustersizes_all = (df_clusters_all
                   .groupby(['cluster_assignment'])
                   .apply(lambda x: len(x))
                   .reset_index(name='count'))
    
    
    
    
    
    
    # Merge medoids and counts
    temp_cols = ['user_id', 'timestamp','latitude', 'longitude',
             'cluster_assignment']
    df_destinations_all = pd.merge(df_clustermedoids_all.loc[:, temp_cols], 
                                 df_clustersizes_all, 
                                 on=['cluster_assignment'], 
                                 how='left')
    
    # Export and preview data
    df_destinations_all.iloc[:3,:]
    
    
    fig_destinations_all = plot_destinations(df_destinations_all[df_destinations_all['count'] > 1])    
    
    
    '''
    #Ranking destinations
    if (n_top > 0):
        geolocator = Nominatim(user_agent="myGeocoder")
        
        df_ranking_destinations = df_destinations_all.sort_values('count', ascending=False)
        df_ranking_destinations = df_ranking_destinations[:n_top-1]
        
        top_destinations = []
        
        for index, row in df_ranking_destinations.iterrows():
            print("")
            print(geolocator.reverse(str(df_ranking_destinations['latitude'][index]) + ", " + str(df_ranking_destinations['longitude'][index]), language='en'))
    '''
        
        
        

    
    
    plot(fig_destinations_all, filename=file)
    print(" ")
    print("Tiempo Destinations (minutos):  ", (time.time() - start_time_Dest)/60)

    return df_destinations_all
