from lectura import*
from stop_locations import *
from destinations import *
from statistics import *


#=== PARAMETRES =======
n_jobs = 6
roaming_distance = 50
minimum_stay = 10
clustering_distance = 100
sample_size = 1
n_top = 0 #10



#====== LECTURA DATASET ==========
# "all" or "000", "001", etc.
df = read_dataset("all", sample_size)

#df_laborables = df_laborables(df)
df_weekend = df_weekend(df)

#df_0_8 = df_between_times(df, 0, 0, 0, 7, 59, 59)
#df_8_18 = df_between_times(df, 8, 0, 0, 17, 59, 59)
#df_18_24 = df_between_times(df, 18, 0, 0, 23, 59, 59)




#===== STOP LOCATIONS ================
#ALL
df_stops = stop_locations(df, n_jobs, roaming_distance, minimum_stay, 'fig_stops_finde.html')
#LABORABLE
#df_stops_laborable = stop_locations(df_laborables, n_jobs, roaming_distance, minimum_stay, 'fig_stops_laborable.html')
#WEEKEND
#df_stops_weekend = stop_locations(df_weekend, n_jobs, roaming_distance, minimum_stay, 'fig_stops_weekend.html')


#===== DESTINATIONS ====================
#ALL
df_destinations = destinations(df_stops, clustering_distance, n_top, 'fig_destinations_finde.html')
#LABORABLE
#df_destinations_laborable = destinations(df_stops_laborable, clustering_distance, 'fig_destinations_laborable.html')
#WEEKEND
#df_destinations_weekend = destinations(df_stops_weekend, clustering_distance, 'fig_destinations_weekend.html')




