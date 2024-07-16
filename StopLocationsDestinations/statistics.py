from libraries import *


#============== TIME ELAPSED BETWEEN MEASUREMENTS ============================
def time_elapsed(df):
    df_stats = pd.DataFrame()
    df_stats['delta_t'] = (df['time']
                           .transform(lambda x: x.diff()) / np.timedelta64(1,'s'))
    delta_t_plot = (df_stats[df_stats['delta_t'] < df_stats['delta_t']
                    .quantile(.96)]
                    .hist(bins=50, figsize=(6,2), alpha=.9, color="blue"))