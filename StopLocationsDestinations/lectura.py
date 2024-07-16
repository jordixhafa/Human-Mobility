from libraries import *


def read_plt(plt_file):
    points = pd.read_csv(plt_file, skiprows=6, header=None,
                         parse_dates=[[5, 6]], infer_datetime_format=True)

    # for clarity rename columns
    points.rename(inplace=True, columns={'5_6': 'timestamp', 0: 'latitude', 1: 'longitude', 3: 'alt'})

    # remove unused columns
    points.drop(inplace=True, columns=[2, 4])

    return points

def read_user(user_folder):

    plt_files = glob.glob(os.path.join(user_folder, 'Trajectory', '*.plt'))
    df = pd.concat([read_plt(f) for f in plt_files])

    return df


def read_all_users(folder):
    subfolders = os.listdir(folder)
    dfs = []
    for i, sf in enumerate(subfolders):
        print('[%d/%d] processing user %s' % (i + 1, len(subfolders), sf))
        df = read_user(os.path.join(folder,sf))
        df['user_id'] = int(sf)
        dfs.append(df)
    return pd.concat(dfs)


def read_dataset(which, sample_size=None):
    if (which == "all"):
        df = read_all_users('/home/jordi/Desktop/UNI/TFG/projecte/Reduced-Data')
    else:
        df = read_user('/home/jordi/Desktop/UNI/TFG/projecte/Data/' + which)
    
    if sample_size != None:
        df = df.sample(df.shape[0]//sample_size, random_state=10)
        
    return df




def df_laborables(df):
    df = df.loc[(df['timestamp'].dt.weekday >= 0) & (df['timestamp'].dt.weekday <= 4)]
    return df

def df_weekend(df):
    df = df.loc[(df['timestamp'].dt.weekday >= 5) & (df['timestamp'].dt.weekday <= 6)]
    return df



def df_between_times(df, start_h, start_m, start_s, end_h, end_m, end_s):
    start_date = datetime(1900, 1, 1, start_h, start_m, start_s)
    end_date = datetime(1900, 1, 1, end_h, end_m, end_s)
    
    after_start_date = df["timestamp"].dt.time >= start_date.time()
    before_end_date = df["timestamp"].dt.time <= end_date.time()
    between_two_dates = after_start_date & before_end_date
    
    return df.loc[between_two_dates]
    



