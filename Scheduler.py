import os 
import glob
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta


current_month = datetime.now().month
current_year = datetime.now().year
df_master = None


# TODO 1 - Have the script take the redcap export and read it. Determine last events for each type. ✅
# TODO 2 - Determine the last timeline events for each type✅  
# TODO 3 - Caclulate if any of the timeline events occur THIS month✅ 
# TODO 4 - Output who needs to be scheduled as first name, last intial, PTID, email contact, and event type. This can be outputted as txt. 


def is_same_month(date, time_delta = None):
    if pd.isna(date):
        return False
    
    if time_delta == "30d":
        new_date = date + relativedelta(days=30)
    elif time_delta == "3m":
        new_date = date + relativedelta(months=3)
    elif time_delta == "6m":
        new_date = date + relativedelta(months=6)
    else:
        raise ValueError("Invalid time_delta.")
    
    return new_date.month == current_month and new_date.year == current_year


# Read and format REDCap export 
file_list = glob.glob("/Users/alexharteloo/Documents/*.csv")
latest_file = max(file_list, key=os.path.getctime)

df_master = pd.read_csv(latest_file)
df_dates = df_master.iloc[:, 2:].apply(pd.to_datetime, errors = "coerce")

df_master.drop(df_master.columns[2:], axis=1, inplace=True)


# COV INF GROUP ✅
df_cov_inf = df_dates.iloc[:, 0:5] 
df_master["cov_inf_max"] = df_cov_inf.max(axis=1)

# FLU INF GROUP ✅
df_flu_inf = df_dates.iloc[:, 5:7] 
df_master["flu_inf_max"] = df_flu_inf.max(axis=1)

# RSV INF GROUP ✅ --> This will need to be updated when multiple RSV infections are implemented 
df_master["rsv_inf_max"] = df_dates["test_date_rsv_1"]

# COV VAX GROUP ✅
df_cov_vax = df_dates.iloc[:, 8:18] 
df_master["cov_vax_max"] = df_cov_vax.max(axis=1)

# FLU VAX GROUP ✅
df_flu_vax = df_dates.iloc[:, 18:22] 
df_master["flu_vax_max"] = df_flu_vax.max(axis=1)

# RSV VAX GROUP ✅
df_rsv_vax = df_dates.iloc[:, 22:24] 
df_master["rsv_vax_max"] = df_rsv_vax.max(axis=1)

print(df_master)
# Most recent of all events TODO
'''
try:
    df_master["recent_event"] = df_master.iloc[:, 2:].idxmax(axis='columns', skipna=True)
except ValueError:
    df_master["recent_event"] = None

print(df_master)
'''

# Timeline check ✅
df_master["30d check"] = df_master.iloc[:, 2:].applymap(lambda x: is_same_month(x, time_delta="30d")).any(axis=1)
df_master["3m check"] = df_master.iloc[:, 2:7].applymap(lambda x: is_same_month(x, time_delta="3m")).any(axis=1)
df_master["6m check"] = df_master.iloc[:, 2:7].applymap(lambda x: is_same_month(x, time_delta="6m")).any(axis=1)

#print(df_master.iloc[:, -3:])
df_filtered_master = df_master[df_master.iloc[:, -3:].any(axis=1)]

#df_filtered_master["event"]

df_filtered_master.to_csv("haarvi_schedule_list.csv", index=False)