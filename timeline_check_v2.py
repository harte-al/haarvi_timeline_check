import os
import glob
import re
import pandas as pd
from datetime import datetime 
from datetime import timedelta
from pathlib import Path
from dateutil.relativedelta import relativedelta


current_month = datetime.now().month
current_year = datetime.now().year

downloads_path = Path.home() / "Downloads"
bd_csv_path = Path.home() / "Downloads" / "all_haarvi_bd_apts.csv"
#bd_grouped_csv_path = Path.home() / "Downloads" / "all_haarvi_bds_grouped.csv"
#master_test_path = downloads_path / "master_test.csv"
master_clean_path = downloads_path / f"haarvi_apts_{current_month}_{current_year}.csv"

cov_inf_columns = ["test_date",	"test_date_cov2", "test_date_cov3", "test_date_4", "test_date_5"]
cov_vax_columns = ["date_dose_1", "date_dose_2","date_dose_3", "date_dose_4", "date_dose_5",	
                   "date_dose_6", "date_dose_7", "date_dose_8", "date_dose_9", "date_dose_10",
                   "date_dose_11", "date_dose_12"]

flu_inf_columns = ["test_date_flu_1", "test_date_flu_2"]
flu_vax_columns = ["flu_vax_date", "flu_vax_date_2022_2023", "flu_vax_date_23_24", "flu_vax_date_24_25", "flu_vax_date_y2526"]

rsv_inf_columns = ["test_date_rsv_1", "test_date_rsv2"]
rsv_vax_columns = ["rsv_vax_date_23_24", "rsv_vax_date_24_25", "rsv_vax_date_y2526"]

date_columns = [cov_inf_columns, cov_vax_columns, flu_inf_columns, flu_vax_columns,
                 rsv_inf_columns, rsv_vax_columns]
unpacked_date_columns = []
for col in date_columns:
    for evt in col:
        unpacked_date_columns.append(evt)

pt_status_columns = ["participant_status___1",	"participant_status___2", "participant_status___3",
                      "participant_status___4", "participant_status___5", "participant_status___6", 
                      "participant_status___7", "participant_status___8"]
"""
*pt_s = 1, Actively participating 
pt_s = 2, Voluntarily opted out
pt_s = 3, Moved away
pt_s = 4, Removed from study
*pt_s = 5, Non-communicating
pt_s = 6, Opted out of blood draws
pt_s = 7, Only surveys
pt_s = 8, Deceased
"""

delta_map = {
    "30d": relativedelta(days=30),
    "3m": relativedelta(months=3),
    "6m": relativedelta(months=6),
    "12m": relativedelta(months=12),
    "18m": relativedelta(months=18),
    "24m": relativedelta(months=24)
}

def get_rc_export(downloads_path): 
    """
    Input: 'Timeline Check' REDCap export raw from HAARVI Study Records Project file path.
    Return: Dataframe of 'Timeline Check' export.
    """
    try:
        print(f"\nLoading REDCap export...")
        csv_files = glob.glob(str(downloads_path / "*.csv"))
        if not csv_files:
            raise FileNotFoundError("[!] No CSV files in Downloads")
    
        latest_csv_file = max(csv_files, key=os.path.getctime)
        latest_file_name = Path(latest_csv_file).name

        if not latest_file_name.startswith("HAARVIStudyRecords-TimelineCheck_DATA"):
                raise ValueError(f"[!] Latest CSV file: '{latest_file_name}' has unexpected name.")
        
        print(f"Successfully loaded REDCap export '{latest_file_name}'. ")
        return pd.read_csv(latest_csv_file) 
    
    except Exception as e: 
        print(f"[!] Exception occurred during REDCap export: {e}")


def convert_to_dt(df, df_columns): 
    """
    Input: DataFrame of 'Timeline Check', list of columns to convert to DateTime.
    Return: DateFrame with DateTime values.
    """
    try:
        print(f"\nConverting {df_columns} to DateTime...")
        for col in df_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors ='coerce')
            else:
                raise ValueError(f"[!] Column: '{col}' not found in DateFrame. ")

        if 'dob' in df.columns:
            df['dob'] =  pd.to_datetime(df['dob'], errors ='coerce')
        else: 
            raise ValueError(f"[!] Column: 'dob' not found in DateFrame. ")
        
        print(f"Successfully converted {df_columns} to Datetime'.")
        return df
    
    except Exception as e:
        print(f"[!] Exception occurred during DateTime conversion: {e}")


def is_same_month(date, time_delta = None):
    """
    Input: Date value, delta map of relative time differences.
    Return: Boolean value if new_date is the same month & year as OS. 
    """

    if pd.isna(date):
        return False
    
    if time_delta not in delta_map:
        raise ValueError(f"Invalid time_delta: {time_delta}")
    new_date = date + delta_map[time_delta]
    
    return new_date.month == current_month and new_date.year == current_year


def calc_age(df, dob_column):
    """
    Input: DataFrame, date of birth column from 'Timeline Check'.
    Return: New column to DataFrame with PT age, validates and removes PTs from Dataframe 
            based on their age. 
    """
    try:
        print(f"\nCalculating PT age...")
        today = datetime.today()

        df['age'] = df[dob_column].apply(lambda dob: today.year - dob.year - ((today.month,
                                                                            today.day) < (dob.month, dob.day)))

        # Validate ages
        print(f"Validating ages...")
        invalid_idxs = []
        for idx, age in df['age'].items():
            if age < 0.5:
                print(f'At idx {idx}, age is less than allowable')
                invalid_idxs.append(idx)
            elif age > 120:
                print(f'At idx {idx}, age is greater than allowable')
                invalid_idxs.append(idx)
            elif pd.isnull(age):
                print(f'At idx {idx}, age is NA')
                invalid_idxs.append(idx)

        if invalid_idxs:
            print(f"Removing indexes {invalid_idxs} from DateFrame.")
            df.drop(index = invalid_idxs, inplace = True)    

        print(f"Successfully calculated PT ages.")
        return df

    except Exception as e:
        print(f"[!] Exception occurred during age calculation: {e}")


def filter_activity(df, status_cols):
    """
    Input: Dataframe, status column name in DateFrame
    Return: Dataframe with actively participating PTs. 
    """
    try:
        print(f"\nChecking participant statuses...")
        exclude_idxs = []

        for pt_s in status_cols:

            for idx, row in df[pt_s].items():
                
                if pt_s == "participant_status___1" and row == 0:
                    #print(f"Excluding idx {idx}, for status {pt_s}.")
                    exclude_idxs.append(idx)
                
                elif pt_s in {"participant_status___2", 
                            "participant_status___3", 
                            "participant_status___4", 
                            "participant_status___6", 
                            "participant_status___7",
                            "participant_status___8"
                            } and row ==1:
                    #print(f"Excluding idx {idx}, for status {pt_s}.") 
                    exclude_idxs.append(idx)
                        
        if exclude_idxs:
            print(f"Removing indexes {exclude_idxs} from DateFrame.")
            print(f"Total dropped: {len(set(exclude_idxs))} from DataFrame.")
            df.drop(index = set(exclude_idxs), inplace = True)    

        print(f"Successfully filtered PTs by REDCap status.")
        return df
    
    except Exception as e: 
        print(f"[!] Exception occurred during activity filtering: {e}")


def standardize_id(ptid):
    """
    Input: PTIDs.
    Return: Standardized PTIDs with consistent formatting.
    """
    try:
        match = re.match(r'(\d+)([A-Za-z]+)', str(ptid))
        if match:
            number, letter = match.groups()
            number_part = f"{int(number):05d}"
            letter_part = letter.upper()
            return number_part + letter_part
        
        return str(ptid).upper()
    except Exception as e: 
        print(f"[!] Exception occurred during PTID standardization: {e}")
    
    
def merge_ptids(df, col_cov, col_ctrl):
    """
    Input: 'Timeline Check" Dataframe, convalescentID column, and controlID column.
    Return: Dataframe with a single PTID column added.
    """
    try:
        print(f"\nMerging PTIDs from {col_cov} and {col_ctrl}...")
        invalid_idxs = []

        df['ptid'] = df[col_cov].combine_first(df[col_ctrl])
        
        print('Validating PTIDs...')
        for idx, ptid in df['ptid'].items():
            if pd.isnull(ptid):
                print(f'At idx: {idx}, PTID is NA. Removing from DF')
                invalid_idxs.append(idx)
        
        if invalid_idxs:
            df.drop(index = invalid_idxs, inplace = True)
        
        print(f"Successfully merged ['{col_cov}'] and ['{col_ctrl}'] to ['ptid'].")
        return df
    
    except Exception as e: 
        print(f"[!] Exception occurred during PTID merging: {e}")


def count_apts(df, date_collect_col):
    """
    Input: DataFrame, 'Date_Collected' column.
    Return: Count of appointments within the last 8 weeks, excludes PTs with 2 or more recent appointments. 
    """
    eight_weeks_ago = datetime.now() - timedelta(weeks=8)

    if date_collect_col not in df.columns:
        raise ValueError(f"[!] Column '{date_collect_col}' is not found in DateFrame.")

    print(f"\nChecking for recent appointments since {eight_weeks_ago.date()}...")
    recent_counts = {}
    exclude_idxs = []

    for idx, dates in df[date_collect_col].items():
    
        count = 0 
        if isinstance(dates, list):
            for d in dates:
                try:
                    dt = pd.to_datetime(d, errors='coerce')
                    if pd.notnull(dt) and dt >= eight_weeks_ago:
                        count += 1 

                except Exception:
                    continue
        recent_counts[idx] = count

        df['test_recent_apts'] = df.index.map(recent_counts)

        if recent_counts[idx] >= 2:
            exclude_idxs.append(idx)

    
    df['recent_apts_count'] = recent_counts

    if exclude_idxs:
        print(f"Exlucding {len(exclude_idxs)} participants with 2+ recent appointments.")
        df.drop(index= exclude_idxs, inplace = True)

    print(f"Successfully filtered PTs by recent appointments.")
    return df


def check_for_apts(df, timeline, timeline_cols, timeline_points:list):
    """
    Input: DataFrame, timeline str, timeline columns, and list of timeline points of interest
    Return: 
    """
    print(f"\nChecking for appointments for {timeline}...")
    
    try:
        if 'eligible_apts' not in df.columns:
            df['eligible_apts'] = [[] for _ in range(len(df))]

        for idx, row in df.iterrows():
            date_list = []
            
            for col in timeline_cols:
                if col in df.columns:
                    date = row[col]              
                if pd.notnull(date):
                    date_list.append(date.date())
                        
            if date_list:
                most_recent = max(date_list)
                
                for tp in timeline_points:
                    apt_this_month = is_same_month(most_recent, tp)
                    if apt_this_month:
                        col_name = f"{timeline}_{tp}"
                        df.at[idx, col_name] = True
                        
                        if col_name not in df.at[idx, 'eligible_apts']:
                            ptid = row['ptid']
                            print(f"PTID: {ptid} is eligible for {col_name} appointment.")
                            df.at[idx, 'eligible_apts'].append(col_name)
    
    except Exception as e:
        print(f"[!] Exception occurred during PTID merging: {e}")

    print(f"Successfully checked for {timeline} appointments.")
    return df


# --- Processing ---

# Blood draw information 
bd_df = pd.read_csv(bd_csv_path)
bd_df['Patient_Study_ID']= bd_df['Patient_Study_ID'].apply(standardize_id)
grouped_bd_df = bd_df.groupby('Patient_Study_ID')['Date_Collected'].apply(list).reset_index()
grouped_bd_df['ptid'] = grouped_bd_df['Patient_Study_ID']
grouped_bd_df.drop('Patient_Study_ID', axis=1, inplace=True)
#grouped_bd_df.to_csv(bd_grouped_csv_path, index = False)

# REDCap Export & cleaning 
master_raw = get_rc_export(downloads_path)

master_raw = convert_to_dt(master_raw, unpacked_date_columns)

master_raw = calc_age(master_raw, 'dob')

master_raw = filter_activity(master_raw, pt_status_columns)

master_raw = merge_ptids(master_raw,'conv_partid', 'ctrl_partid')
master_raw['ptid'] = master_raw['ptid'].apply(standardize_id)

master_raw = master_raw.merge(grouped_bd_df, on = 'ptid', how='left')

master_raw = count_apts(master_raw, 'Date_Collected')

check_for_apts(master_raw, 'cov_vax', cov_vax_columns, ['30d','3m','6m']) 
check_for_apts(master_raw, 'cov_inf', cov_inf_columns, ['30d','3m','6m'])
check_for_apts(master_raw, 'flu_vax', flu_vax_columns, ['30d','3m','6m'])
check_for_apts(master_raw, 'flu_inf', flu_inf_columns, ['30d','3m','6m'])
check_for_apts(master_raw, 'rsv_vax', rsv_vax_columns, ['30d','3m','6m', '12m', '18m', '24m'])
check_for_apts(master_raw, 'rsv_inf', rsv_inf_columns, ['30d','3m','6m', '12m', '18m', '24m'])

clean_cols = ['global_study_id','ptid','participant_email', 'age','Date_Collected','eligible_apts']
invalid_idxs = [] 
master_clean = master_raw[clean_cols]
for idx, apts in master_clean['eligible_apts'].items():
    if apts == []:
        invalid_idxs.append(idx)
if invalid_idxs:
    master_clean.drop(index = invalid_idxs, inplace = True)

master_clean.to_csv(master_clean_path, index=False)





