# HAARVI Timeline Check Script
This script processes participant data from the HAARVI study to identify appointment eligibility based on infection and vaccination timelines. It uses data from the REDCap "HAARVI Study Records" using the "Timeline Check" report. The script filteres participants based on activty status, age, and outputs a cleaned CSV. file of eligible participants. 

## Files Used 
- HAARVIStudyRecords-TimelineCheck_DATA*.csv: REDCap export located in the user's Downloads folder.
- all_haarvi_bd_apts.csv: Blood draw data file located in the user's Downloads folder.

## Output
- haarvi_apts_<month>_<year>.csv: Cleaned dataset of eligible participants saved to the Downloads folder.

## Features 
- Loads and validates REDCap export data.
- Converts relevant columns to datetime format.
- Calculates participant age and removes invalid entries.
- Filters participants based on REDCap status codes.
- Merges convalescent and control IDs.
- Standardizes PTIDs.
- Merges blood draw data (all_haarvi_bd_apts.csv) and removes participants with more than 2 recent appointments.
- Checks appointment eligibility for COVID-19, Flu, and RSV infection/vaccination.
- Checks timeline points for 30d, 3m, 6m, 12m, 18m, and 24m.

## Required Python Packages 
- pandas
- datetime
- dateutil
- pathlib
- glob
- re
- os

## How to Run
- Ensure the required CSV. files are in your Downloads folder
- Run the script using Python
- The output CSV. will be saved in the Downloads folder.

## Contact 
For questions or support, contact hartea@uw.edu 
