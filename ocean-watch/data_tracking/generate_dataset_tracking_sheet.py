import os
import pandas as pd

METADATA_SHEET="https://docs.google.com/spreadsheets/d/1A3RbymgsB5bwljFsL20Brj-M29as0m2yPCAoAK25N6k/export?format=csv&gid=0"

# Pull in metadata
current_mdata = pd.read_csv(METADATA_SHEET, header=0)

# Subset to Ocean Wach datasets
projects = pd.notnull(current_mdata['Project'])
project_mdata = current_mdata.loc[projects]
ow_projects = project_mdata[project_mdata['Project'].str.contains("Ocean")].reset_index()

ow_projects_on_rw = pd.notnull(ow_projects['API_ID'])
ow_mdata = ow_projects.loc[ow_projects_on_rw]

# Keep relevant columns
ow_mdata = ow_mdata[['New WRI_ID', 'API_ID', 'Status', 'Public Title']]

# Add column for whether the dataset was added by the Ocean Watch Data Team
not_added_by_ow = [
    'bio.004.rw2', 
    'bio.007.rw1.nrt',
    'bio.037.rw0.nrt', 
    'com.017.rw2',
    'com.022.rw0', 
    'ene.009.rw0', 
    'foo.055.rw1',  
    'wat.059.rw0']

for index, row in ow_mdata.iterrows():
    ow_mdata.at[index, 'added_for_ow'] = False if row['New WRI_ID'] in not_added_by_ow else True

added_by_ow = ow_mdata[ow_mdata['added_for_ow'] == True]
count_added = len(added_by_ow)

count_published = len(added_by_ow[added_by_ow['Status']== 'Published'])

print('Number of datasets added by Ocean Watch Data Team: ' + str(count_added) + ' ('+ str(count_published) + ' of which are published in RW catalog)')

# path to processed table
processed_table = os.path.join(os.getenv('OCEANWATCH_DATA_DIR'), 'ow_dataset_tracking_sheet.xlsx')

# save processed table to the Ocean Watch Data Directory
ow_mdata.to_excel(processed_table, index= False)
