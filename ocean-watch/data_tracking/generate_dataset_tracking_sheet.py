import os
from typing import OrderedDict
import pandas as pd
import LMIPy as lmi
import json
import requests

METADATA_SHEET = "https://docs.google.com/spreadsheets/d/1A3RbymgsB5bwljFsL20Brj-M29as0m2yPCAoAK25N6k/export?format=csv&gid=0"

# Load OW json
ow_json_url = "https://raw.githubusercontent.com/resource-watch/resource-watch/develop/public/static/data/ocean-watch.json"
ow_json = pd.read_json(ow_json_url)

# empty dictionary to store widget IDs and information
crp_widget_dict= OrderedDict()

# empty list to store mini_explore datasets
crp_mini_ex_datasets= []

# iterate through the jsons for the CR dashboard and profile
crp_json_urls= ['https://api.resourcewatch.org/v1/dashboard/429', 'https://api.resourcewatch.org/v1/dashboard/504']
for url in crp_json_urls:
    # load the json
    crp_object = json.loads(requests.get(url).text)['data']['attributes']['content']
    # remove backslashes
    crp_object_clean = crp_object.replace('/','')
    # Read in the dashboard content as a json
    crp_json= pd.read_json(json.loads(json.dumps(crp_object_clean)))
    # iterate through the content to get widget and dataset IDs 
    for index, row in crp_json.iterrows():
        if row['type'] == 'widget':
            widget_id = row['content']['widgetId']
            if widget_id not in crp_widget_dict:
                widget_object = lmi.Widget(widget_id)
                widget_name = widget_object.attributes['name']
                crp_widget_dict.setdefault(widget_id,[]).append({widget_name,'chart','CRP'})
        if row['type'] == 'mini-explore':
            crp_mini_ex_content= row['content'].replace('\n ','')
            crp_mini_ex_content = crp_mini_ex_content.replace('\\','')
            datasets = pd.read_json(json.loads(json.dumps(crp_mini_ex_content)))['datasetGroups'][0]['datasets']
            crp_mini_ex_datasets.extend(datasets)

# Pull in widgets on OW
WIDGET_TRACKING_SHEET = os.path.join(os.getenv('OCEANWATCH_DATA_DIR'), 'ow_widget_tracking_sheet.xlsx')
ow_widgets = pd.read_excel(WIDGET_TRACKING_SHEET, header=0)

# iterate through the tracking sheet to get widget IDs
ow_widget_dict = OrderedDict()
for id, t, name in zip(ow_widgets.widget_id,ow_widgets.widget_type, ow_widgets.widget_name):
    if id  not in ow_widget_dict:
        ow_widget_dict.setdefault(id,[]).append({name,t, 'OW'})

# Pull datasets in the OW mini explore from the OW json
ow_mini_ex_datasets = []
mini_explore = ow_json['production']['country-profile'][3][0]['content'][1][0]['config']['datasetGroups']
for group in mini_explore:
    ow_mini_ex_datasets.extend(group['datasets'])

# Read in RW metadata tracking sheet
current_mdata = pd.read_csv(METADATA_SHEET, header=0)

# Subset to datasets associated with Projects
projects = pd.notnull(current_mdata['Project'])
project_mdata = current_mdata.loc[projects]

# Subset Ocean Wach datasets
ow_projects = project_mdata[project_mdata['Project'].str.contains("Ocean")].reset_index()

# Subset to datasets on RW (with an API ID)
ow_projects_on_rw = pd.notnull(ow_projects['API_ID'])
ow_mdata = ow_projects.loc[ow_projects_on_rw]

#Subset Coral Reef Dashboard Datasets 
coral_mdata = project_mdata[project_mdata['Project'].str.contains("Coral")].reset_index()

# Concatenate dataframes of OW and Coral Reef datasets
ocean_mdata = pd.concat([ow_mdata, coral_mdata], axis=0).drop_duplicates('API_ID').reset_index()

# Keep relevant columns
ocean_mdata = ocean_mdata[['New WRI_ID','Public Title', 'API_ID', 'Status']]

# Add column for whether the dataset was added by the Ocean Watch Data Team
not_added_by_ow = [
    'bio.004.rw2', 
    'bio.007.rw1.nrt',
    'bio.037.rw0.nrt', 
    'com.005.rw0',
    'com.017.rw2',
    'com.012.rw0',
    'com.022.rw0', 
    'ene.009.rw0', 
    'foo.055.rw1',  
    'wat.059.rw0']

# iterate through the dataset the OW datasets and append the relevant assets to the `widgets` column
for index, row in ocean_mdata.iterrows():
    ocean_mdata.at[index, 'added_for_ow'] = False if row['New WRI_ID'] in not_added_by_ow else True
    dataset_object = lmi.Dataset(row['API_ID'])
    assets = {}
    for widget in dataset_object.widget:
        id = widget.id
        if id in ow_widget_dict:
            assets.setdefault(id,ow_widget_dict[id])
        if id in crp_widget_dict:
            assets.setdefault(id,crp_widget_dict[id])
    if row['API_ID'] in ow_mini_ex_datasets:
        assets.setdefault(id,[]).append({'mini-explore', 'OW'})
    if row['API_ID'] in crp_mini_ex_datasets:
        assets.setdefault(id,[]).append({'mini-explore', 'CRP'})
    ocean_mdata.loc[index, 'widgets'] = [assets]

for dataset in crp_mini_ex_datasets:
    if dataset not in ocean_mdata['API_ID'].unique():
        print(dataset + ' not in tracking sheet')

added_by_ow = ocean_mdata[ocean_mdata['added_for_ow'] == True]
count_added = len(added_by_ow)

count_published = len(added_by_ow[added_by_ow['Status']== 'Published'])

print('Number of datasets added by Ocean Watch Data Team: ' + str(count_added) + ' ('+ str(count_published) + ' of which are published in RW catalog)')


# path to processed table
processed_table = os.path.join(os.getenv('OCEANWATCH_DATA_DIR'), 'ow_dataset_tracking_sheet.xlsx')

# save processed table to the Ocean Watch Data Directory
ocean_mdata.to_excel(processed_table, index= False)
