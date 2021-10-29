from cartoframes.auth import set_default_credentials
from cartoframes import read_carto, to_carto
import geopandas as gpd
import pandas as pd
import os
import logging 
from shapely import geometry
import requests
import re
import glob
from zipfile import ZipFile
import shutil
import urllib
import json

# get top-level logger object
logger = logging.getLogger()
for handler in logger.handlers: logger.removeHandler(handler)
# manually set level 
logger.setLevel(logging.INFO)
# print to console
console = logging.StreamHandler()
logger.addHandler(console)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

data_dir = 'data'

CARTO_USER = os.getenv('CARTO_WRI_RW_USER')
CARTO_KEY = os.getenv('CARTO_WRI_RW_KEY')
set_default_credentials(username=CARTO_USER, base_url="https://{user}.carto.com/".format(user=CARTO_USER),api_key=CARTO_KEY)

slugs = ["a_coastal_zone_general_auto","a_coastal_fisheries_auto","a_coastal_management_auto","a_mangroves_auto","a_sea_level_rise_protection_auto","a_fisheries_and_aquaculture_auto","m_fisheries_and_aquaculture_auto","m_renewable_energy_ocean_auto","m_maritime_auto"]

indicator_url = 'https://www.climatewatchdata.org/api/v1/data/ndc_content/indicators'
req = requests.get(indicator_url)
json_dict= json.loads(req.text)
df = pd.DataFrame.from_dict(json_dict['data'])
df = df = df.loc[df['slug'].isin(slugs)]


# insert the url used to download the data from the source website

ids =[id for id in df['id']]

url_shell= 'https://www.climatewatchdata.org/api/v1/data/ndc_content?indicator_ids[]={}&indicator_ids[]={}&indicator_ids[]={}&indicator_ids[]={}&indicator_ids[]={}&indicator_ids[]={}&indicator_ids[]={}&indicator_ids[]={}&indicator_ids[]={}&per_page=1000'
url_template = url_shell.format(*tuple(ids)) + '&page={}'


paginated_dfs = []
# download the cata from the source
for i in range(1,10):
    url = url_template.format(i)
    req = requests.get(url)
    text_data= req.text
    json_dict= json.loads(text_data)
    df = pd.DataFrame.from_dict(json_dict['data'])
    paginated_dfs.append(df)

df = pd.concat(paginated_dfs).reset_index()
df=df.drop_duplicates(['id'])

df['count'] =''
for index, row in df.iterrows():
    if 'No' in row['value']:
         df.loc[index, 'count'] = 0
    else: 
        df.loc[index, 'count'] = 1

cm = df.loc[df['indicator_id'] == 'a_coastal_management_auto']
cm = cm.loc[cm['value'] == 'Sectoral Measure Specified']
print(len(cm.index))

df_world = df.groupby(['indicator_id']).agg({'indicator_name':'first', 'sector':'first', 'subsector':'first','count':'sum'}).reset_index()
df_world['iso_code3'] = 'GLB'
df_world['country'] = 'Global'
df = pd.concat([df, df_world]).reset_index()


df=df.drop(['index','id','country','source', 'global_category', 'overview_category'], axis = 1)
df.info(verbose=True)

logger.info('Upload dataframe to Carto')
# upload data frame to Carto
to_carto(df, 'ocn_025_rw0_ocean_ndc_measures', if_exists='replace')