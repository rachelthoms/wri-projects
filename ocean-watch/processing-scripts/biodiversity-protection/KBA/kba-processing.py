from cartoframes.auth import set_default_credentials
from cartoframes import read_carto, to_carto
import geopandas as gpd
import pandas as pd
import os
import logging 
from shapely import geometry
import shutil


# set up logging

# get top-level logger object
logger = logging.getLogger()
for handler in logger.handlers: logger.removeHandler(handler)
# manually set level 
logger.setLevel(logging.INFO)
# print to console
console = logging.StreamHandler()
logger.addHandler(console)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# authenticate carto account 
CARTO_USER = os.getenv('CARTO_WRI_RW_USER')
CARTO_KEY = os.getenv('CARTO_WRI_RW_KEY')
set_default_credentials(username=CARTO_USER, base_url="https://{user}.carto.com/".format(user=CARTO_USER),api_key=CARTO_KEY)

# Set the data directory
data_dir = '/home/rthoms/Github/resource-watch/wri-projects/ocean-watch/processing-scripts/biodiversity-protection/KBA/data'

logger.info('Pull KBAs from OneDrive')
# Copy the kba data set from OneDrive into the daa directory
raw_data_file = os.path.join(os.getenv("OCEANWATCH_DATA_DIR"),'KBA_coverage.csv')
dest_dir = os.path.join(data_dir, os.path.basename(raw_data_file))[:-4]
shutil.copy(raw_data_file, dest_dir)

# read data into a pandas dataframe 
df=pd.read_csv(raw_data_file, encoding='latin-1')

logger.info('Upload dataframe to Carto')
# upload data frame to Carto
to_carto(df, 'ocn_024a_rw0_key_biodiversity_area_protection', if_exists='replace')

