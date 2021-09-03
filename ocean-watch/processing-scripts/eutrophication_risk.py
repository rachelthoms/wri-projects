from cartoframes.auth import set_default_credentials
from cartoframes import read_carto, to_carto
import geopandas as gpd
import pandas as pd
import os
import logging 
from shapely import geometry


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
                        
logger.info('Pull ICEP Basins from Carto table')
# read in Aqueduct Index Coastal Eutrophication Potentional Data (ICEP)
gdf_icep= read_carto('wat_059_aqueduct_coastal_eutrophication_potential')
# project to the a projected CRS
gdf_icep.to_crs('epsg:3035', inplace=True)

logger.info('Calculate basin centroids')
# calculate the centroids for each basin polygon
gdf_icep['points'] = gdf_icep['the_geom'].centroid
# remname the points column to geometry
gdf_icep.rename(columns = {'points': 'geometry'}, inplace = True)
# drop the polygon geometry
gdf_icep.drop(columns=['the_geom'], inplace= True)
# create a new geodataframe with the centroids as the geometry and set CRS
gdf_icep =gpd.GeoDataFrame(gdf_icep)
gdf_icep.set_crs('epsg:3035', inplace=True)
# reproject CRS to geographic CRS to match gadm polygons
gdf_icep.to_crs('epsg:4326', inplace=True)


logger.info('Pull country boundaries from Carto table')
# read in the GADM country boundaries used in the OW geostore
gdf_gadm = read_carto("gadm36_0")
# rename the_geom column to geometry
gdf_gadm.rename(columns = {'the_geom': 'geometry'}, inplace = True)

logger.info('Join centroids with country boundaries')
# join the centroid and country polygons (intersection)
gdf= gpd.sjoin(gdf_icep,gdf_gadm,how = 'inner')
# group and aggregate the dataframe by the country and risk level, counting the number of centroid    
gdf = gdf.groupby(['gid_0','cep_label']).agg({'name_0': 'first', 'pfaf_id':'nunique'}).reset_index()
# rename count colum
gdf.rename(columns = {'pfaf_id': 'count'}, inplace = True)
# subset the dataframe to remove geometry
df = gdf[['gid_0', 'name_0', 'cep_label', 'count']]

# create a column for the proportion
# group by country and risk level
df_grouped = df.groupby(['gid_0']).agg({'count':'sum'}).reset_index()
df_grouped.rename(columns = {'count': 'country_total'}, inplace = True)
df = pd.merge(df,df_grouped[['gid_0', 'country_total']], on='gid_0', how='left')
df['proportion'] = df['count']/df['country_total']


# calculate statistics for the world
# totals by risk level
df_global = df.groupby(['cep_label']).agg({'count':'sum'}).reset_index()
# global total
total_basins = sum(df_global['count'])
# proportion
df_global['proportion'] = df_global['count']/total_basins

df_global['gid_0'] = 'GLB'
df_global['name_0'] = 'Global'
df_global['country_total'] = total_basins

df = pd.concat([df,df_global]).reset_index()
df['name'] = df.name_0.astype(str)


logger.info('Upload dataframe to Carto')
# upload data frame to Carto
test = pd.isna(df['name_0'])
df.to_csv('data.csv')
to_carto(df, 'ocn_calcs_014_eutrophication_risk', if_exists='replace')
