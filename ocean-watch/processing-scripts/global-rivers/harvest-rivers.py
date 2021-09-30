import osm2geojson 
import requests
import json
import fiona
from fiona.crs import from_epsg
import time
import logging


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

# Define grid to iterate through a series bboxs that cover world starting from the top left and moving to the bottom right coordinate

# Define the start coordinate
x_start = -180
y_start = 90
# Define the grid size
x_size = 10
y_size = 10
# Define the stop coordinate
x_stop = 180
y_stop = -90

# Set up to query Overpass API
# Define the url for the hosted Overpass API (other urls can be found at https://wiki.openstreetmap.org/wiki/Overpass_API)
overpass_url = "https://lz4.overpass-api.de/api/interpreter"

# template for Overpass QL query
template = """
[out:json]
[timeout:180];
(way{bbox}[waterway=river]["name"];
way{bbox}[waterway=canal]["name"];);
(._;>;);out;
"""

# Define a list to store the elements
rivers=[]

# Iterate through the coordinates of the bbox grid, request the defined query, and save the geometries of the resulting elements

# Start at the left-most x coordinate
x_left = x_start
# Move from left to right until the left most x-cordinate is reached
while x_left <= x_stop - x_size:
    # Start at the top y coordinate
    y_top = y_start
    # move top to bottom until the bottom-most y coodinate is reached
    while y_top >= y_stop + y_size:
        # Define the resulting bbox
        bbox_tuple = (y_top-y_size, x_left, y_top, x_left+x_size)
        logger.info("Getting rivers for: " + str(bbox_tuple))
        # Attempt to query the data, moving on after 10 failed attempts
        for attempt in range(10):                
            try: 
                # Format the query with the current bbox
                overpass_query= template.format(time=time[attempt],bbox=bbox_tuple)
                # Request elements from the query
                response = requests.get(overpass_url, params={'data': overpass_query,}, headers = {'User-agent': 'wriuser'})
                # save the resulting json
                data = response.json()
                # convert the data from json to geojson format                  
                geojson= osm2geojson.json2geojson(data)
                # move on to next bbox
                break
            except: 
                logger.warning("error " + str(response.status_code) + "- retrying in 3 minutes")
                # wait three minutes before re-requesting 
                time.sleep(180)                
        else:
            logger.error("unable to fetch data for" + str(bbox_tuple))
            # move on to next bbox after 10 failed attemps
            break            
        for way in geojson['features']: 
            rivers.append(way)
        logger.info(str(len(geojson['features'])) + " rivers added. Total rivers: " + str(len(rivers)))
        # move to the bbox lower
        y_top = y_top - y_size 
    # move to the bbox to the right           
    x_left = x_left + x_size

# Save the resulting elements as a shapefile

# Define the schema
schema = {'geometry': 'LineString', 'properties': {'Name':'str:80'}}
# File to save the elements
shapeout = "osm_rivers_edit.shp"
# Write each element into the shapefile
with fiona.open(shapeout, 'w',crs=from_epsg(4326),driver='ESRI Shapefile', schema=schema) as output:
    for way in rivers:
        # the shapefile geometry use (lon,lat) 
        line = {'type': 'LineString', 'coordinates': way['geometry']['coordinates']}
        # the shapefile properties
        prop = {'Name': way['properties']['tags']['name']}
        try:
            output.write({'geometry': line, 'properties':prop})  
        except:
            logger.warning("could not add " + prop["Name"])