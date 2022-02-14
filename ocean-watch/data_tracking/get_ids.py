import os
from random import paretovariate
from typing import OrderedDict
import pandas as pd
import LMIPy as lmi
import json
import requests
import re

# The content on the OceanWatch pages are configured using a json file managed by Vizzuality
ow_json_url = "https://raw.githubusercontent.com/resource-watch/resource-watch/develop/public/static/data/ocean-watch.json"

# read the content as a string, so we can search for assets (dataset, widget, and layer) ids
res = requests.get(ow_json_url)
ow_json = json.loads(res.text)
json_string = json.dumps(ow_json)

# create a regex pattern that matches RW dataset, widget, and layer ids
pattern = re.compile('[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}')

# find all matches in the OW json
ids = pattern.findall(json_string)

# create empty dataframes to store information about each asset
widgets = pd.DataFrame()
layers = pd.DataFrame()
datasets = pd.DataFrame()

def get_dataset_info(id):
    object = lmi.Dataset(id)
    wri_id = object.attributes['name'].split(' ')[0]
    name = object.attributes['name'].replace(wri_id,"").lstrip(" ")
    connector_url = object.attributes['connectorUrl']
    table_name =  object.attributes['tableName']
    return id, wri_id, name


def get_widget_info(id):
    object = lmi.Widget(id)
    name = object.attributes['name']
    parent_dataset_id= object.attributes['dataset']
    return id, name, parent_dataset_id

def get_layer_info(id):
    object = lmi.Layer(id)
    name = object.attributes['name']
    parent_dataset_id= object.attributes['dataset']
    return id, name, parent_dataset_id

def get_layers_from_widget(id):
    object = lmi.Widget(id)
    if 'paramsConfig' in object.attributes['widgetConfig']:
        layers = pattern.findall(json.dumps(object.attributes['widgetConfig']['paramsConfig']))
    for layer in layers:
        layer_object = lmi.Layer(layer)
        layer_name = layer_object.attributes['name']
        parent_dataset_id= layer_object.attributes['dataset']
        return layer, id, parent_dataset_id

for id in ids:
    try:
        id, wri_id, name = get_dataset_info(id)
        datasets = datasets.append({'id': id, 'wri_id': wri_id, 'name': name}, ignore_index=True)
    except: 
        try: 
            id, name, parent_dataset_id = get_widget_info(id)
            widgets = widgets.append({'id': id, 'name': name, 'parent_dataset_id': parent_dataset_id}, ignore_index=True)
            try: 
                layer, id, parent_dataset_id= get_layers_from_widget(id)
                layers = layers.append({'layer_id': layer, 'widget_id': id, 'parent_dataset_id': parent_dataset_id}, ignore_index=True)
            except:
                pass
        except:
            try:
                id, name, parent_dataset_id = get_layer_info(id)
                layers = layers.append({'layer_id': id, 'widget_id': 'mini-explore', 'parent_dataset_id': parent_dataset_id}, ignore_index=True)
            except: 
                pass
widgets = widgets.drop_duplicates(subset=['id'])
layers = layers.drop_duplicates(subset=['layer_id','widget_id'])

for id in layers['parent_dataset_id']:
    if id not in datasets['id']:
        id, wri_id, name = get_dataset_info(id)
        datasets = datasets.append({'id': id, 'wri_id': wri_id, 'name': name}, ignore_index=True)

for id in widgets['parent_dataset_id']:
    if id not in datasets['id']:
        id, wri_id, name = get_dataset_info(id)
        datasets = datasets.append({'id': id, 'wri_id': wri_id, 'name': name}, ignore_index=True)

datasets = datasets.drop_duplicates(subset=['id'])

print("hi")
    