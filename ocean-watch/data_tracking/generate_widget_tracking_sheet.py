import os
import pandas as pd
import numpy as np
import LMIPy as lmi
import requests
import json  

DATASET_TRACKING_SHEET = os.path.join(os.getenv('OCEANWATCH_DATA_DIR'), 'ow_dataset_tracking_sheet.xlsx')

# Pull in dataset tracking sheet
datasets = pd.read_excel(DATASET_TRACKING_SHEET, header=0)
dataset_ids = [id for id in datasets['API_ID']]

ow_json_url = "https://raw.githubusercontent.com/resource-watch/resource-watch/develop/public/static/data/ocean-watch.json"
ow_json = pd.read_json(ow_json_url)

json_widgets = pd.DataFrame(columns=['widget_id', 'page', 'section_name', 'json_name'])
for page in ow_json.index:
    if page == 'intro':
        section_name = 'indicator'
        indicators = ow_json['production']['intro']['steps']
        for indicator in indicators:
            json_name=indicator['id']
            if 'content' in indicator:
                    content = indicator['content']
                    if 'widget' in content:
                        widget = content['widget'] 
                        for item in widget: 
                            if 'id' in item:
                                id = item['id']
                                json_widgets = json_widgets.append({'widget_id': id, 'page': page, 'section_name': section_name, 'json_name': json_name}, ignore_index=True)
                    if 'sections' in content:
                        section = content['sections']
                        for widget in section: 
                            widget = widget['widget']
                            for item in widget:
                                if 'id' in item:
                                    id = item['id']
                                    json_widgets = json_widgets.append({'widget_id': id, 'page': page, 'section_name': section_name, 'json_name': json_name}, ignore_index=True)

    if page == 'country-profile':
        indicators = ow_json['production']['country-profile'][0][0]['content'][0][0]['config']['indicators']
        for indicator in indicators:
            section_name = 'indicator'
            json_name = indicator['id']
            if 'widgets' in indicator:
                id = indicator['widgets'][0]['id']
                json_widgets = json_widgets.append({'widget_id': id, 'page': page, 'section_name': section_name, 'json_name': json_name}, ignore_index=True)
            if 'sections' in indicator: 
                widgets = indicator['sections']
                for widget in widgets:
                    widget = widget['widgets']
                    for item in widget:
                        if 'id' in item:
                            id = item['id']
                            json_widgets = json_widgets.append({'widget_id': id, 'page': page, 'section_name': section_name, 'json_name': json_name}, ignore_index=True)
        
        land_based_pollution = ow_json['production']['country-profile'][1][0]['content']
        for section in land_based_pollution:
            section_name = 'land_based_polution'
            for subsection in section: 
                if 'widget' in subsection:
                    id = subsection['widget']
                    json_name = subsection['comment']
                    json_widgets = json_widgets.append({'widget_id': id, 'page': page, 'section_name': section_name, 'json_name': json_name}, ignore_index=True)
        
        mov_ocean_pollutants = ow_json['production']['country-profile'][2][0]['content']
        for section in mov_ocean_pollutants:
            section_name = 'mov_ocean_pollutants'
            for subsection in section: 
                if 'widget' in subsection:
                    id = subsection['widget']
                    json_name = subsection['comment']
                    json_widgets = json_widgets.append({'widget_id': id, 'page': page, 'section_name': section_name, 'json_name': json_name}, ignore_index=True)
                if 'config' in subsection:
                    widgets=subsection['config']['widgets']
                    for widget in widgets:
                        json_name = subsection['config']['title']
                        id = widget
                        json_widgets = json_widgets.append({'widget_id': id, 'page': page, 'section_name': section_name, 'json_name': json_name}, ignore_index=True)

        goods_services = ow_json['production']['country-profile'][4][0]['content']
        for section in goods_services:
            section_name= 'value_goods_services'
            for subsection in section:
                if 'config' in subsection:
                    indicators=subsection['config']['indicators']
                    for indicator in indicators:
                        json_name = indicator['id']
                        if 'widgets' in indicator:
                            id = indicator['widgets'][0]['id']
                            json_widgets = json_widgets.append({'widget_id': id, 'page': page, 'section_name': section_name, 'json_name': json_name}, ignore_index=True)
                if 'widget' in subsection:
                    id = subsection['widget']
                    json_name = subsection['comment']
                    json_widgets = json_widgets.append({'widget_id': id, 'page': page, 'section_name': section_name, 'json_name': json_name}, ignore_index=True)

json_widget_ids = [id for id in json_widgets['widget_id']]
json_widget_ids = np.unique(np.array(json_widget_ids))

ow_widgets = pd.DataFrame(columns=['widget_id', 'dataset_id', 'widget_name','widget_description','caption','links'])
for dataset_id in dataset_ids:
    dataset_object = lmi.Dataset(dataset_id)
    widget_list = dataset_object.widget
    for widget in widget_list:
        widget_id = widget.id
        if widget_id in json_widget_ids:
            widget_object = lmi.Widget(widget_id)
            widget_name = widget_object.attributes['name']
            widget_description = widget_object.attributes['description']
            try: 
                metadata_url =  f'http://api.resourcewatch.org/v1/dataset/{dataset_id}/widget/{widget_id}/metadata'
                metadata_object = json.loads(requests.get(metadata_url).text)['data'][0]
                caption = metadata_object['attributes']['info']['caption']
                links = metadata_object['attributes']['info']['widgetLinks']
            except:
                caption = ''
                links = ''
            ow_widgets = ow_widgets.append({'widget_id': widget_id, 'dataset_id': dataset_id, 'widget_name': widget_name, 'widget_description': widget_description, 'caption': caption, 'links': links }, ignore_index=True)

widget_table =  pd.merge(json_widgets,ow_widgets, on='widget_id', how='left')    
widget_table = widget_table[['widget_id','dataset_id', 'page',  'section_name', 'json_name', 'widget_name', 'widget_description', 'caption', 'links']]       

# path to processed table
processed_table = os.path.join(os.getenv('OCEANWATCH_DATA_DIR'), 'ow_widget_tracking_sheet.xlsx')

# save processed table to the Ocean Watch Data Directory
widget_table.to_excel(processed_table, index= False)