import LMIPy as lmi
import os
import pandas as pd

# open the text file with a list of layer ids while removing line breaks
orphan_layers_path= os.path.join(os.getenv('DOWNLOAD_DIR'), 'dataset_ids.txt')
with open(orphan_layers_path, 'r') as file:
    orphan_layers_str = ""
    for readline in file: 
        line_strip = readline.strip()
        orphan_layers_str += line_strip

# remove extra characters and split layer_ids into a list
orphan_layers = orphan_layers_str.strip(']["').replace("'", "").split(',')

# create dataframe to store layer information
orphan_layers_df = pd.DataFrame(columns=['layer_id', 'name', 'createdAt','updatedAt'])

# loop through list of layers, get information from api, and add it to the dataframe
for layer_id in orphan_layers:
    layer_object = lmi.Layer(layer_id)
    name = layer_object.attributes['name']
    createdAt = layer_object.attributes['createdAt']
    updatedAt = layer_object.attributes['updatedAt']
    orphan_layers_df = orphan_layers_df.append({'layer_id': layer_id, 'name': name, 'createdAt': createdAt, 'updatedAt': updatedAt}, ignore_index=True)

# path to processed table
processed_table = os.path.join(os.getenv('DOWNLOAD_DIR'), 'orphaned_layers.xlsx')

# save processed table
orphan_layers_df.to_excel(processed_table, index= False)