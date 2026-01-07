import os
import requests
import getpass, pprint, time, os, json
import json
from dotenv import load_dotenv, find_dotenv

# download date
date = "06-01-2021"

# Set input directory, change working directory
inDir = os.path.dirname(os.path.abspath(__file__))
os.chdir(inDir)                                         # Change to working directory
api = 'https://appeears.earthdatacloud.nasa.gov/api/'   # Set the AρρEEARS API to a variable
print(f"Working Directory: {inDir}")

# login using .env
load_dotenv(find_dotenv())
user = os.environ.get('NASA_USER')
password = os.environ.get('NASA_PASSWORD')

token_response = requests.post('{}login'.format(api), auth=(user, password)).json()    # Insert API URL, call login service, provide credentials & return json
del user, password                                                              # Remove user and password information
token_response                                                                  # Print response

token = token_response['token']                      # Save login token to a variable
head = {'Authorization': 'Bearer {}'.format(token)}  # Create a header to store token information, needed to submit a request


# task params
task_name = date
task_type = 'area'                  # Type of task, area or point
proj = 'geographic'                 # Set output projection 
outFormat = 'geotiff'               # Set output file format type
startDate = date                    # Start of the date range for which to extract data: MM-DD-YYYY
endDate = date                      # End of the date range for which to extract data: MM-DD-YYYY
prodLayer = [
      { "layer": "_1_km_16_days_EVI", "product": "MOD13A2.061" },
      { "layer": "_1_km_16_days_NDVI", "product": "MOD13A2.061" },
      { "layer": "Geophysical_Data_surface_temp", "product": "SPL4SMGP.008" },
      { "layer": "Geophysical_Data_sm_rootzone_wetness", "product": "SPL4SMGP.008" },
      { "layer": "Geophysical_Data_land_evapotranspiration_flux", "product": "SPL4SMGP.008" },
      { "layer": "Geophysical_Data_sm_surface_wetness", "product": "SPL4SMGP.008" }
    ]

try:
    with open('BC_mask.json', 'r', encoding='utf-8') as f:
        ROI = json.load(f)
except FileNotFoundError:
    print(f"Error: Could not find 'BC_mask.json' in {inDir}")
    exit(1)


task = {
    'task_type': task_type,
    'task_name': task_name,
    'params': {
         'dates': [
         {
             'startDate': startDate,
             'endDate': endDate
         }],
         'layers': prodLayer,
         'output': {
                 'format': {
                         'type': outFormat}, 
                         'projection': proj},
         'geo': ROI,
    }
}


task_response = requests.post(f'{api}task', json=task, headers=head).json()    # Post json to the API task service, return response as json
print(task_response)                                                                             # Print task response

task_id = task_response['task_id']
print(task_id)


# Ping API until request is complete, then continue to Section 4
starttime = time.time()
while requests.get('{}task/{}'.format(api, task_id), headers=head).json()['status'] != 'done':
    print(requests.get('{}task/{}'.format(api, task_id), headers=head).json()['status'])
    time.sleep(20.0 - ((time.time() - starttime) % 20.0))
print(requests.get('{}task/{}'.format(api, task_id), headers=head).json()['status'])



# Base output directory: ../../../data/raw/nasa relative to this script
# In railway, we need to change this output path to the persistent storage path (Railway Volumes)
VOLUME_PATH = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "/data")
baseOutDir = os.path.join(VOLUME_PATH, "raw", "nasa")
destDir = os.path.join(baseOutDir, task_name)
os.makedirs(destDir, exist_ok=True)



folder_rules = {
    "rootzone_wetness": "rootzone_wetness",
    "evapotranspiration": "evapotranspiration",
    "surface_wetness": "surface_wetness",
    "surface_temp": "surface_temp",
    "evi": "evi",
    "ndvi": "ndvi"
}

bundle = requests.get('{}bundle/{}'.format(api,task_id), headers=head).json()  # Call API and return bundle contents for the task_id as json
print(bundle)                                                   # Print bundle contents

files = {}                                                       # Create empty dictionary
for f in bundle['files']: files[f['file_id']] = f['file_name']   # Fill dictionary with file_id as keys and file_name as values
files    

for f in files:
    file_name = files[f]

    if not file_name.lower().endswith(('.tif', '.tiff')):
        continue
    if 'quality' in file_name.lower():
        continue

    current_save_dir = destDir

    for keyword, subfolder in folder_rules.items():
        if keyword in file_name.lower():
            current_save_dir = os.path.join(destDir, subfolder)
            break
    os.makedirs(current_save_dir, exist_ok=True)
    dl = requests.get('{}bundle/{}/{}'.format(api, task_id, f), headers=head, stream=True, allow_redirects = 'True')  # Get a stream to the bundle file
    if files[f].endswith('.tif'):
        filename = files[f].split('/')[1]
    else:
        filename = files[f] 
    filepath = os.path.join(current_save_dir, filename)                                                       # Create output file path
    with open(filepath, 'wb') as f:                                                                  # Write file to dest dir
        for data in dl.iter_content(chunk_size=8192): f.write(data) 

print('Downloaded files can be found at: {}'.format(destDir))