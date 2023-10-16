import json
import os
import sys

# initialize an empty dictionary
erd_dict = {}

# add an element to the dictionary
erd_dict['tables'] = []

# create a function that will iterate over erd_dict['tables'] and find the current_table_name and return the index
def find_table_index(current_table_name):
    for index, table in enumerate(erd_dict['tables']):
        if table['table_name'] == current_table_name:
            return index

# Populate all tables and primary keys

# initialize a new table
new_table = True
current_table_name = ""
table_index = -9999

with open('/tmp/ecto_erd.qdbd', 'r') as f:
    # read file line by line
    for line in f:
        if new_table == True:
            # add a new table to the dictionary
            current_table_name = line.strip()
            table_index = find_table_index(current_table_name)
            if table_index == None:
                erd_dict['tables'].append({'table_name': line.strip(), 'referenced_by': []})
                table_index = find_table_index(current_table_name)
            new_table = False

        if line.strip() == '---':
            continue

        # if the line is empty, then we are at the end of the table
        if line.strip() == '':
            new_table = True
            current_table_name = ""
            table_index = -9999
            continue

        # find substring PK in the line
        if 'PK' in line:
            split_line = line.split()
            # get first element of the split_line
            primary_key = split_line[0].strip()

            # add an element to the dictionary, with key as primary_key and value as the primary_key, where the table_name is equal to current_table_name

            erd_dict['tables'][table_index]['primary_key'] = primary_key


        # find substring FK in the line
        if ' FK >- ' in line:
            split_line = line.split(' FK >- ')
            # get first element of the split_line
            first = split_line[0].strip()
            last = split_line[1].strip()

            first_split = first.split()
            foreign_key = first_split[0].strip()

            last_split = last.split('.')
            foreign_table = last_split[0].strip()

            # add an element to the dictionary, with key as foreign_key and value as the foreign_key, where the table_name is equal to current_table_name
            primary_key_table_index = find_table_index(foreign_table)
            if primary_key_table_index == None:
                erd_dict['tables'].append({'table_name': foreign_table, 'referenced_by': []})
                primary_key_table_index = find_table_index(foreign_table)

            if not (current_table_name == 'favorite_video_styles' and foreign_table == 'users'):
                erd_dict['tables'][primary_key_table_index]['referenced_by'].append({'table_name': current_table_name, 'foreign_key': foreign_key})

# Print the dictionary
print(json.dumps(erd_dict, indent=4))

# Dump the dictionary to a json file
with open('/tmp/ecto_erd.json', 'w') as f:
    f.write(json.dumps(erd_dict, indent=4))

# Find environment to prepare for S3 Upload
ENVIRONMENT = sys.argv[1]

if ENVIRONMENT == None or ENVIRONMENT.strip() == "" or ENVIRONMENT.strip() == "local":
    ENVIRONMENT = "local"

if ENVIRONMENT.strip() == "prod":
    BUCKET = "fw-temp-gdpr-deleted-data-loop-prod"
else:
    BUCKET = "fw-temp-gdpr-deleted-data-loop-staging"

ENVIRONMENT = ENVIRONMENT.strip()

# Print the bucket and environment
print("BUCKET: " + BUCKET)
print("ENVIRONMENT: " + ENVIRONMENT)

# Command to upload json file to S3
s3_upload_command = "aws s3 cp /tmp/ecto_erd.json s3://" + BUCKET + "/" + ENVIRONMENT +  "/ecto_erd.json"
print(s3_upload_command)

# Upload json file to S3
# Deliberately not using boto3 to avoid dependency
os.system(s3_upload_command)
