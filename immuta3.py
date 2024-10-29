import argparse
import sys
from os import environ
import os
import requests
import json
import urllib3
urllib3.disable_warnings()

#  % python3 immuta2.py -ho 'mybigimmuta.hosted.immutacloud.com' \
#  > -u 'service_account' \
#  > -p 'amazingpassword' \
#  > -r 'ACCOUNTADMIN'
#  > -i 150 \
#  > -k '1111222223333344445555'


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    description="""Script to (i) update dataSource connection username/password/role
                     (ii) set the schema project owner to new profileId
                     (iii) update a dataSources to have new profileId
                     (iv) update the schema project description to reflect changes.""",
                    epilog="""None
                        """)
    parser.add_argument("-ho", "--hostname",
                    help="""Immuta hostname  i.e. 'immuta.company.com'""")
    parser.add_argument("-u", "--username",
                    help="""New connection username.""")
    parser.add_argument("-p", "--password",
                    help="""New connection password.""")
    parser.add_argument("-r", "--role",
                    help="""New connection role.""")
    parser.add_argument("-i", "--id",
                    help="""profileId which owns dataSources].""")
    parser.add_argument("-k", "--key",
                    help="""Immuta API key.""")

    if len(sys.argv) < 4:
        parser.print_help()
        sys.exit()
    args = parser.parse_args()
    
    if args.key is not None:
        api_key = args.key
    elif args.key is None or args.key == "":
        logger.error("API key not set. Exiting.")
        sys.exit()

    if args.hostname is not None:
        host = args.hostname
    elif args.hostname is None or args.hostname == "":
        logger.error("Hostname not set. Exiting.")
        sys.exit()

    if args.id is not None:
        profileId = args.id
    elif args.id is None or args.id == "":
        logger.error("profileId not set. Exiting.")
        sys.exit()

    if args.username is not None:
        newConnectionUserName = args.username
    elif args.username is None or args.username == "":
        logger.error("username not set. Exiting.")
        sys.exit()

    if args.password is not None:
        newConnectionPassword = args.password
    elif args.password is None or args.password == "":
        logger.error("password not set. Exiting.")
        sys.exit()

    if args.role is not None:
        newConnectionRole = f"role={args.role}"
    elif args.role is None or args.role == "":
        logger.error("role not set. Exiting.")
        sys.exit()

    project_endpoint = '/project'
    bulk_endpoint = f'https://{host}/snowflake/bulk'
    url_project = f'https://{host}{project_endpoint}'

    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json'
    }

    # Make the request to get the list of projects
    response = requests.get(url_project, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        # Iterate over each item in the 'hits' list
        for item in data.get('hits', []):
            # Check if the 'type' is 'Schema', null means it is not a schemaProject
            if item.get('type') == 'Schema':
                # Get the project ID
                project_id = item['id']
                project_name = item['name']
                print(f"\u2713 Project name: \"{project_name}\" (project id: {project_id})")
                # Make a request to get details for each project
                url_project_details = f'https://{host}{project_endpoint}/{project_id}'
                response_project_details = requests.get(url_project_details, headers=headers)
                
                if response_project_details.status_code == 200:
                    project_details = response_project_details.json()
                    
                    url_project_members = f'https://{host}{project_endpoint}/{project_id}/members'
                    project_members_payload = {
                        "profileId": profileId,
                        "state": "owner"
                    }
                    project_members_response = requests.post(url_project_members, json=project_members_payload, headers=headers)
                    if project_members_response.status_code == 200:
                        print(f"Set owner {profileId} on project_id: {project_id}")

                    # Extract the schema detail
                    schema = project_details.get('schema')
                    
                    # Make a request to get the data sources for each project, we are getting just the database detail.  There could be other endpoints that are more efficient.  This is just an example on one of the way to get the database info.
                    url_project_data_sources = f'https://{host}{project_endpoint}/{project_id}/dataSources'
                    response_data_sources = requests.get(url_project_data_sources, headers=headers)
                    
                    if response_data_sources.status_code == 200:
                        data_sources = response_data_sources.json()
                        
                        # Make a list of all the data source id's
                        #dataSourceIds = [ds["dataSourceId"] for ds in data_sources["dataSources"]]
                        for data_source in data_sources["dataSources"]:
                            ds_id = data_source["dataSourceId"]
                            # print(f"data source ids: {dataSourceIds}")
                            add_owner_payload = {
                                "metadata":{},
                                "profileId":profileId,
                                "state":"owner"
                                }
                            owner_payload = json.dumps(add_owner_payload, indent=2)
                            # print(add_owner_payload)
                            url_add_owner_to_each_data_source = f'https://{host}/dataSource/{ds_id}/access'
                            response_add_owner_to_each_data_source = requests.post(url_add_owner_to_each_data_source, json=owner_payload, headers=headers)
                            if response_add_owner_to_each_data_source.status_code != 200:
                                print(f"Failed to update owner. Status Code: {response_add_owner_to_each_data_source.status_code}")
                                print(response_add_owner_to_each_data_source.text)
                        # Extract the connection string from the first data source
                        connection_string = None
                        if data_sources.get('dataSources'):
                            connection_string = data_sources['dataSources'][0].get('connectionString')
                        
                        # Split the connection string to extract necessary details
                        if connection_string:
                            # Extract database (after port)
                            database = connection_string.split('/')[1]
                            
                            # Rebuild the connection string
                            rebuilt_connection_string = f"{connection_string.split('@')[1]}/{schema}"
                            
                            # Build the JSON payload for the /snowflake/bulk endpoint.  Replace your username/password/role as fit.
                            payload = {
                                "handler": {
                                    "metadata": {
                                        "ssl": True,
                                        "port": 443,
                                        "database": database,
                                        "hostname": connection_string.split('@')[1].split('/')[0].split(':')[0],
                                        "username": connection_string.split('@')[0],
                                        "userFiles": [],
                                        "warehouse": "DEV_WH",
                                        "authenticationMethod": "userPassword",
                                        "connectionStringOptions": newConnectionRole,
                                        "username": newConnectionUserName,
                                        "password": newConnectionPassword
                                    }
                                },
                                "connectionString": rebuilt_connection_string
                            }
                            
                            # Convert payload to JSON string with double quotes
                            json_payload = json.dumps(payload, indent=2)
                            # print(json_payload)
                            # Uncomment this section when ready to execute the PUT request; i tested this with an if-condition for a project_id == xxx.
                            response_put = requests.put(bulk_endpoint, headers=headers, data=json_payload)
                            #if response_put.status_code == 200:
                            #    print(f"Schema project connnection updated")
                            #else:
                            #    print(f"Failed to update schema project {project_id} Status Code: {response_put.status_code}")
                            #    print(response_put.text)
                            
                            # Now tidy up and update the schema project "description"
                            newDescription=f"This project contains all data sources under the schema, {schema}, from {newConnectionUserName}@{rebuilt_connection_string}."
                            project_description_payload = { "description":newDescription }
                            json_project_description_payload = json.dumps(project_description_payload, indent=2)
                            # print(json_project_description_payload)
                            response_put_new_description = requests.put(url_project_details, headers=headers, data=json_project_description_payload)
                            #if response_put_new_description.status_code == 200:
                            #    print(f"Description updated")
                            #else:
                            #    print(f"Failed to update description for project {project_id} Status Code: {response_put_new_description.status_code}")
                            #    print(response_put_new_description.text)
