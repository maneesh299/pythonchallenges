from google.cloud import asset_v1
from google.oauth2 import service_account
import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime
import time

# Load credentials from JSON file
def get_iam_policies_for_projects(org_id):
        """
        Creates a list of all projects and metadata correspding to the child of
        each row in the folders_df which contains a list of parent folders
        and appends to dataframe

        :param: folders_df: A dataframe containing a list of folders

        :return: projects_list A dataframe containing a list of all projects
        """
        json_root = "results"
        projects_list = pd.DataFrame()
        credentials = service_account.Credentials.from_service_account_file("/home/sa.json")
        service = build('cloudasset', 'v1', credentials=credentials)
        try:
            request = service.v1().searchAllIamPolicies(scope=org_id)
            data = request.execute()
            df = pd.json_normalize(data[json_root])
            while request is not None:
                    request = service.v1().searchAllIamPolicies_next(request, data)
                    if (request is None):
                        break
                    else:
                        data = request.execute()
                        df = pd.concat([df, pd.json_normalize(data[json_root])])
            df['extract_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            projects_list = pd.concat([projects_list, df])    
        except KeyError:
            pass

        projects_list.rename(columns=lambda x: x.lower().replace('.', '_').replace('-', '_'), inplace=True)
        projects_list.reset_index(drop=True, inplace=True)
        return projects_list
iams_full_resource = get_iam_policies_for_projects("organizations/1234556789234")
iams_full_resource.to_csv("output.csv", index=True)    
print(iams_full_resource)
