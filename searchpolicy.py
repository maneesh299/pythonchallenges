from google.cloud import asset_v1
from google.oauth2 import service_account
import pandas as pd
from googleapiclient.discovery import build
from datetime import datetime
import time
import random
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from googleapiclient.errors import HttpError


def get_iam_policies_for_projects(org_id, max_retries=5, initial_wait=1, max_wait=60):
    json_root = "results"
    projects_list = pd.DataFrame()
    credentials = service_account.Credentials.from_service_account_file("/home/mysa.json")
    service = build('cloudasset', 'v1', credentials=credentials)

    @retry(
        stop=stop_after_attempt(max_retries),
        wait=wait_exponential(multiplier=1, min=initial_wait, max=max_wait),
        retry=retry_if_exception_type((HttpError)),
        before_sleep=lambda retry_state: print(
            f"Rate limit hit. Attempt {retry_state.attempt_number}/{max_retries}. "
            f"Retrying in {retry_state.next_action.sleep} seconds..."
        )
    )
    def execute_with_retry(request):
        try:
            return request.execute()
        except HttpError as e:
            if e.resp.status == 429:  
                print(f"Rate limit exceeded: {e}")
                raise  
            else:
                print(f"Non-retryable HTTP error: {e}")
                raise  
    
    try:
        request = service.v1().searchAllIamPolicies(scope=org_id)
        data = execute_with_retry(request)
        
        if json_root in data:
            df = pd.json_normalize(data[json_root])
            
            
            while request is not None:
                request = service.v1().searchAllIamPolicies_next(request, data)
                
                if request is None:
                    break
                
                data = execute_with_retry(request)
                
                if json_root in data:
                    df = pd.concat([df, pd.json_normalize(data[json_root])])
            
            df['extract_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            projects_list = pd.concat([projects_list, df])
            
    except Exception as e:
        print(f"Unhandled error: {e}")
    
    if not projects_list.empty:
        projects_list.rename(columns=lambda x: x.lower().replace('.', '_').replace('-', '_'), inplace=True)
        projects_list.reset_index(drop=True, inplace=True)
    
    return projects_list


if __name__ == "__main__":
    iams_full_resource = get_iam_policies_for_projects("organizations/12356778899")
    iams_full_resource.to_csv("output.csv", index=True)
    print(iams_full_resource)
