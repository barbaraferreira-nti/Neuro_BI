import requests

# Your personal API token
api_token = "pk_87400670_1GCFTG5FMDBR9B5UH1UKJFKJX3U5JMDS"
# The base URL for ClickUp API v2
base_url = "https://api.clickup.com/api/v2"

headers = {
    "Authorization": api_token,
    "Content-Type": "application/json"
}

# Example: Get all Teams (Workspaces) you belong to
def get_teams(team_id):
    response = requests.get(f"{base_url}/team/{team_id}/space", headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return f"Error: {response.status_code}"
    
def get_folders(space_id):
    response = requests.get(f"{base_url}/space/{space_id}/folder", headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return f"Error: {response.status_code}"
    
def get_tasks(list_id):
    response = requests.get(f"{base_url}/list/{list_id}/task", headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return f"Error: {response.status_code}"

def get_task(task_id):
    response = requests.get(f"{base_url}/task/{task_id}", headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return f"Error: {response.status_code}"
    
def get_comments(task_id):
    response = requests.get(f"{base_url}/task/{task_id}/comment", headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return f"Error: {response.status_code}"

teams = get_comments("868hpk0c5")
print(teams)