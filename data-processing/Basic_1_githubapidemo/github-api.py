
import base64
import requests
import os
from dotenv import load_dotenv
# ---------------------------
# CONFIGURATION
# ---------------------------


# ---------------------------
# LOAD CONFIGURATION
# ---------------------------
load_dotenv()  # load variables from .env

TOKEN = os.getenv("GITHUB_TOKEN")
OWNER = os.getenv("GITHUB_OWNER")
REPO = os.getenv("GITHUB_REPO")

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json"
}


# ---------------------------
# 1. GET Repository Details
# ---------------------------
def get_repo(owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}"
    response = requests.get(url, headers=headers)
    print("GET Repo Response:", response.json())

# ---------------------------
# 2. POST Create Repository
# ---------------------------
def create_repo(repo_name):
    url = "https://api.github.com/user/repos"
    data = {
        "name": repo_name,
        "description": "Repository created through GitHub API",
        "private": False
    }
    response = requests.post(url, headers=headers, json=data)
    print("POST Create Repo Response:", response.json())

# ---------------------------
# 3. PUT Upload or Update File
# ---------------------------
def upload_file(owner, repo, path, content_text, message):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    content = base64.b64encode(content_text.encode()).decode()

    data = {
        "message": message,
        "content": content
    }

    response = requests.put(url, headers=headers, json=data)
    print("PUT Upload File Response:", response.json())

# ---------------------------
# 4. PATCH Update Repository Metadata
# ---------------------------
def update_repo(owner, repo, new_name, new_desc):
    url = f"https://api.github.com/repos/{owner}/{repo}"

    data = {
        "name": new_name,
        "description": new_desc
    }

    response = requests.patch(url, headers=headers, json=data)
    print("PATCH Update Repo Response:", response.json())

# ---------------------------
# 5. DELETE Repository
# ---------------------------
def delete_repo(owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}"
    response = requests.delete(url, headers=headers)
    print("DELETE Repo Status Code:", response.status_code)

# ---------------------------
# MAIN WORKFLOW (UNCOMMENT TO RUN)
# ---------------------------
if __name__ == '__main__':
# # Step 1: Create a new repository
#     create_repo(REPO)

# # Step 2: Get repository details
#     get_repo(OWNER, REPO)

# # Step 3: Upload README.md
#     upload_file(OWNER, REPO, "README.md", "# Hello from GitHub API", "Add README via API")

# # Step 4: Update repository metadata
#     update_repo(OWNER, REPO, "automation-demo-renamed", "Updated description via API")

# # Step 5: Delete repository
    delete_repo(OWNER, "automation-demo-renamed")
