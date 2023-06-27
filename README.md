# Git shortcuts
Handy tools that automate routine tasks

## Getting the repository
1. Go to repos folder
1. `git clone https://***.git`

## Making changes
1. `git checkout dev`
1. `git pull`
1. Make feature changes and tests
1. `git checkout -b feature/JIRA-TICKET-AND-SOME-TITLE` (example: `git checkout -b feature/ADA2-42-add-cool-stuff`)
1. `git add .`
1. `git commit -m "JIRA-TICKET Commit message"` (example: `git commit -m "ADA2-42 Did an excellent job making good stuff happen. Excited about the changes!"`)
1. `git push origin feature/JIRA-TICKET-AND-SOME-TITLE`
1. Create merge request from `feature/JIRA-TICKET-AND-SOME-TITLE` to `dev`
1. Obtain approval(s)
1. Merge pull request to dev
1. Test the changes in the cloud
1. Create merge request from `dev` to `prod`
1. Obtain approval(s)
1. Make appropriate release announcements
1. Merge pull request and verify the output
1. Put a tag on the release commit (example: v2.13.0)
1. `git checkout dev`
1. `git branch -d feature/JIRA-TICKET-AND-SOME-TITLE`

## Revert to previous release (only the release maser can do that)
1. `git checkout prod`
1. `git pull`
1. `git reset --hard RELEASE_TAG_NAME` (RELEASE_TAG_NAME example: v2.12.0)
1. `git push --force origin prod`

## Update commit message (when forgot to add the jira ticket)
1. `git commit --amend -m "JIRA-TICKET Commit message"`
1. `git push origin feature/JIRA-TICKET-AND-SOME-TITLE`

## Update branch name (when forgot to start it with either `feature` or `bugfix`)
1. `git branch -m feature/JIRA-TICKET-AND-SOME-TITLE`
1. `git push origin feature/JIRA-TICKET-AND-SOME-TITLE`

## Other useful comands
1. `git status` - check repo status
1. `git diff` - check the uncommited changes
1. `git branch -a` - list all the branches
1. `git checkout .` - revert all local uncommited changes
1. `git log --all --graph --decorate` - display the log history
1. `git remote prune origin` - remove branches that are no longer in the remote repo
1. Username setup
    * `git config --global user.name "Your Name"`
    * `git config --global user.email you@example.com`
1. `git commit --amend --author="User Name <username@mail.com>"` - change user in the commit

# Virtual environment shortcuts

## Python venv
1. `python -m venv .venv_name` creates a virtual environment in a desired folder
1. Activate the environment
    - `.venv_name\Scripts\Activate.ps1` for Windows
    - `source .venv_name\bin\activate` for Mac
1. If environment configuration is in poetry
    - `pip install poetry` installs the poetry
    - `cd path_to_the_folder_with_pyproject.toml` goes to the directory with the configuration file
    - `poetry install` installs all the dependencies needed for the environment
1. If environment configuration is in requirements.txt
    - `cd path_to_the_folder_with_requirements.txt` goes to the directory with the configuration file
    - `pip install -r requirements.txt`
1. `pip install ipykernel` if need to set a kernel in jupyter notebook
1. `python -m pytest` runs the unit tests
1. `deactivate` deactivates the environment
