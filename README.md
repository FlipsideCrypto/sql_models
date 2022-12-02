# dbt-replacement

Please follow the below steps to do the dbt cloud replacement:
## Step 1: Checkout the branch 
`git pull`

`git checkout AN-2377-added-schedule-dbt-run-template-github-actions`

## Step 2: Created a new temp branch based on main
`git checkout -b tmp-dbt-replacement`

## Step 3: Checked out the changes
`git checkout AN-2377-added-schedule-dbt-run-template-github-actions .github/workflows`

## Step 4: Merged it to main

## Step 5: Set up the repository secrets
Go to `Github Repo -> Settings -> Environments`, click create new environment, call it `workflow`, and then type in the repo like this by adding the profiles under the current folder
![environment](./images/environment_screenshot.png)

## Step 6: END
Updated the `profiles.yml` file target from dev to prod if you would like the refresh happen in prod