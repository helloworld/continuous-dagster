# dagster-github

## Introduction
This library provides an integration with github apps, to support performing various automation operations within your github repositories and with the tighter permissions scopes that github apps allow for vs using a personal token.

Presently, it provides a thin wrapper on the [github v4 graphql API](https://developer.github.com/v4/).

## Getting Started
To use this integration, you'll first need to create a Github App for it.

1. **Create App**: Follow the instructions in [https://developer.github.com/apps/quickstart-guides/setting-up-your-development-environment/](https://developer.github.com/apps/quickstart-guides/setting-up-your-development-environment/), You will end up with a private key and App ID, which will be used when configuring the dagster-github resource. **Note** you will need to grant your app the relevent permissions for the API requests you want to make, for example to post issues it will need read/write access for the issues repository permission, more info on github application permissions can be found [here](https://developer.github.com/v3/apps/permissions/)

2. **Install App**: Follow the instructions in [https://developer.github.com/apps/quickstart-guides/setting-up-your-development-environment/#step-7-install-the-app-on-your-account](https://developer.github.com/apps/quickstart-guides/setting-up-your-development-environment/#step-7-install-the-app-on-your-account)

3. **Find your installation_id**: You can pull this from the github app administration page, `https://github.com/apps/<app-name>/installations/<installation_id>`. **Note** if your app is installed more than once you can also programatically retrieve these IDs.

Sharing your App ID and Installation ID is fine, but make sure that the Private Key for your app is stored securily.

## Posting Issues
Now, you can create issues in Github from Dagster with the Github resource:

```python
import os

from dagster import solid, execute_pipeline, ModeDefinition
from dagster_github import github_resource


@solid(resource_defs={'github'})
def github_solid(context):
    context.resources.github.create_issue(
        repo_name='dagster',
        repo_owner='dagster-io',
        title='Dagster\'s first github issue',
        body='this open source thing seems like a pretty good idea',
    )

@pipeline(
    mode_defs=[ModeDefinition(resource_defs={'github': github_resource})],
)
def github_pipeline():
    github_solid()

execute_pipeline(
    github_pipeline, {'resources': {'github': {'config': {
        "github_app_id": os.getenv('GITHUB_APP_ID'),
        "github_app_private_rsa_key": os.getenv('GITHUB_PRIVATE_KEY'),
        "github_installation_id": os.getenv('GITHUB_INSTALLATION_ID'),
    }}}}
)
```
Run the above code, and you'll see the issue appear in github:
<img width="636" alt="Screen Shot 2020-01-06 at 9 23 16 PM" src="https://user-images.githubusercontent.com/5943242/72079909-c6012300-32c9-11ea-8acc-19e6f5f3d067.png">

By provisioning `github_resource` as a Dagster pipeline resource, you can post to Github from within any solid execution.


## Executing GraphQL queries
```python
import os

from dagster import solid, execute_pipeline, ModeDefinition
from dagster_github import github_resource


@solid(resource_defs={'github'})
def github_solid(context):
    context.resources.github.execute(
        query="""
        query get_repo_id($repo_name: String!, $repo_owner: String!) {
            repository(name: $repo_name, owner: $repo_owner) {
                id
            }
        }
        """,
        variables={"repo_name": repo_name, "repo_owner": repo_owner},
    )

@pipeline(
    mode_defs=[ModeDefinition(resource_defs={'github': github_resource})],
)
def github_pipeline():
    github_solid()

execute_pipeline(
    github_pipeline, {'resources': {'github': {'config': {
        "github_app_id": os.getenv('GITHUB_APP_ID'),
        "github_app_private_rsa_key": os.getenv('GITHUB_PRIVATE_KEY'),
        "github_installation_id": os.getenv('GITHUB_INSTALLATION_ID'),
    }}}}
)
```
