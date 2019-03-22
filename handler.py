import datetime
import json
import logging

import boto3
import requests


def load_params(namespace: str, env: str, region_name: str = 'us-east-1') -> dict:
    """
    Load parameters from SSM Parameter Store.
    Function from https://www.davehall.com.au/blog/dave/2018/08/26/aws-parameter-store

    :namespace: The application namespace.
    :env: The current application environment.
    :return: The config loaded from Parameter Store.
    """
    config = {}
    path = f'/{namespace}/{env}/'
    ssm = boto3.client('ssm', region_name=region_name)
    more = None
    args = {'Path': path, 'Recursive': True, 'WithDecryption': True}
    while more is not False:
        if more:
            args['NextToken'] = more
        params = ssm.get_parameters_by_path(**args)
        for param in params['Parameters']:
            key = param['Name'].split('/')[3]
            config[key] = param['Value']
        more = params.get('NextToken', False)
    return config


def check_pending_pull_requests(event: dict, context) -> dict:
    """
    Checks pending pull requests and sends a notification

    """

    ssm_parameters = load_params('dev_tools', 'dev')

    # GraphQL query that gets all OPEN pull requests for each repo within an organization
    # @todo: fix hardcoded pagination
    
    query = '''
{
  organization(login: "github_organization") {
    repositories(first: 100) {
      totalCount,
      edges {
        node {
          name
          pullRequests(first: 20, states: OPEN) {
            totalCount,
            edges {
              node {
                reviewRequests(first: 20) {
                  totalCount
                  edges {
                    node {
                      requestedReviewer {
                        __typename
                        ... on User {
                          login
                        }
                        ... on Team {
                          name
                        }                        
                      }
                    }
                  }
                }
                title,
                createdAt
                url,
                author {
                  login
                },
                assignees(first: 10) {
                  totalCount,
                  edges {
                    node {
                      login
                    }
                  }                  
                },
                reviews(first: 20) {
                  totalCount
                  edges {
                    node {
                      author {
                        login
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
'''

    headers = {"Authorization": f"token {ssm_parameters['github_access_token']}"}
    result = requests.post('https://api.github.com/graphql',
                           json={'query': query.replace('github_organization', ssm_parameters['github_organization'])},
                           headers=headers)

    if result.status_code == 200:
        data = result.json()
    else:
        raise Exception(f'Query failed to run by returning code of {result.status_code}. {query}')

    if not data['data']['organization']:
        return {}

    # for all repos, check if there are OPEN pull requests and get relevant info about the status.
    message = ''
    for repository in data['data']['organization']['repositories']['edges']:
        repository_name = repository['node']['name']
        if repository['node']['pullRequests']['totalCount'] > 0:

            for pull_node in repository['node']['pullRequests']['edges']:

                pull = pull_node['node']

                # figure out the time that has passed since the pull request was created
                now = datetime.datetime.utcnow()

                created = datetime.datetime.strptime(pull['createdAt'], '%Y-%m-%dT%H:%M:%SZ')

                time_diff = (now - created)

                if time_diff.days:
                    time_since_created = f"{time_diff.days} day{'' if time_diff.days == 1 else 's'} ago"
                    # add warning emojis for old pull requests
                    if 2 < time_diff.days < 5:
                        time_since_created += ' :warning:'
                    elif time_diff.days >= 5:
                        time_since_created += ' :fire:'
                else:
                    due_hours = int(time_diff.seconds / 3600)
                    if due_hours:
                        time_since_created = f"{due_hours} hour{'' if due_hours == 1 else 's'} ago"
                    else:
                        minutes_ago = int(time_diff.seconds / 60)
                        time_since_created = f"{minutes_ago} minute{'' if minutes_ago == 1 else 's'} ago"

                # pull request summary for the message
                message += f" Repository: {repository_name}."
                message += f" Pull: {pull['title']}.\n"
                message += f" URL: {pull['url']}.\n"
                message += f" Author: {pull['author']['login']}. Created: {time_since_created}\n"
                message += f" Reviews: {pull['reviews']['totalCount']}, "
                message += f" pending {pull['reviewRequests']['totalCount']}.\n\n"

    if message:
        # send notification via slack
        slack_headers = {'Content-type': 'application/json',
                         'Authorization': f"Bearer {ssm_parameters['slack_token']}"}

        r = requests.post(url=ssm_parameters['slack_webhook_url'],
                          headers=slack_headers,
                          data=json.dumps({'text': f'The following pull requests are OPEN:\n\n{message}'}))

        if r.status_code != 200:
            logging.error(f'Got status {r.status_code} while trying to post to the slack webhook url.')

    return message
