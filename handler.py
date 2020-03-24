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


def check_open_pull_requests(event: dict, context) -> dict:
    """
    Checks pending pull requests and sends a notification

    """
    # GraphQL query to get open pulls within an organization.
    # For now only parsing top 100 pulls, and most recent reviews.

    query = '''
{
  search(query: "org:github_organization is:pr state:open", type: ISSUE, first: 100) {
    edges {
      node {
        ... on PullRequest {
          url
          title
          createdAt
          author {
            login
          }
          repository {
            name
          }
          assignees(first: 10) {
            totalCount
            edges {
              node {
                login
              }
            }
          }
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
          reviews(first: 20) {
            totalCount
            edges {
              node {
                author {
                  login
                }
                state
              }
            }
          }
        }
      }
    }
  }
}
'''
    # load configuration from the parameter store
    ssm_parameters = load_params('dev_tools', 'dev')
    skip_repositories = ssm_parameters['pr_skip_repositories'] if 'pr_skip_repositories' in ssm_parameters else ()

    # get pulls from github
    headers = {"Authorization": f"token {ssm_parameters['github_access_token']}"}
    result = requests.post('https://api.github.com/graphql',
                           json={'query': query.replace('github_organization', ssm_parameters['github_organization'])},
                           headers=headers)

    if result.status_code != 200:
        logging.error(f"Github's API returned code {result.status_code} for query: {query}")
        return

    data = result.json()
    if not data or not data.get('data'):
        logging.error(f"Github's API returned invalid data: {data}")
        return {}

    # parse results and create slack message for notification
    message = ''
    for pr_node in data['data']['search']['edges']:

        pr = pr_node['node']
        repository_name = pr['repository']['name']

        if repository_name in skip_repositories:
            continue

        # figure out the time that has passed since the pull request was created
        now = datetime.datetime.utcnow()
        created = datetime.datetime.strptime(pr['createdAt'], '%Y-%m-%dT%H:%M:%SZ')
        time_diff = (now - created)

        if time_diff.days:
            time_since_created = f"{time_diff.days} day{'' if time_diff.days == 1 else 's'} ago"
            # add warnings for old pull requests
            if 3 < time_diff.days < 6:
                time_since_created += ' :warning:'
            elif time_diff.days >= 6:
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
        message += f" Pull: {pr['title']}.\n"
        message += f" URL: {pr['url']}.\n"

        # collect activity
        activity = {'APPROVED': 0,
                    'CHANGES_REQUESTED': 0,
                    'COMMENTED': 0,
                    'DISMISSED': 0,
                    'PENDING': 0}

        if pr['reviews']['totalCount'] > 0:
            for review_node in pr['reviews']['edges']:
                activity[review_node['node']['state']] += 1

            if activity['CHANGES_REQUESTED']:
                message += f" :changes_requested: Changes requested, completion may take some time.\n"

        message += f" Author: {pr['author']['login']}. Created: {time_since_created}\n"

        if pr['reviewRequests']['totalCount']:
            message += f" Reviewers: {pr['reviewRequests']['totalCount']} pending.\n"
        else:
            message += " No pending reviewers.\n"

        activity_msg = ''
        if activity['APPROVED']:
            activity_msg += f"{activity['APPROVED']} approval{'' if activity['APPROVED'] == 1 else 's'}"

        if activity['COMMENTED']:
            activity_msg += f", {activity['COMMENTED']} comment{'' if activity['COMMENTED'] == 1 else 's'}"

        if activity['DISMISSED']:
            activity_msg += f", {activity['DISMISSED']} dismissed approval{'' if activity['DISMISSED'] == 1 else 's'}"

        if activity_msg:
            message += f" Activity: {activity_msg}.\n"

        message += "\n"

    if message:
        # send notification via slack
        slack_headers = {'Content-type': 'application/json',
                         'Authorization': f"Bearer {ssm_parameters['slack_access_token']}"}

        r = requests.post(url=ssm_parameters['slack_webhook_url'],
                          headers=slack_headers,
                          data=json.dumps({'text': f'The following pull requests are OPEN:\n\n{message}'}))

        if r.status_code != 200:
            logging.error(f'Got status {r.status_code} while trying to post to the slack webhook url.')

    return message
