import logging
import re
import os
import sys
import json
import time

import requests


logging.basicConfig(level=logging.DEBUG)
requests_log = logging.getLogger('urllib3')
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True
LOGGER = logging.getLogger(__name__)


def get_required_env(name):
    val = os.environ.get(name)
    if not val or val == '':
        raise Exception(f'Missing required environment variable: {name}')
    return val


# Original Python source from: https://github.pie.apple.com/rio/carnival-client/blob/dev/scripts/getcookie.py
# Adapted via the 2to3 tool, with other minor adjustments for loading credentials via env, etc
def get_auth_cookie(ac_username, ac_password):
    # See https://docs.carnival.apple.com/documents/api-docs/carnival-web-service-authentication/
    # > To find the App_ID :
    # > Log out of any Carnival environment, once you log back in, copy the IDMS URL and use the appIdKey.
    app_id = "ab74395cc3e8da24ee5b56ff70129f2eb49d4d14dddea094f4e0217f164042c9" # IF1 Carnival

    login_url = f'https://idmsac.corp.apple.com/IDMSWebAuth/login?rv=1&appIdKey={app_id}'
    authenticate_url = 'https://idmsac.corp.apple.com/IDMSWebAuth/authenticate'

    login_response = requests.get(url=login_url, headers={})
    LOGGER.info(f'Got login response: {login_response.status_code}')
    login_response.raise_for_status()

    set_cookies = login_response.headers['set-cookie']
    cookie_match = re.search('.*JSESSIONID=([0-9A-Fa-f]+).*', set_cookies)
    if not cookie_match:
        raise Exception(f'Could not get login cookie from IDMS login, expected cookie key JSESSIONID')
    jsessionid = cookie_match.group(1)

    auth_cookies = {'JSESSIONID': jsessionid}

    auth_payload = {
        'appIdKey': app_id,
        'appleId': ac_username,
        'accountPassword': ac_password,
    }

    auth_response = requests.post(url=authenticate_url, cookies=auth_cookies, data=auth_payload)
    LOGGER.info(f'Got auth response: {auth_response.status_code}')
    auth_response.raise_for_status()

    for redirect_response in auth_response.history:
        if redirect_response.cookies['acack']:
            return redirect_response.cookies['acack']

    raise Exception('Could not get an auth cookie from Carnival. Are you sure your credentials are correct?')


def parse_id_from_local_install_response(resp_json):
    assert resp_json['status'] == 'SUCCESS'
    description = resp_json['description']
    regex = re.compile('.*id=(\\d+)$')
    parsed_id = regex.match(description).group(1)
    return parsed_id


def submit_local_install(git_committer_email, auth_acack_cookie, carnival_app_name, carnival_build_version):
    # Use a date in the past so the Carnival workflow starts immediately
    epoch_formatted = '1970/01/01 12:00 AM'

    LOGGER.info(f'Creating Carnival local install request on behalf of committer {git_committer_email}...')

    url = 'https://if1.carnival.apple.com/Carnival/services/1.0/workflow/request'
    params = {'userEmailAddress': git_committer_email}
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    cookies = {'acack': auth_acack_cookie}
    payload = json.dumps({
        'actions': [{
            'type': 'localinstallbuild',
            'project': 'CIEDb',
            'packages': f'cie-cassandra-{carnival_app_name}{carnival_build_version}',
            'forceInstall': False,
            'installOnHostsStyle': 'CONF_AND_CARNIVAL_INSTALL_STYLE',
            'acceptanceThreshold': '100.0',
            'retries': 1,
            'failIfAllHostsOffline': False,
        }],
        'autoApprove': True,
        'autoSchedule': True,
        'editable': True,
        'notes': ['Automated request via Workflow API'],
        'qaVerifiedUser': git_committer_email,
        'scheduledDate': epoch_formatted
    })

    request_args_cookies_withheld = dict(url=url, params=params, headers=headers, data=payload)
    LOGGER.info(f'Submitting local install request (cookies withheld): {request_args_cookies_withheld}')
    resp = requests.post(**request_args_cookies_withheld, cookies=cookies)
    LOGGER.info(f'Got local install response: {resp.text}')
    resp.raise_for_status()

    resp_json = resp.json()
    return parse_id_from_local_install_response(resp_json)


def await_local_install_complete(local_install_request_id):
    max_attempts = 30
    sleep_secs = 10
    terminal_good_statuses = ['COMPLETED', 'ACTIONS_COMPLETED']
    terminal_bad_statuses = ['DENIED', 'FAILED', 'CANCELED', 'PAUSED']

    url = f'https://if1.carnival.apple.com/Carnival/services/1.0/workflow/request/status?ids={local_install_request_id}'
    headers = {'Content-Type': 'application/json'}

    for num_attempt in range(max_attempts):
        LOGGER.info(f'Making attempt {num_attempt} out of {max_attempts} to fetch local install status...')
        resp = requests.get(url=url, headers=headers)
        LOGGER.info(f'Got response for attempt {num_attempt}: {resp.status_code} {resp.text}')

        if resp.status_code == requests.codes.ok:
            resp_json = resp.json()
            status = resp_json[local_install_request_id]
            if status in terminal_good_statuses:
                msg = f'Carnival local install request {local_install_request_id} completed successfully with status {status}'
                LOGGER.info(msg)
                return
            elif status in terminal_bad_statuses:
                msg = f'Carnival local install request {local_install_request_id} unable to complete successfully due to status {status}'
                raise Exception(msg)
            else:
                msg = f'Carnival local install request {local_install_request_id} is still in progress with status {status}'
                LOGGER.info(msg)

        LOGGER.info(f'Sleeping {sleep_secs} seconds to await completion...')
        time.sleep(sleep_secs)

    status_link = f'https://if1.carnival.apple.com/Carnival/TaskDetail.jsp?id={local_install_request_id}'
    msg = f'Did not observe Carnival local install completion within {max_attempts} attempts {sleep_secs} seconds apart. Cannot proceed without local install completion. Check on Carnival request {status_link} until completion, then manually resume this pipeline with build parameter CONTINUE_LOCAL_INSTALL_REQUEST_ID={local_install_request_id}.'
    raise Exception(msg)


def main():
    ac_username = get_required_env('AC_USERNAME')
    ac_password = get_required_env('AC_PASSWORD')

    # Pipeline runs may use CONTINUE_LOCAL_INSTALL_REQUEST_ID=<prior_request_id> to continue a prior run in case of an error
    local_install_request_id = None
    if os.environ.get('BUILD_PARAM_CONTINUE_LOCAL_INSTALL_REQUEST_ID'):
        local_install_request_id = os.environ.get('BUILD_PARAM_CONTINUE_LOCAL_INSTALL_REQUEST_ID')
        LOGGER.info(f'Continuing from prior local install request {local_install_request_id}, not submitting new request')
    else:
        git_committer_email = get_required_env('BUILD_PARAM_GIT_COMMITTER_EMAIL')
        carnival_app_name = get_required_env('BUILD_PARAM_CARNIVAL_APP_NAME')
        carnival_build_version = get_required_env('BUILD_PARAM_CARNIVAL_BUILD_VERSION')

        auth_acack_cookie = get_auth_cookie(ac_username, ac_password)
        local_install_request_id = submit_local_install(git_committer_email, auth_acack_cookie, carnival_app_name, carnival_build_version)

    await_local_install_complete(local_install_request_id)


if __name__ == '__main__':
    main()