import logging
import re
import os
import sys
import json
import time
from datetime import datetime
from urllib.parse import urlparse, parse_qs

import tzlocal
import requests


logging.basicConfig(level=logging.DEBUG)
requests_log = logging.getLogger('urllib3')
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True
LOGGER = logging.getLogger(__name__)

# Requires tailing slash, otherwise will redirects to include it, and we don't
# want to need to follow the first but not subsequent redirects for
# cookie-hijacking
CARNIVAL_IF1_BASE_URL = 'https://if1.carnival.apple.com/Carnival/'
CASSANDRA_CONNECT_ENV_BASE_URL = 'https://cassandra.apple.com'
CASSANDRA_CONNECT_API_TIMEOUT_SECS = 60
CASSANDRA_CONNECT_API_HEADERS = {'Content-Type': 'application/json'}

PIPELINE_SPEC_ID = os.environ.get('PIPELINE_SPEC_ID')
RIO_BUILD_ID = os.environ.get('RIO_BUILD_ID')


def get_required_env(name):
    val = os.environ.get(name)
    if not val or val == '':
        raise Exception(f'Missing required environment variable: {name}')
    return val


# Original Python source from: https://github.pie.apple.com/rio/carnival-client/blob/dev/scripts/getcookie.py
# Adapted via the 2to3 tool, with other minor adjustments for loading credentials via env, etc
def get_carnival_auth_cookie(ac_username, ac_password):

    LOGGER.info(f'Making an unauthenticated request to Carnival base URL {CARNIVAL_IF1_BASE_URL} to get IdMS appIdKey and rv params...')
    unauth_resp = requests.get(url=CARNIVAL_IF1_BASE_URL, allow_redirects=False)
    unauth_redirect_location = unauth_resp.headers['Location']
    LOGGER.info(f'Parsing IdMS params from Location header: {unauth_redirect_location}')
    app_id_key, rv = parse_unauth_redirect_location(unauth_redirect_location)
    LOGGER.info(f'Got IdMS params: appIdKey={app_id_key} rv={rv}')

    login_url = f'https://idmsac.corp.apple.com/IDMSWebAuth/login?rv={rv}&appIdKey={app_id_key}'
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
        'appIdKey': app_id_key,
        'appleId': ac_username,
        'accountPassword': ac_password,
    }

    auth_response = requests.post(url=authenticate_url, cookies=auth_cookies, data=auth_payload)
    LOGGER.info(f'Got auth response: {auth_response.status_code}')
    try:
        auth_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        LOGGER.info(f'Auth response returned error status, but can be ignored. This endpoint is known to return 410 but include a valid cookie for authentication; see rdar://84278366. Ignoring exception RequestException={e}')

    for redirect_response in auth_response.history:
        if redirect_response.cookies['acack']:
            return redirect_response.cookies['acack']

    raise Exception('Could not get an auth cookie from Carnival. Are you sure your credentials are correct?')


def parse_unauth_redirect_location(location_header):
    parsed_url = urlparse(location_header)
    if not parsed_url.query:
        raise ValueError(f'Could not extract querystring from Location header: {location_header}')
    parsed_query = parse_qs(parsed_url.query)
    app_id_key = parsed_query['appIdKey'][0]
    rv = parsed_query['rv'][0]
    return app_id_key, rv


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

    # See comment above around trailing slashes
    url = f'{CARNIVAL_IF1_BASE_URL}services/1.0/workflow/request'
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
        'notes': [f'Automated request via Workflow API, initiated by Rio pipeline {PIPELINE_SPEC_ID} build {RIO_BUILD_ID}'],
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


def get_connect_title(pipeline_spec_id, rio_build_id):
    # Connect only supports 127-char titles, so truncate if necessary
    max_len = 127
    return f'Rio {pipeline_spec_id}/{rio_build_id}'[:max_len]


def get_targets_if1(cie_target_if1_clusters):
    # Make sure not empty, or Connect will rollout to all eligible clusters
    results = []

    for target in re.split('\\s+', cie_target_if1_clusters):
        if not target:
            continue
        parts = target.split('/')
        if len(parts) != 2:
            raise Exception(f'Target clusters malformed. Expected app_name/cluster_name, got: {target}.')
        app_name = parts[0]
        cluster_name = parts[1]
        results.append({'clusterName': app_name, 'partitionName': cluster_name})

    if not results:
        raise Exception('Must specify targets for rollout. Make sure file CIE_TARGET_IF1_CLUSTERS has targets, or env var is set.')
    return results


def submit_connect_release_rollout(od_username, od_password, target_cassandra_version, targets_if1):
    # Persistent session required for cookie auth
    session = requests.Session()

    # Login
    url = f'{CASSANDRA_CONNECT_ENV_BASE_URL}/auth/login'
    payload = json.dumps({'username': od_username, 'password': od_password})
    resp = None
    try:
        LOGGER.info(f'Authenticating to Cassandra Connect with od_username: {od_username}...')
        resp = session.post(url=url, data=payload, headers=CASSANDRA_CONNECT_API_HEADERS, timeout=CASSANDRA_CONNECT_API_TIMEOUT_SECS)
    except requests.exceptions.RequestException as e:
        # Something went wrong (timeout, host not found, etc.)
        LOGGER.info(f'Authentication to Cassandra Connect failed with exception: {e}')
        raise e

    if resp.status_code != 200:
        raise Exception(f'Authentication to Cassandra Connect failed with status: {resp.status_code}, response body: {resp.text}')
    else:
        LOGGER.info(f'Authenticated to Cassandra Connect successfully')
    resp_json = resp.json()

    auth_token = resp_json.get('access_token')
    xsrf_token = resp.cookies.get('XSRF-TOKEN')

    # Submit release rollout
    url = f'{CASSANDRA_CONNECT_ENV_BASE_URL}/ops/api/v1/version-upgrade/rollout-release'
    headers = {**CASSANDRA_CONNECT_API_HEADERS, 'Authorization': f'Bearer {auth_token}', 'X-XSRF-TOKEN': xsrf_token}

    title = get_connect_title(PIPELINE_SPEC_ID, RIO_BUILD_ID)

    payload = json.dumps({
        'stream': 'Cassandra',
        'env': 'IF1',
        'upgradeType': 'MinorVersionUpgrade',
        'cassVersion': target_cassandra_version,
        'startDate': datetime.now(tz=tzlocal.get_localzone()).isoformat(),
        'title': title,
        'partitions': targets_if1,
        'config': {
            'ignoreActivityCheck': True
        }
    })

    LOGGER.info(f'Submitting release rollout url={url} payload={payload}')

    resp = session.post(url=url, headers=headers, data=payload, timeout=CASSANDRA_CONNECT_API_TIMEOUT_SECS)

    if resp.status_code < 200 or resp.status_code >= 400:
        raise Exception(f'Post storage release rollout failed with status: {resp.status_code}, response body: {resp.text}')

    resp_json = resp.json()
    LOGGER.info(f'Posted storage release rollout, got response: {resp_json}')


def main():
    ac_username = get_required_env('AC_USERNAME')
    ac_password = get_required_env('AC_PASSWORD')

    if not os.environ.get('BUILD_PARAM_SKIP_TO_CONNECT_ROLLOUT'):
        # Pipeline runs may use CONTINUE_LOCAL_INSTALL_REQUEST_ID=<prior_request_id> to continue a prior run in case of an error
        local_install_request_id = None
        if os.environ.get('BUILD_PARAM_CONTINUE_LOCAL_INSTALL_REQUEST_ID'):
            local_install_request_id = os.environ.get('BUILD_PARAM_CONTINUE_LOCAL_INSTALL_REQUEST_ID')
            LOGGER.info(f'Continuing from prior local install request {local_install_request_id}, not submitting new request')
        else:
            git_committer_email = get_required_env('BUILD_PARAM_GIT_COMMITTER_EMAIL')
            carnival_app_name = get_required_env('BUILD_PARAM_CARNIVAL_APP_NAME')
            carnival_build_version = get_required_env('BUILD_PARAM_CARNIVAL_BUILD_VERSION')
            auth_acack_cookie = get_carnival_auth_cookie(ac_username, ac_password)
            local_install_request_id = submit_local_install(git_committer_email, auth_acack_cookie, carnival_app_name, carnival_build_version)

        await_local_install_complete(local_install_request_id)
    else:
        LOGGER.info('Skipping directly to Connect rollout...')

    od_username = get_required_env('OD_USERNAME')
    od_password = get_required_env('OD_PASSWORD')
    target_cassandra_version = get_required_env('BUILD_PARAM_CIE_VERSION')
    cie_target_if1_clusters = get_targets_if1(get_required_env('BUILD_PARAM_CIE_TARGET_IF1_CLUSTERS'))
    submit_connect_release_rollout(od_username, od_password, target_cassandra_version, cie_target_if1_clusters)


if __name__ == '__main__':
    main()
