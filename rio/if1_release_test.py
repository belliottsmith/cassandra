import json
import re

import pytest

import if1_release


def test_parse_id_from_local_install_response():
    submit_local_install_success_response_body = json.loads('{"status":"SUCCESS","code":"201 Created","description":"https://if1.carnival.apple.com/Carnival/TaskDetail.jsp?id=1629505026063"}')
    parsed = if1_release.parse_id_from_local_install_response(submit_local_install_success_response_body)
    assert parsed == '1629505026063'


def test_get_connect_title():
    placeholder = '-' * 100
    title = if1_release.get_connect_title(placeholder, placeholder)
    assert title == f'Rio ----------------------------------------------------------------------------------------------------/----------------------'


def test_get_targets_if1():
    assert if1_release.get_targets_if1('a/b') == [{'clusterName': 'a', 'partitionName': 'b'}]
    assert if1_release.get_targets_if1('a/b\nc/d') == [{'clusterName': 'a', 'partitionName': 'b'}, {'clusterName': 'c', 'partitionName': 'd'}]
    assert if1_release.get_targets_if1('a/b\t\tc/d') == [{'clusterName': 'a', 'partitionName': 'b'}, {'clusterName': 'c', 'partitionName': 'd'}]
    assert if1_release.get_targets_if1('a/b\t\tc/d  ') == [{'clusterName': 'a', 'partitionName': 'b'}, {'clusterName': 'c', 'partitionName': 'd'}]

    # Check malformed
    with pytest.raises(Exception) as excinfo:
        if1_release.get_targets_if1('a/b/c')
        assert excinfo
    with pytest.raises(Exception) as excinfo:
        if1_release.get_targets_if1('a')
        assert excinfo

    # Postcondition: result can't be empty
    with pytest.raises(Exception) as excinfo:
        if1_release.get_targets_if1('')
        assert excinfo
    with pytest.raises(Exception):
        if1_release.get_targets_if1(None)
        assert excinfo

def test_parse_unauth_redirect_location():
    # Expected result
    location_header = 'https://idmsac.corp.apple.com/IDMSWebAuth/login?appIdKey=abcdef&rv=123&path='
    app_id_key, rv = if1_release.parse_unauth_redirect_location(location_header)
    assert app_id_key == 'abcdef'
    assert rv == '123'

    # Malformed: missing param keys
    with pytest.raises(Exception) as excinfo:
        location_header = 'https://idmsac.corp.apple.com/IDMSWebAuth/login'
        app_id_key, rv = if1_release.parse_unauth_redirect_location(location_header)
        assert excinfo

    # Malformed: missing param values
    with pytest.raises(Exception) as excinfo:
        location_header = 'https://idmsac.corp.apple.com/IDMSWebAuth/login?appIdKey=&rv=&path='
        app_id_key, rv = if1_release.parse_unauth_redirect_location(location_header)
        assert excinfo
