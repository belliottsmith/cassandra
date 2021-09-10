import json
import carnival_local_install

SUBMIT_LOCAL_INSTALL_SUCCESS_RESPONSE_BODY = json.loads('{"status":"SUCCESS","code":"201 Created","description":"https://if1.carnival.apple.com/Carnival/TaskDetail.jsp?id=1629505026063"}')

def test_parse_id_from_local_install_response():
    parsed = carnival_local_install.parse_id_from_local_install_response(SUBMIT_LOCAL_INSTALL_SUCCESS_RESPONSE_BODY)
    assert parsed == '1629505026063'