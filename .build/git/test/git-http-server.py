#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
A minimal smart HTTP git server for test-git-hooks.sh, so that the hooks can be tested against a network remote.
Serves the repositories under <root> by bridging requests to git http-backend, on a free port written to <port-file>.

usage: git-http-server.py <root> <port-file>
"""

import http.server
import os
import subprocess
import sys

ROOT, PORT_FILE = sys.argv[1], sys.argv[2]


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def handle_request(self):
        path, _, query = self.path.partition('?')
        length = int(self.headers.get('Content-Length') or 0)
        body = self.rfile.read(length) if length else b''
        env = dict(os.environ,
                   GIT_PROJECT_ROOT=ROOT, GIT_HTTP_EXPORT_ALL='1', REMOTE_USER='test', REMOTE_ADDR='127.0.0.1',
                   REQUEST_METHOD=self.command, PATH_INFO=path, QUERY_STRING=query,
                   CONTENT_TYPE=self.headers.get('Content-Type', ''), CONTENT_LENGTH=str(len(body)),
                   HTTP_CONTENT_ENCODING=self.headers.get('Content-Encoding', ''),
                   GIT_PROTOCOL=self.headers.get('Git-Protocol', ''))
        out = subprocess.run(['git', 'http-backend'], input=body, env=env, capture_output=True).stdout
        head, _, payload = out.partition(b'\r\n\r\n')
        status = 200
        headers = []
        for line in head.decode().split('\r\n'):
            name, _, value = line.partition(':')
            if not name:
                continue
            if name.lower() == 'status':
                status = int(value.strip().split()[0])
            else:
                headers.append((name, value.strip()))
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = handle_request
    do_POST = handle_request


server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
with open(PORT_FILE + '.tmp', 'w') as f:
    f.write(str(server.server_address[1]))
os.rename(PORT_FILE + '.tmp', PORT_FILE)
server.serve_forever()
