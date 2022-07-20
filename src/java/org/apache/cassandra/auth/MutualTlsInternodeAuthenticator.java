/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.auth;

import java.net.InetAddress;
import java.security.cert.Certificate;

import org.apache.cassandra.exceptions.ConfigurationException;

/*
 * Performs mTLS authentication for internode connections by extracting identities from outbound certificates
 * and verifying them against the authorized identities in the roles table.
 */
public class MutualTlsInternodeAuthenticator extends BaseMutualTlsAuthenticator implements IInternodeAuthenticator
{
    @Override
    public boolean authenticate(InetAddress remoteAddress, int remotePort)
    {
        throw new UnsupportedOperationException("mTLS Authenticator only supports certificate based authenticate method");
    }

    @Override
    public boolean authenticate(InetAddress remoteAddress, int remotePort,
                                Certificate[] certificates, InternodeConnectionDirection connectionType)
    {
        if (connectionType == InternodeConnectionDirection.INBOUND) {
            return authenticator.isAuthorized(castCertsToX509(certificates));
        }
        // Outbound connections don't need to be authenticated again in certificate based connections. SSL handshake
        // makes sure that we are talking to valid server by checking root certificates of the server in the
        // truststore of the client.
        return true;
    }


    @Override
    public void validateConfiguration() throws ConfigurationException
    {

    }

    @Override
    public void setupInternode()
    {
        super.initializeAuthenticator();
    }
}
