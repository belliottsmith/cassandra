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
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.aci.cassandra.mtls.User;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.NoSpamLogger;
import org.jetbrains.annotations.NotNull;

/*
 * Performs mTLS authentication for client connections by extracting identities from client certificate
 * and verifying them against the ones in the roles table.
 */

public class MutualTlsAuthenticator extends BaseMutualTlsAuthenticator implements IAuthenticator
{
    private static final Logger logger = LoggerFactory.getLogger(MutualTlsAuthenticator.class);
    private static final NoSpamLogger nospamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);

    @Override
    public boolean requireAuthentication()
    {
        return true;
    }

    @Override
    public Set<? extends IResource> protectedResources()
    {
        return ImmutableSet.of(DataResource.table(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES));
    }

    @Override
    public void validateConfiguration() throws ConfigurationException
    {

    }

    @Override
    public void setup()
    {
        super.initializeAuthenticator();
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress clientAddress)
    {
        return null;
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress clientAddress, Certificate[] certificates)
    {
        return new CertificateNegotiator(certificates);
    }

    @Override
    public AuthenticatedUser legacyAuthenticate(Map<String, String> credentials) throws AuthenticationException
    {
        return null;
    }

    @VisibleForTesting
    class CertificateNegotiator implements SaslNegotiator
    {
        private final Certificate[] clientCertificateChain;

        private CertificateNegotiator(@NotNull final Certificate[] clientCertificateChain)
        {
            this.clientCertificateChain = clientCertificateChain;
        }

        @Override
        public byte[] evaluateResponse(byte[] clientResponse) throws AuthenticationException
        {
            return null;
        }

        @Override
        public boolean isComplete()
        {
            return true;
        }

        @Override
        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException
        {
            X509Certificate[] convertedChain = castCertsToX509(clientCertificateChain);
            final List<User> authorizedUsers = authenticator.authorizedUsers(convertedChain);
            if (authorizedUsers.isEmpty())
            {
                final String allUsers = authenticator.allUsers(convertedChain)
                                                     .stream()
                                                     .map(User::displayName)
                                                     .collect(Collectors.joining());
                nospamLogger.error("None of the users {} are authorized", allUsers);
                throw new AuthenticationException("None of the users " + allUsers + " are authorized");
            }
            return new AuthenticatedUser(authorizedUsers.get(0).toURN());
        }
    }
}
