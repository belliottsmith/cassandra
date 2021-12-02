package org.apache.cassandra.auth;

import java.net.InetAddress;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.aci.cassandra.mtls.CompositeMtlsAuthenticator;
import com.apple.aci.cassandra.mtls.IdentityType;
import com.apple.aci.cassandra.mtls.User;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.NoSpamLogger;
import org.jetbrains.annotations.NotNull;

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

public class MutualTlsAuthenticator implements IAuthenticator
{
    private static final Logger logger = LoggerFactory.getLogger(MutualTlsAuthenticator.class);
    private static final NoSpamLogger nospamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);
    private static final String ROLE = "role";
    private static final Pattern PATTERN = getURNRegexPattern();
    private CompositeMtlsAuthenticator authenticator;

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
        final String query = String.format("SELECT DISTINCT %s FROM %s.%s;",
                                           ROLE,
                                           SchemaConstants.AUTH_KEYSPACE_NAME,
                                           AuthKeyspace.ROLES);
        final SelectStatement authenticateStatement = (SelectStatement) QueryProcessor.getStatement(query, ClientState.forInternalCalls());
        final QueryOptions options = QueryOptions.forInternalCalls(AuthProperties.instance.getReadConsistencyLevel(),
                                                                   Lists.newArrayList());
        final ResultMessage.Rows rows = authenticateStatement.execute(QueryState.forInternalCalls(), options, System.nanoTime());
        final List<String> URNS = rows.result.rows.stream()
                                                  .map(row -> row.get(0))
                                                  .map(byteBuffer -> new String(byteBuffer.array()))
                                                  .filter(MutualTlsAuthenticator::isURN)
                                                  .collect(Collectors.toList());
        authenticator = new CompositeMtlsAuthenticator.Builder()
                        .withManagementDsidOfRoot(CassandraRelevantProperties.MANAGEMENT_DSID_OF_ROOT.getString())
                        .withAuthorizedURNS(URNS)
                        .build();
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

        private X509Certificate[] castCertsToX509(Certificate[] clientCertificateChain)
        {
            return Arrays.stream(clientCertificateChain)
                         .map(X509Certificate.class::cast)
                         .toArray(X509Certificate[]::new);
        }
    }

    private static boolean isURN(final String urn)
    {
        // appending '/' to make urn:XXXX -> urn:XXXX/ for the regular expression
        final String modifiedURN = urn.endsWith("/") ? urn : urn + '/';
        final Matcher matcher = PATTERN.matcher(modifiedURN);
        return matcher.matches();
    }

    private static Pattern getURNRegexPattern() {
        final String identityTypeRegex = Arrays.stream(IdentityType.values())
                                               .map(IdentityType::getUrnType)
                                               .collect(Collectors.joining("|"));
        final String urnRegex = String.format("urn:(%s):((\\S+)\\/(\\S+)\\/)+", identityTypeRegex);
        return Pattern.compile(urnRegex);
    }
}
