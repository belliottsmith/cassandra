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

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.apple.aci.cassandra.mtls.CompositeMtlsAuthenticator;
import com.apple.aci.cassandra.mtls.IdentityType;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * BaseMutualTlsAuthenticator contains most of the common implementation details of {@link MutualTlsInternodeAuthenticator}
 * for internode mTLS and {@link MutualTlsAuthenticator} for client mTLS.
 */
class BaseMutualTlsAuthenticator
{
    public static final String URN_REGEX = "urn:(%s):((\\S+)\\/(\\S+)\\/)+";
    private static final Pattern PATTERN = getURNRegexPattern();
    protected CompositeMtlsAuthenticator authenticator;

    protected void initializeAuthenticator()
    {
        // Authorized user identities are stored as URNs (uniform resource name) inside roles table
        // Read those valid users from roles table and create an authenticator
        final List<String> urns = DatabaseDescriptor.getRoleManager()
                                                    .getAllRoles().stream()
                                                    .map(RoleResource::getRoleName)
                                                    .filter(this::isURN)
                                                    .collect(Collectors.toList());
        authenticator = new CompositeMtlsAuthenticator.Builder()
                .withManagementDsidOfRoot(CassandraRelevantProperties.MANAGEMENT_DSID_OF_ROOT.getString())
                .withAuthorizedURNS(urns)
                .build();
    }

    protected X509Certificate[] castCertsToX509(Certificate[] clientCertificateChain)
    {
        return Arrays.asList(clientCertificateChain).toArray(new X509Certificate[0]);
    }

    private boolean isURN(final String urn)
    {
        // appending '/' to make urn:XXXX -> urn:XXXX/ for the regular expression
        final String modifiedURN = urn.endsWith("/") ? urn : urn + '/';
        final Matcher matcher = PATTERN.matcher(modifiedURN);
        return matcher.matches();
    }

    private static Pattern getURNRegexPattern()
    {
        final String identityTypeRegex = Arrays.stream(IdentityType.values())
                                               .map(IdentityType::getUrnType)
                                               .collect(Collectors.joining("|"));
        final String urnRegex = String.format(URN_REGEX, identityTypeRegex);
        return Pattern.compile(urnRegex);
    }
}
