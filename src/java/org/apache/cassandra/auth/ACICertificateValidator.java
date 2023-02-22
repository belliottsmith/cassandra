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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.aci.cassandra.mtls.CompositeMtlsValidatorAndIdentityProvider;
import com.apple.aci.cassandra.mtls.IdentityType;
import com.apple.aci.cassandra.mtls.User;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.AuthenticationException;

/**
 * Identity of Apple certificates has a defined format URN. This class is an mTLS certificate validator for apple
 * specific certificates. This provides 3 important methods
 * <p>
 * - {@link ACICertificateValidator#isValidCertificate} method that performs all sanity checks on the certificate
 * based on the type of apple certificates like verifying CN, Issuer, Organization etc..
 * - {@link ACICertificateValidator#identity} method that extracts identity as URNs from the certificates
 * a client with given certificate or not
 */
public class ACICertificateValidator implements MutualTlsCertificateValidator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ACICertificateValidator.class);
    private static final String URN_REGEX = "urn:(%s):((\\S+)\\/(\\S+)\\/)+";
    private static final Pattern PATTERN = getURNRegexPattern();
    private final CompositeMtlsValidatorAndIdentityProvider validator;

    public ACICertificateValidator()
    {
        final String managementDsid = CassandraRelevantProperties.MANAGEMENT_DSID_OF_ROOT.getString();
        this.validator = new CompositeMtlsValidatorAndIdentityProvider(managementDsid);
    }

    @Override
    public boolean isValidCertificate(Certificate[] clientCertificateChain)
    {
        X509Certificate[] convertedChain = castCertsToX509(clientCertificateChain);
        return validator.isValid(convertedChain);
    }

    @Override
    public String identity(Certificate[] clientCertificateChain) throws AuthenticationException
    {
        X509Certificate[] convertedChain = castCertsToX509(clientCertificateChain);
        List<User> users = validator.extractUsers(convertedChain);
        if (users.isEmpty())
        {
            throw new AuthenticationException("Could not extract any identities from the certificate, might not be an Apple supported certificate");
        }
        return users.get(0).toURN();
    }

    public static boolean validateIdentity(String identity)
    {
        // appending '/' to make urn:XXXX -> urn:XXXX/ for the regular expression
        final String modifiedURN = identity.endsWith("/") ? identity : identity + '/';
        final Matcher matcher = PATTERN.matcher(modifiedURN);
        return matcher.matches();
    }

    private static X509Certificate[] castCertsToX509(Certificate[] clientCertificateChain)
    {
        return Arrays.asList(clientCertificateChain).toArray(new X509Certificate[0]);
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
