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

import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.exceptions.AuthenticationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


@RunWith(Parameterized.class)
public class MutualTlsAuthenticatorParameterizedTest extends MutualTlsAuthenticatorTest
{
    @Parameterized.Parameter(0)
    public String certificatePath;
    @Parameterized.Parameter(1)
    public String identity;


    @Parameterized.Parameters()
    public static Collection<Object[]> versions()
    {
        final List<Object[]> paths = new ArrayList<>();
        paths.add(new Object[]{ "auth/SampleCorpCertificate.pem", "urn:certmanager:idmsGroup/845340" });
        paths.add(new Object[]{ "auth/SampleBedRockCertificate.pem", "urn:bedrock:ns/1273976/uid/1273978" });
        paths.add(new Object[]{ "auth/SampleGDBCCertificate.pem", "urn:appcertname:identityName/sredbv42-sredb-cassandra.cassandra-prod.kube" });
        return paths;
    }

    @BeforeClass
    public static void initialize()
    {
        setup();
    }

    @Test
    public void testAuthorizedUsers() throws CertificateException, IOException, TimeoutException
    {
        initializeMtlsAuthenticatorWithUrn(mutualtlsAuthenticator, true, identity);
        final Certificate[] chain = loadCertificateChain(certificatePath);

        // Verify authenticated user if as expected
        final IAuthenticator.SaslNegotiator saslNegotiator = mutualtlsAuthenticator.newSaslNegotiator(getMockInetAddress(), chain);
        final AuthenticatedUser authenticatedUser = saslNegotiator.getAuthenticatedUser();
        assertNotNull(authenticatedUser);
        assertEquals(identity, authenticatedUser.getName());
    }

    @Test(expected = AuthenticationException.class)
    public void testUnauthorizedUsers() throws CertificateException, IOException, TimeoutException
    {
        initializeMtlsAuthenticatorWithUrn(mutualtlsAuthenticator, false, identity);
        final Certificate[] chain = loadCertificateChain(certificatePath);

        final IAuthenticator.SaslNegotiator saslNegotiator = mutualtlsAuthenticator.newSaslNegotiator(getMockInetAddress(), chain);
        saslNegotiator.getAuthenticatedUser();
    }

    @Test(expected = AuthenticationException.class)
    public void testInvalidUsers() throws CertificateException, IOException, TimeoutException
    {
        initializeMtlsAuthenticatorWithUrn(mutualtlsAuthenticator, true, identity);
        final Certificate[] clientCertificates = loadCertificateChain("auth/SampleInvalidCertificate.pem");
        final IAuthenticator.SaslNegotiator saslNegotiator = mutualtlsAuthenticator.newSaslNegotiator(getMockInetAddress(), clientCertificates);
        saslNegotiator.getAuthenticatedUser();
    }
}
