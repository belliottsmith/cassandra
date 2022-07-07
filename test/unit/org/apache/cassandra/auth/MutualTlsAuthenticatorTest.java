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

import org.apache.cassandra.exceptions.AuthenticationException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Collection;
import java.util.concurrent.TimeoutException;

import static org.apache.cassandra.auth.BaseMutualTlsAuthenticatorTest.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


@RunWith(Parameterized.class)
public class MutualTlsAuthenticatorTest
{
    @Parameterized.Parameter(0)
    public String certificatePath;
    @Parameterized.Parameter(1)
    public String identity;


    @Parameterized.Parameters()
    public static Collection<Object[]> versions()
    {
        return setupParameterizedTestWithVariousCertificateTypes();
    }

    @BeforeClass
    public static void setup()
    {
        initializeDB();
    }

    @After
    public void after() throws IOException, TimeoutException
    {
        // Clear roles table after each test
        clearDB();
    }

    @Test
    public void testAuthorizedUsers() throws CertificateException, IOException, TimeoutException
    {
        initializeRolesTable(identity);
        final Certificate[] chain = loadCertificateChain(certificatePath);

        // Verify authenticated user is as expected
        final IAuthenticator mutualTlsAuthenticator = createAndInitializeMtlsAuthenticator();
        final IAuthenticator.SaslNegotiator saslNegotiator = mutualTlsAuthenticator.newSaslNegotiator(getMockInetAddress(), chain);
        final AuthenticatedUser authenticatedUser = saslNegotiator.getAuthenticatedUser();
        assertNotNull(authenticatedUser);
        assertEquals(identity, authenticatedUser.getName());
    }

    @Test(expected = AuthenticationException.class)
    public void testUnauthorizedUsers() throws CertificateException, IOException, TimeoutException
    {
        final Certificate[] chain = loadCertificateChain(certificatePath);
        final IAuthenticator mutualTlsAuthenticator = createAndInitializeMtlsAuthenticator();
        final IAuthenticator.SaslNegotiator saslNegotiator = mutualTlsAuthenticator.newSaslNegotiator(getMockInetAddress(), chain);
        saslNegotiator.getAuthenticatedUser();
    }

    @Test(expected = AuthenticationException.class)
    public void testInvalidUsers() throws CertificateException, IOException, TimeoutException
    {
        initializeRolesTable(identity);
        final Certificate[] clientCertificates = loadCertificateChain("auth/SampleInvalidCertificate.pem");
        final IAuthenticator mutualTlsAuthenticator = createAndInitializeMtlsAuthenticator();
        final IAuthenticator.SaslNegotiator saslNegotiator = mutualTlsAuthenticator.newSaslNegotiator(getMockInetAddress(), clientCertificates);
        saslNegotiator.getAuthenticatedUser();
    }

    private MutualTlsAuthenticator createAndInitializeMtlsAuthenticator() {
        final MutualTlsAuthenticator mutualTlsAuthenticator = new MutualTlsAuthenticator();
        mutualTlsAuthenticator.initializeAuthenticator();
        return mutualTlsAuthenticator;
    }
}
