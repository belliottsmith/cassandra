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

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.cassandra.auth.BaseMutualTlsAuthenticatorTest.*;
import static org.apache.cassandra.auth.IInternodeAuthenticator.InternodeConnectionDirection.INBOUND;
import static org.apache.cassandra.auth.IInternodeAuthenticator.InternodeConnectionDirection.OUTBOUND;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class MutualTlsInternodeAuthenticatorTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
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
    public static void initialize()
    {
        System.setProperty("cassandra.config", "cassandra-mtls.yaml");
        initializeDB();
    }

    @Test
    public void testAuthenticateWithoutCertificatesShouldThrowUnsupportedOperation() throws UnknownHostException
    {
        final InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.1");
        final Map<String, String> parameters = Collections.singletonMap("valid_ids", identity);
        final IInternodeAuthenticator authenticator = new MutualTlsInternodeAuthenticator(parameters);
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("mTLS Authenticator only supports certificate based authenticate method");
        authenticator.authenticate(address.getAddress(), address.getPort());
    }

    @Test
    public void testAuthenticationOfOutboundConnectionsShouldBeSuccess() throws UnknownHostException
    {
        final InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.1");
        final Map<String, String> parameters = Collections.singletonMap("valid_ids", identity);
        final IInternodeAuthenticator authenticator = new MutualTlsInternodeAuthenticator(parameters);
        assertTrue(authenticator.authenticate(address.getAddress(), address.getPort(), new Certificate[0], OUTBOUND));
    }

    @Test
    public void testAuthorizedUsers() throws IOException, CertificateException, TimeoutException
    {
        final InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.1");
        final Map<String, String> parameters = Collections.singletonMap("valid_ids", identity);
        final IInternodeAuthenticator authenticator = new MutualTlsInternodeAuthenticator(parameters);
        final Certificate[] clientCertificates = loadCertificateChain(certificatePath);
        assertTrue(authenticator.authenticate(address.getAddress(), address.getPort(), clientCertificates, INBOUND));
    }

    @Test
    public void testUnauthorizedUser() throws IOException, CertificateException, TimeoutException
    {
        final InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.1");
        final Map<String, String> parameters = Collections.singletonMap("valid_ids", "urn:certmanager:idmsGroup/8453");
        final IInternodeAuthenticator authenticator = new MutualTlsInternodeAuthenticator(parameters);
        final Certificate[] clientCertificates = loadCertificateChain(certificatePath);
        assertFalse(authenticator.authenticate(address.getAddress(), address.getPort(), clientCertificates, INBOUND));
    }

    @Test
    public void testNoValidUsersInConfig()
    {
        final Map<String, String> parameters = Collections.singletonMap("valid_ids", "");
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("No valid clientIds in internode_authenticator.parameters.valid_ids, no internode clients will be trusted");
        final IInternodeAuthenticator authenticator = new MutualTlsInternodeAuthenticator(parameters);
    }
}
