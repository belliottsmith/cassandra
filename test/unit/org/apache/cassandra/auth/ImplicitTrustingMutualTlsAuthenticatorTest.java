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

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.auth.BaseMutualTlsAuthenticatorTest.initializeDB;
import static org.apache.cassandra.auth.BaseMutualTlsAuthenticatorTest.loadCertificateChain;
import static org.apache.cassandra.auth.IInternodeAuthenticator.InternodeConnectionDirection.INBOUND;
import static org.apache.cassandra.auth.ImplicitTrustingMutualTlsAuthenticator.getUrnsFromKeystore;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ImplicitTrustingMutualTlsAuthenticatorTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    @BeforeClass
    public static void setup()
    {
        System.setProperty("cassandra.config", "cassandra-mtls.yaml");
        initializeDB();
    }

    @After
    public void after() throws IOException, TimeoutException
    {
        DatabaseDescriptor.getRawConfig().internode_authenticator.parameters.clear();
    }

    @Test
    public void testGetUrnsFromKeystore()
    {
        final List<String> urns = getUrnsFromKeystore("test/conf/cassandra_ssl_test_outbound.keystore", "cassandra", "JKS");
        assertFalse(urns.isEmpty());
        assertEquals(urns.get(0), "urn:bedrock:ns/1273976/uid/1273978");
    }

    @Test
    public void testAuthorizedUser() throws UnknownHostException, CertificateException
    {
        final Config config = DatabaseDescriptor.getRawConfig();
        config.server_encryption_options = config.server_encryption_options.withOutboundKeystore("test/conf/cassandra_ssl_test_outbound.keystore")
                                                                           .withOutboundKeystorePassword("cassandra");
        final IInternodeAuthenticator authenticator = new ImplicitTrustingMutualTlsAuthenticator();
        final InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.1");
        final Certificate[] bedrockCertificates = loadCertificateChain("auth/SampleBedRockCertificate.pem");
        assertTrue(authenticator.authenticate(address.getAddress(), address.getPort(), bedrockCertificates, INBOUND));
    }

    @Test
    public void testUnAuthorizedUser() throws UnknownHostException, CertificateException
    {
        final Config config = DatabaseDescriptor.getRawConfig();
        config.server_encryption_options = config.server_encryption_options.withOutboundKeystore("test/conf/cassandra_ssl_test_outbound.keystore")
                                                                           .withOutboundKeystorePassword("cassandra");

        final IInternodeAuthenticator authenticator = new ImplicitTrustingMutualTlsAuthenticator();
        final InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.1");
        final Certificate[] corpCertificates = loadCertificateChain("auth/SampleCorpCertificate.pem");
        assertFalse(authenticator.authenticate(address.getAddress(), address.getPort(), corpCertificates, INBOUND));
    }

    @Test
    public void testNoUrnsInKeystore()
    {
        final Config config = DatabaseDescriptor.getRawConfig();
        config.server_encryption_options = config.server_encryption_options.withOutboundKeystore("test/conf/cassandra_ssl_test.keystore")
                                                                           .withOutboundKeystorePassword("cassandra");
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("No URN was extracted from the outbound keystore 'test/conf/cassandra_ssl_test.keystore'");
        final IInternodeAuthenticator authenticator = new ImplicitTrustingMutualTlsAuthenticator();
    }
}
