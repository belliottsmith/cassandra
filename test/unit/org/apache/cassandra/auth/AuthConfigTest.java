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
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.auth.BaseMutualTlsAuthenticatorTest.loadCertificateChain;
import static org.apache.cassandra.auth.IInternodeAuthenticator.InternodeConnectionDirection.INBOUND;
import static org.apache.cassandra.config.YamlConfigurationLoaderTest.load;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AuthConfigTest
{
    @Test
    public void testNewInstanceForMutualTlsAuthenticator() throws IOException, CertificateException
    {
        final Config config = load("cassandra-mtls.yaml");
        config.internode_authenticator.class_name = "org.apache.cassandra.auth.MutualTlsInternodeAuthenticator";
        final MutualTlsInternodeAuthenticator authenticator = ParameterizedClass.newInstance(config.internode_authenticator,
                                                                                             Arrays.asList("", "org.apache.cassandra.auth."));

        final InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.1");

        // Valid User
        final Certificate[] corpCertificates = loadCertificateChain("auth/SampleCorpCertificate.pem");
        assertTrue(authenticator.authenticate(address.getAddress(), address.getPort(), corpCertificates, INBOUND));

        // Invalid User
        final Certificate[] bedrockCertificates = loadCertificateChain("auth/SampleBedRockCertificate.pem");
        assertFalse(authenticator.authenticate(address.getAddress(), address.getPort(), bedrockCertificates, INBOUND));
    }


    @Test
    public void testNewInstanceForImplicitTrustingMutualTlsAuthenticator() throws IOException, CertificateException
    {
        final Config config = load("cassandra-mtls.yaml");
        config.internode_authenticator.class_name = "org.apache.cassandra.auth.ImplicitTrustingMutualTlsAuthenticator";
        config.internode_authenticator.parameters.clear();
        config.server_encryption_options = config.server_encryption_options.withOutboundKeystore("test/conf/cassandra_ssl_test_outbound.keystore")
                                                                           .withOutboundKeystorePassword("cassandra");
        DatabaseDescriptor.setConfig(config);
        final ImplicitTrustingMutualTlsAuthenticator authenticator = ParameterizedClass.newInstance(config.internode_authenticator,
                                                                                                    Arrays.asList("", "org.apache.cassandra.auth."));

        final InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.1");

        // Valid User, Only bedrock certificate is present in the outbound keystore, so only bedrock identies should be trusted
        final Certificate[] bedrockCertificates = loadCertificateChain("auth/SampleBedRockCertificate.pem");
        assertTrue(authenticator.authenticate(address.getAddress(), address.getPort(), bedrockCertificates, INBOUND));

        // Invalid User, Corp certificate is not present in the outbound keystore, so corp identities should not be trusted
        final Certificate[] corpCertificates = loadCertificateChain("auth/SampleCorpCertificate.pem");
        assertFalse(authenticator.authenticate(address.getAddress(), address.getPort(), corpCertificates, INBOUND));
    }
}
