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
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.auth.AuthTestUtils.loadCertificateChain;
import static org.apache.cassandra.auth.AuthTestUtils.setupParameterizedTestWithVariousCertificateTypes;
import static org.apache.cassandra.auth.IInternodeAuthenticator.InternodeConnectionDirection.INBOUND;
import static org.junit.Assert.assertFalse;

public class ACIMutualTlsInternodeAuthenticatorTest extends MutualTlsInternodeAuthenticatorTest
{
    @Parameterized.Parameter(2)
    public String dsid;

    @Parameterized.Parameters()
    public static Collection<Object[]> versions()
    {
        return setupParameterizedTestWithVariousCertificateTypes();
    }

    @BeforeClass
    public static void initialize()
    {
        MutualTlsInternodeAuthenticatorTest.initialize();
    }

    @Before
    public void before()
    {
        System.setProperty("cassandra.issueingcertificate.dsid", dsid);
        super.before();
    }

    String getValidatorClass()
    {
        return "org.apache.cassandra.auth.ACICertificateValidator";
    }

    @Test
    public void testUnauthorizedUser() throws IOException, CertificateException, TimeoutException
    {
        System.setProperty("cassandra.issueingcertificate.dsid", "1405206");
        final InetAddressAndPort address = InetAddressAndPort.getByName("127.0.0.1");
        final Map<String, String> parameters = getParams();
        final IInternodeAuthenticator authenticator = new MutualTlsInternodeAuthenticator(parameters);
        // GCBD certificate is not present in the outbound keystore, so its identity is not trusted
        final Certificate[] clientCertificates = loadCertificateChain("auth/SampleGCBDCertificate.pem");
        assertFalse(authenticator.authenticate(address.getAddress(), address.getPort(), clientCertificates, INBOUND));
    }
}
