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
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import org.apache.cassandra.exceptions.AuthenticationException;

import static org.apache.cassandra.auth.AuthTestUtils.getMockInetAddress;
import static org.apache.cassandra.auth.AuthTestUtils.initializeIdentityRolesTable;
import static org.apache.cassandra.auth.AuthTestUtils.loadCertificateChain;
import static org.junit.Assert.assertEquals;

public class ACIMutualTlsAuthenticatorTest extends MutualTlsAuthenticatorTest
{
    @Parameterized.Parameter(2)
    public String dsid;

    @Parameterized.Parameters()
    public static Collection<Object[]> versions()
    {
        return AuthTestUtils.setupParameterizedTestWithVariousCertificateTypes();
    }

    @BeforeClass
    public static void setup()
    {
        MutualTlsAuthenticatorTest.setup();
    }

    @Before
    public void before()
    {
        System.setProperty("cassandra.issueingcertificate.dsid", dsid);
    }

    @After
    public void after() throws IOException, TimeoutException
    {
        super.after();
    }

    String getValidatorClass()
    {
        return "org.apache.cassandra.auth.ACICertificateValidator";
    }

    @Test
    public void testGCBDCertONGCBDCluster() throws IOException, TimeoutException, CertificateException
    {
        // set cassandra.issueingcertificate.dsid property to GCBD dsid
        System.setProperty("cassandra.issueingcertificate.dsid", "1405206");
        initializeIdentityRolesTable("urn:appcertname:identityName/sredbv42-sredb-cassandra.cassandra-prod.kube");

        final Certificate[] chain = loadCertificateChain("auth/SampleGCBDCertificate.pem");
        final MutualTlsAuthenticator authenticator = createAndInitializeMtlsAuthenticator();
        final IAuthenticator.SaslNegotiator saslNegotiator = authenticator.newSaslNegotiator(getMockInetAddress(), chain);
        assertEquals("readonly_user", saslNegotiator.getAuthenticatedUser().getName());
    }

    @Test
    public void testGCBDCertONNonGCBDCCluster() throws IOException, TimeoutException, CertificateException
    {
        // set cassandra.issueingcertificate.dsid property to Non-GCBD dsid
        System.setProperty("cassandra.issueingcertificate.dsid", "1399644");
        initializeIdentityRolesTable("urn:appcertname:identityName/sredbv42-sredb-cassandra.cassandra-prod.kube");

        final Certificate[] chain = loadCertificateChain("auth/SampleGCBDCertificate.pem");
        final MutualTlsAuthenticator authenticator = createAndInitializeMtlsAuthenticator();
        final IAuthenticator.SaslNegotiator saslNegotiator = authenticator.newSaslNegotiator(getMockInetAddress(), chain);
        expectedException.expect(AuthenticationException.class);
        expectedException.expectMessage("Invalid or not supported certificate");
        saslNegotiator.getAuthenticatedUser();
    }

    @Test
    public void testInvalidUsers() throws CertificateException, IOException, TimeoutException
    {
        initializeIdentityRolesTable(identity);
        final Certificate[] clientCertificates = loadCertificateChain("auth/SampleInvalidCertificate.pem");
        final IAuthenticator mutualTlsAuthenticator = createAndInitializeMtlsAuthenticator();
        final IAuthenticator.SaslNegotiator saslNegotiator = mutualTlsAuthenticator.newSaslNegotiator(getMockInetAddress(), clientCertificates);
        expectedException.expect(AuthenticationException.class);
        expectedException.expectMessage("Invalid or not supported certificate");
        saslNegotiator.getAuthenticatedUser();
    }
}
