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
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.concurrent.TimeoutException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MutualTlsAuthenticatorTest
{
    protected final MutualTlsAuthenticator mutualtlsAuthenticator = new MutualTlsAuthenticator();

    @BeforeClass
    public static void setup()
    {
        System.setProperty("cassandra.issueingcertificate.dsid", "1405206");
        SchemaLoader.loadSchema();
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.initServer(0);
    }

    @Test(expected = AuthenticationException.class)
    public void testInvalidURNInRolesTable() throws IOException, TimeoutException, CertificateException
    {
        // Invalid urn in roles table should be filtered out and cassandra should start with out any failure
        initializeMtlsAuthenticatorWithUrn(mutualtlsAuthenticator, true, "urn:not-a-valid-urn");
        final Certificate[] clientCertificates = loadCertificateChain("auth/SampleCorpCertificate.pem");
        final IAuthenticator.SaslNegotiator saslNegotiator = mutualtlsAuthenticator.newSaslNegotiator(getMockInetAddress(), clientCertificates);

        // Since valid urn is not present in the roles table we should get authentication exeption
        saslNegotiator.getAuthenticatedUser();
    }

    @Test
    public void testGDBCCertONGDBCCluster() throws IOException, TimeoutException, CertificateException
    {
        System.setProperty("cassandra.issueingcertificate.dsid", "1405206");
        initializeMtlsAuthenticatorWithUrn(mutualtlsAuthenticator, true, "urn:appcertname:identityName/sredbv42-sredb-cassandra.cassandra-prod.kube");
        final Certificate[] clientCertificates = loadCertificateChain("auth/SampleGDBCCertificate.pem");
        final IAuthenticator.SaslNegotiator saslNegotiator = mutualtlsAuthenticator.newSaslNegotiator(getMockInetAddress(), clientCertificates);

        final AuthenticatedUser user = saslNegotiator.getAuthenticatedUser();
        assertEquals("urn:appcertname:identityName/sredbv42-sredb-cassandra.cassandra-prod.kube", user.getName());
    }

    @Test(expected = AuthenticationException.class)
    public void testGDBCCertONNonGDBCCluster() throws IOException, TimeoutException, CertificateException
    {
        System.setProperty("cassandra.issueingcertificate.dsid", "1399644");
        initializeMtlsAuthenticatorWithUrn(mutualtlsAuthenticator, true, "urn:appcertname:identityName/sredbv42-sredb-cassandra.cassandra-prod.kube");

        final Certificate[] clientCertificates = loadCertificateChain("auth/SampleGDBCCertificate.pem");
        final IAuthenticator.SaslNegotiator saslNegotiator = mutualtlsAuthenticator.newSaslNegotiator(getMockInetAddress(), clientCertificates);
        saslNegotiator.getAuthenticatedUser();
    }

    protected InetAddress getMockInetAddress() throws UnknownHostException
    {
        return InetAddress.getByName("127.0.0.1");
    }

    protected Certificate[] loadCertificateChain(final String path) throws CertificateException
    {
        final InputStream inputStreamCorp = MutualTlsAuthenticator.class.getClassLoader().getResourceAsStream(path);
        assertNotNull(inputStreamCorp);
        final Collection<? extends Certificate> c = CertificateFactory.getInstance("X.509").generateCertificates(inputStreamCorp);
        X509Certificate[] certs = new X509Certificate[c.size()];
        for (int i = 0; i < certs.length; i++)
        {
            certs[i] = (X509Certificate) c.toArray()[i];
        }
        return certs;
    }

    protected void initializeMtlsAuthenticatorWithUrn(final IAuthenticator mutualTlsAuthenticator,
                                                      boolean shouldInitializeRolesTable,
                                                      final String identity) throws IOException, TimeoutException
    {
        StorageService.instance.truncate(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES);
        if (shouldInitializeRolesTable)
        {
            final String insertQuery = "Insert into %s.%s (role,can_login,is_superuser,member_of,salted_hash) values ('%s',True,True,null,'testPswd')";
            QueryProcessor.process(String.format(insertQuery, SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES, identity), ConsistencyLevel.ONE);
        }
        mutualTlsAuthenticator.setup();
    }
}
