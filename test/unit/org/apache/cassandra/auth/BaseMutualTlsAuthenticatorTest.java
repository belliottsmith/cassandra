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

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;


public class BaseMutualTlsAuthenticatorTest
{
    @BeforeClass
    public static void setup()
    {
        initializeDB();
    }

    @After
    public void after() throws IOException, TimeoutException
    {
        clearDB();
    }
    @Test
    public void testInvalidURN() throws IOException, TimeoutException, CertificateException
    {
        initializeRolesTable("urn:not-a-valid-urn");
        initializeRolesTable("urn:certmanager:idmsGroup/845340");

        final BaseMutualTlsAuthenticator baseMutualTlsAuthenticator = new BaseMutualTlsAuthenticator();
        baseMutualTlsAuthenticator.initializeAuthenticator();

        // Authenticator should be initialized
        assertNotNull(baseMutualTlsAuthenticator.authenticator);

        // Invalid urn in roles table should be filtered out and cassandra should start without any failure
        // Authentication should be successful for certmanager certificate
        final Certificate[] clientCertificates = loadCertificateChain("auth/SampleCorpCertificate.pem");
        assertTrue(baseMutualTlsAuthenticator.authenticator.isAuthorized(baseMutualTlsAuthenticator.castCertsToX509(clientCertificates)));
    }

    @Test
    public void testSetup() throws IOException, TimeoutException, CertificateException
    {
        initializeRolesTable("urn:certmanager:idmsGroup/845340");

        final BaseMutualTlsAuthenticator baseMutualTlsAuthenticator = new BaseMutualTlsAuthenticator();
        baseMutualTlsAuthenticator.initializeAuthenticator();

        // Authenticator should be initialized
        assertNotNull(baseMutualTlsAuthenticator.authenticator);

        // Authentication should be successful for certmanager certificate since it's identity is present in roles table
        final Certificate[] certMgrCertificates = loadCertificateChain("auth/SampleCorpCertificate.pem");
        assertTrue(baseMutualTlsAuthenticator.authenticator.isAuthorized(baseMutualTlsAuthenticator.castCertsToX509(certMgrCertificates)));

        // Authentication should fail for bedrock certificate since it's identity is present in roles table
        final Certificate[] bedrockCertificates = loadCertificateChain("auth/SampleBedRockCertificate.pem");
        assertFalse(baseMutualTlsAuthenticator.authenticator.isAuthorized(baseMutualTlsAuthenticator.castCertsToX509(bedrockCertificates)));
    }

    @Test
    public void testGDBCCertONGDBCCluster() throws IOException, TimeoutException, CertificateException
    {
        // set cassandra.issueingcertificate.dsid property to GDBC dsid
        System.setProperty("cassandra.issueingcertificate.dsid", "1405206");
        initializeRolesTable("urn:appcertname:identityName/sredbv42-sredb-cassandra.cassandra-prod.kube");

        final Certificate[] clientCertificates = loadCertificateChain("auth/SampleGDBCCertificate.pem");
        final BaseMutualTlsAuthenticator baseMutualTlsAuthenticator = new BaseMutualTlsAuthenticator();
        baseMutualTlsAuthenticator.initializeAuthenticator();
        assertTrue(baseMutualTlsAuthenticator.authenticator.isAuthorized(baseMutualTlsAuthenticator.castCertsToX509(clientCertificates)));
    }

    @Test
    public void testGDBCCertONNonGDBCCluster() throws IOException, TimeoutException, CertificateException
    {
        // set cassandra.issueingcertificate.dsid property to Non-GDBC dsid
        System.setProperty("cassandra.issueingcertificate.dsid", "1399644");
        initializeRolesTable("urn:appcertname:identityName/sredbv42-sredb-cassandra.cassandra-prod.kube");

        final Certificate[] clientCertificates = loadCertificateChain("auth/SampleGDBCCertificate.pem");
        final BaseMutualTlsAuthenticator baseMutualTlsAuthenticator = new BaseMutualTlsAuthenticator();
        baseMutualTlsAuthenticator.initializeAuthenticator();

        assertFalse(baseMutualTlsAuthenticator.authenticator.isAuthorized(baseMutualTlsAuthenticator.castCertsToX509(clientCertificates)));
    }

    static InetAddress getMockInetAddress() throws UnknownHostException
    {
        return InetAddress.getByName("127.0.0.1");
    }

    static Certificate[] loadCertificateChain(final String path) throws CertificateException
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

    static void initializeRolesTable(final String identity) throws IOException, TimeoutException
    {
        StorageService.instance.truncate(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES);
        final String insertQuery = "Insert into %s.%s (role,can_login,is_superuser,member_of,salted_hash) values ('%s',True,True,null,'testPswd')";
        QueryProcessor.process(String.format(insertQuery, SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES, identity), ConsistencyLevel.ONE);
    }

    static void initializeDB()
    {
        System.setProperty("cassandra.issueingcertificate.dsid", "1405206");
        SchemaLoader.loadSchema();
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.initServer(0);
    }

    static void clearDB() throws IOException, TimeoutException
    {
        StorageService.instance.truncate(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES);
    }

    static List<Object[]> setupParameterizedTestWithVariousCertificateTypes()
    {
        final List<Object[]> paths = new ArrayList<>();
        paths.add(new Object[]{ "auth/SampleCorpCertificate.pem", "urn:certmanager:idmsGroup/845340" });
        paths.add(new Object[]{ "auth/SampleBedRockCertificate.pem", "urn:bedrock:ns/1273976/uid/1273978" });
        paths.add(new Object[]{ "auth/SampleGDBCCertificate.pem", "urn:appcertname:identityName/sredbv42-sredb-cassandra.cassandra-prod.kube" });
        return paths;
    }
}
