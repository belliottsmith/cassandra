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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.MBeanWrapper;

import static org.apache.cassandra.auth.AuthTestUtils.getMockInetAddress;
import static org.apache.cassandra.auth.AuthTestUtils.initializeIdentityRolesTable;
import static org.apache.cassandra.auth.AuthTestUtils.loadCertificateChain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;


@RunWith(Parameterized.class)
public class MutualTlsAuthenticatorTest
{
    @Parameterized.Parameter(0)
    public String certificatePath;
    @Parameterized.Parameter(1)
    public String identity;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Parameterized.Parameters()
    public static Collection<Object[]> versions()
    {
        return Collections.singletonList(new Object[]{ "auth/SampleMtlsClientCertificate.pem", "spiffe://testdomain.com/testIdentifier/testValue" });
    }

    @BeforeClass
    public static void setup()
    {
        SchemaLoader.loadSchema();
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.initServer(0);
        ((CassandraRoleManager)DatabaseDescriptor.getRoleManager()).loadIdentityStatement();
        System.setProperty("cassandra.issueingcertificate.dsid", "1405206");
    }

    @After
    public void after() throws IOException, TimeoutException
    {
        MBeanWrapper.instance.unregisterMBean("org.apache.cassandra.auth:type=IdentitiesCache");
        StorageService.instance.truncate(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.IDENTITY_TO_ROLES);
        System.setProperty("cassandra.issueingcertificate.dsid", "1405206");
    }

    String getValidatorClass()
    {
        return "org.apache.cassandra.auth.SpiffeCertificateValidator";
    }

    @Test
    public void testAuthorizedUsers() throws CertificateException, IOException, TimeoutException
    {
        initializeIdentityRolesTable(identity);
        final Certificate[] chain = loadCertificateChain(certificatePath);

        // Verify authenticated user is as expected
        final IAuthenticator mutualTlsAuthenticator = createAndInitializeMtlsAuthenticator();
        final IAuthenticator.SaslNegotiator saslNegotiator = mutualTlsAuthenticator.newSaslNegotiator(getMockInetAddress(), chain);
        final AuthenticatedUser authenticatedUser = saslNegotiator.getAuthenticatedUser();
        assertNotNull(authenticatedUser);
        assertEquals("readonly_user", authenticatedUser.getName());
    }

    @Test
    public void testUnauthorizedUsers() throws CertificateException, IOException, TimeoutException
    {
        // As identity of certificate is not added to identity_role_table, connection should fail
        final Certificate[] chain = loadCertificateChain(certificatePath);
        final IAuthenticator mutualTlsAuthenticator = createAndInitializeMtlsAuthenticator();
        final IAuthenticator.SaslNegotiator saslNegotiator = mutualTlsAuthenticator.newSaslNegotiator(getMockInetAddress(), chain);
        expectedException.expect(AuthenticationException.class);
        expectedException.expectMessage("None of the users " + identity + " are authorized");
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
        expectedException.expectMessage("Unable to extract Spiffe from the certificate");
        saslNegotiator.getAuthenticatedUser();
    }

    @Test
    public void testChangeInValidUrns() throws CertificateException, IOException, TimeoutException
    {
        DatabaseDescriptor.setCredentialsValidity(10);
        initializeIdentityRolesTable(identity);
        final Certificate[] chain = loadCertificateChain(certificatePath);
        final IAuthenticator mutualTlsAuthenticator = createAndInitializeMtlsAuthenticator();
        final IAuthenticator.SaslNegotiator saslNegotiator = mutualTlsAuthenticator.newSaslNegotiator(getMockInetAddress(), chain);
        assertEquals("readonly_user", saslNegotiator.getAuthenticatedUser().getName());
        // following call truncates identity table. After removing the identity of certificate, we should get
        // authentication exception
        initializeIdentityRolesTable("another_id");
        expectedException.expect(AuthenticationException.class);
        expectedException.expectMessage("None of the users " + identity + " are authorized");
        saslNegotiator.getAuthenticatedUser();
    }

    @Test
    public void testValidatorClassNameIsNotSet()
    {
        expectedException.expect(ConfigurationException.class);
        expectedException.expectMessage("authenticator.parameters.validator_class_name is not set");
        new MutualTlsAuthenticator(Collections.emptyMap());
    }

    @Test
    public void testAddingAndRemovingIdentitiesToTableReflectsInCache() throws IOException, TimeoutException
    {
        DatabaseDescriptor.setCredentialsValidity(10);
        final String identity1 = "id1";
        final String identity2 = "id2";

        initializeIdentityRolesTable(identity1);
        final MutualTlsAuthenticator.IdentityCache urnCache = new MutualTlsAuthenticator.IdentityCache();
        assertEquals("readonly_user", urnCache.get(identity1));

        initializeIdentityRolesTable(identity2);
        assertNull(urnCache.get(identity1));
        assertEquals("readonly_user", urnCache.get(identity2));
    }

    MutualTlsAuthenticator createAndInitializeMtlsAuthenticator()
    {
        final Map<String, String> parameters = Collections.singletonMap("validator_class_name", getValidatorClass());
        final MutualTlsAuthenticator mutualTlsAuthenticator = new MutualTlsAuthenticator(parameters);
        mutualTlsAuthenticator.setup();
        return mutualTlsAuthenticator;
    }
}
