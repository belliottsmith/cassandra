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

import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.cassandra.auth.AuthTestUtils.loadCertificateChain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class ACICertificateValidatorTest
{
    @Parameterized.Parameter(0)
    public String certificatePath;
    @Parameterized.Parameter(1)
    public String identity;
    @Parameterized.Parameter(2)
    public String ouid;

    @Parameterized.Parameters()
    public static Collection<Object[]> versions()
    {
        return AuthTestUtils.setupParameterizedTestWithVariousCertificateTypes();
    }

    @Before
    public void setup()
    {
        System.setProperty("cassandra.issueingcertificate.dsid", ouid);
    }

    @Test
    public void isValidCertTest() throws CertificateException
    {
        final ACICertificateValidator validator = new ACICertificateValidator();
        final Certificate[] chain = loadCertificateChain(certificatePath);
        assertTrue(validator.isValidCertificate(chain));
    }

    @Test
    public void isValidCertWithInvalidCertTest() throws CertificateException
    {
        final ACICertificateValidator validator = new ACICertificateValidator();
        final Certificate[] chain = loadCertificateChain("auth/SampleInvalidCertificate.pem");
        assertFalse(validator.isValidCertificate(chain));
    }

    @Test
    public void getIdentityTest() throws CertificateException
    {
        final ACICertificateValidator validator = new ACICertificateValidator();
        final Certificate[] chain = loadCertificateChain(certificatePath);
        assertEquals(identity, validator.identity(chain));
    }

    @Test
    public void validateIdentityTest()
    {
        // URN should be a valid identity
        assertTrue(ACICertificateValidator.validateIdentity("urn:certmanager:idmsGroup/8073850"));
        // a non urn should return false
        assertFalse(ACICertificateValidator.validateIdentity("non-urn"));
    }
}
