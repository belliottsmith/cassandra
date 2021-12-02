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

import com.apple.aci.cassandra.mtls.CompositeMtlsAuthenticator;
import com.apple.aci.cassandra.mtls.User;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;

/*
 * Performs mTLS authentication for internode connections by extracting identities from outbound certificates
 * configured for the cassandra instance.
 *
 * If the certificate identitiy is same as the identity present in the outbound keystore, the user is authorized
 * Otherwise, the connection will fail.
 *
 */
public class ImplicitTrustingMutualTlsAuthenticator extends BaseMutualTlsAuthenticator implements IInternodeAuthenticator
{
    private static final Logger logger = LoggerFactory.getLogger(ImplicitTrustingMutualTlsAuthenticator.class);

    public ImplicitTrustingMutualTlsAuthenticator()
    {
        final Config config = DatabaseDescriptor.getRawConfig();
        final List<String> urns = getUrnsFromKeystore(config.server_encryption_options.outbound_keystore,
                                                      config.server_encryption_options.outbound_keystore_password,
                                                      config.server_encryption_options.store_type);
        if (!urns.isEmpty())
        {
            logger.info("Initializing internode authenticator with urns {}", urns);
            authenticator = new CompositeMtlsAuthenticator.Builder()
            .withManagementDsidOfRoot(CassandraRelevantProperties.MANAGEMENT_DSID_OF_ROOT.getString())
            .withAuthorizedURNS(urns)
            .build();
        }
        else
        {
            final String message = String.format("No URN was extracted from the outbound keystore '%s'", config.server_encryption_options.outbound_keystore);
            logger.info(message);
            throw new ConfigurationException(message);
        }
    }

    @Override
    public boolean authenticate(InetAddress remoteAddress, int remotePort, Certificate[] certificates, InternodeConnectionDirection connectionType)
    {
        return authenticateInternodeWithMtls(remoteAddress, remotePort, certificates, connectionType);
    }

    @VisibleForTesting
    static List<String> getUrnsFromKeystore(final String outboundKeyStorePath, final String outboundKeyStorePassword, final String storeType)
    {
        final List<User> allUsers = new ArrayList<>();
        try (InputStream ksf = Files.newInputStream(Paths.get(outboundKeyStorePath)))
        {
            final KeyStore ks = KeyStore.getInstance(storeType);
            ks.load(ksf, outboundKeyStorePassword.toCharArray());
            Enumeration<String> enumeration = ks.aliases();
            final CompositeMtlsAuthenticator authenticator = new CompositeMtlsAuthenticator.Builder().build();
            while (enumeration.hasMoreElements())
            {
                final String alias = enumeration.nextElement();
                final Certificate certificate = ks.getCertificate(alias);
                final X509Certificate castedCert = (X509Certificate) certificate;
                allUsers.addAll(authenticator.allUsers(new X509Certificate[]{ castedCert }));
            }
        }
        catch (Exception e)
        {
            logger.error("Failed to get urn from outbound_keystore {}", outboundKeyStorePath, e);
        }
        return allUsers.stream().map(User::toURN).collect(Collectors.toList());
    }

    @Override
    public void validateConfiguration() throws ConfigurationException
    {

    }
}
