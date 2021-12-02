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

import java.net.InetAddress;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.aci.cassandra.mtls.CompositeMtlsAuthenticator;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.ConfigurationException;

/*
 * Performs mTLS authentication for internode connections by extracting identities from outbound certificates
 * and verifying them against the authorized identities from cassandra.yaml.
 *
 * When this Authenticator is used, specify the valid users in the internode_authenticator.parameters.valid_ids
 *
 * Example:
 * internode_authenticator:
 *   class_name : org.apache.cassandra.auth.MutualTlsInternodeAuthenticator
 *   parameters :
 *     valid_ids: trusted_certificate_identity_1,trusted_certificate_identity_2
 *
 */
public class MutualTlsInternodeAuthenticator extends BaseMutualTlsAuthenticator implements IInternodeAuthenticator
{
    private static final String VALID_IDS_KEY_FOR_MTLS = "valid_ids";
    private static final Logger logger = LoggerFactory.getLogger(MutualTlsInternodeAuthenticator.class);

    public MutualTlsInternodeAuthenticator(Map<String, String> parameters)
    {
        final String validIdsString = parameters.get(VALID_IDS_KEY_FOR_MTLS);
        if (validIdsString == null || validIdsString.isEmpty())
        {
            final String message = "No valid clientIds in internode_authenticator.parameters.valid_ids, no internode clients will be trusted";
            logger.error(message);
            throw new ConfigurationException(message);
        }
        List<String> urns = Arrays.asList(validIdsString.split(","));

        logger.info("Creating internode authenticator with urns {}", urns);
        authenticator = new CompositeMtlsAuthenticator.Builder()
        .withManagementDsidOfRoot(CassandraRelevantProperties.MANAGEMENT_DSID_OF_ROOT.getString())
        .withAuthorizedURNS(urns)
        .build();
    }

    @Override
    public boolean authenticate(InetAddress remoteAddress, int remotePort)
    {
        throw new UnsupportedOperationException("mTLS Authenticator only supports certificate based authenticate method");
    }

    @Override
    public boolean authenticate(InetAddress remoteAddress, int remotePort, Certificate[] certificates, InternodeConnectionDirection connectionType)
    {
        return authenticateInternodeWithMtls(remoteAddress, remotePort, certificates, connectionType);
    }


    @Override
    public void validateConfiguration() throws ConfigurationException
    {

    }
}
