/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.security;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

public class SSLFactoryTest
{
    private EncryptionOptions addKeystoreOptions(EncryptionOptions options)
    {
        options.keystore = "test/conf/keystore.jks";
        options.keystore_password = "cassandra";
        options.truststore = options.keystore;
        options.truststore_password = options.keystore_password;
        options.enabled = true;
        return options;
    }

    @Test
    public void testFilterCipherSuites()
    {
        String[] supported = new String[] {"x", "b", "c", "f"};
        String[] desired = new String[] { "k", "a", "b", "c" };
        assertArrayEquals(new String[] { "b", "c" }, SSLFactory.filterCipherSuites(supported, desired));

        desired = new String[] { "c", "b", "x" };
        assertArrayEquals(desired, SSLFactory.filterCipherSuites(supported, desired));
    }

    @Test
    public void testServerSocketCiphers() throws IOException
    {
        ServerEncryptionOptions options = new EncryptionOptions.ServerEncryptionOptions();
        options.keystore = "test/conf/keystore.jks";
        options.keystore_password = "cassandra";
        options.truststore = options.keystore;
        options.truststore_password = options.keystore_password;
        options.cipher_suites = new String[] {
            "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_256_CBC_SHA",
            "TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"
        };

        // enabled ciphers must be a subset of configured ciphers with identical order
        try (SSLServerSocket socket = SSLFactory.getServerSocket(options, InetAddress.getLocalHost(), 55123))
        {
            String[] enabled = socket.getEnabledCipherSuites();
            String[] wanted = Iterables.toArray(Iterables.filter(Lists.newArrayList(options.cipher_suites),
                                                                 Predicates.in(Lists.newArrayList(enabled))),
                                                String.class);
            assertArrayEquals(wanted, enabled);
        }
    }

    @Test
    public void testGetSslContext_HappyPath() throws IOException {
        ServerEncryptionOptions options = new EncryptionOptions.ServerEncryptionOptions();
        addKeystoreOptions(options);
        SSLContext ctx = SSLFactory.getOrCreateSslContext(options, true);
        assertNotNull(ctx);
    }

    @Test
    public void testSslContextReload_HappyPath() throws IOException, InterruptedException
    {
        try
        {
            ServerEncryptionOptions defaultOptions = new EncryptionOptions.ServerEncryptionOptions();
            EncryptionOptions options = addKeystoreOptions(defaultOptions);
            options.enabled = true;

            SSLFactory.initHotReloading((ServerEncryptionOptions) options, options, true);

            SSLContext oldCtx = SSLFactory.getOrCreateSslContext(options, true);
            File keystoreFile = new File(options.keystore);

            SSLFactory.checkCertFilesForHotReloading((ServerEncryptionOptions) options, options);
            keystoreFile.setLastModified(System.currentTimeMillis() + 15000);

            SSLFactory.checkCertFilesForHotReloading((ServerEncryptionOptions) options, options);
            SSLContext newCtx = SSLFactory.getOrCreateSslContext(options, true);

            Assert.assertNotSame(oldCtx, newCtx);
        }
        finally
        {
            DatabaseDescriptor.loadConfig();
        }
    }

    @Test
    public void testSslFactoryHotReload_BadPassword_DoesNotClearExistingSslContext() throws IOException,
                                                                                            InterruptedException
    {
        try
        {
            ServerEncryptionOptions encryptionOptions = new EncryptionOptions.ServerEncryptionOptions();
            addKeystoreOptions(encryptionOptions);

            ServerEncryptionOptions options = new ServerEncryptionOptions(encryptionOptions);
            options.enabled = true;

            SSLFactory.initHotReloading(options, options, true);
            SSLContext oldCtx = SSLFactory.getOrCreateSslContext(options, true);
            File keystoreFile = new File(options.keystore);

            SSLFactory.checkCertFilesForHotReloading(options, options);
            keystoreFile.setLastModified(System.currentTimeMillis() + 5000);

            ServerEncryptionOptions modOptions = new ServerEncryptionOptions(options);
            modOptions.keystore_password = "bad password";
            SSLFactory.checkCertFilesForHotReloading(modOptions, modOptions);
            SSLContext newCtx = SSLFactory.getOrCreateSslContext(options, true);

            Assert.assertSame(oldCtx, newCtx);
        }
        finally
        {
            DatabaseDescriptor.loadConfig();
        }
    }

    @Test
    public void testSslFactoryHotReload_CorruptOrNonExistentFile_DoesNotClearExistingSslContext() throws IOException,
                                                                                                         InterruptedException
    {
        ServerEncryptionOptions encryptionOptions = new EncryptionOptions.ServerEncryptionOptions();
        addKeystoreOptions(encryptionOptions);

        try
        {
            File testKeystoreFile = new File(encryptionOptions.keystore + ".test");
            FileUtils.copyFile(new File(encryptionOptions.keystore), testKeystoreFile);
            encryptionOptions.keystore = testKeystoreFile.getPath();

            EncryptionOptions options = new ServerEncryptionOptions(encryptionOptions);
            options.enabled = true;

            SSLFactory.initHotReloading((ServerEncryptionOptions) options, options, true);
            SSLContext oldCtx = SSLFactory.getOrCreateSslContext(options, true);
            SSLFactory.checkCertFilesForHotReloading((ServerEncryptionOptions) options, options);
            testKeystoreFile.setLastModified(System.currentTimeMillis() + 15000);
            FileUtils.forceDelete(testKeystoreFile);

            SSLFactory.checkCertFilesForHotReloading((ServerEncryptionOptions) options, options);;
            SSLContext newCtx = SSLFactory.getOrCreateSslContext(options, true);

            Assert.assertSame(oldCtx, newCtx);
        }
        finally
        {
            DatabaseDescriptor.loadConfig();
            FileUtils.deleteQuietly(new File(encryptionOptions.keystore + ".test"));
        }
    }
}
