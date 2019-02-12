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
package org.apache.cassandra.security;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import io.netty.handler.ssl.OpenSsl;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * A Factory for providing and setting up Client and Server SSL wrapped
 * Socket and ServerSocket
 */
public final class SSLFactory
{
    private static final Logger logger = LoggerFactory.getLogger(SSLFactory.class);

    private static boolean checkedExpiry = false;


    /**
     * Cached references of SSL Contexts
     */
    private static final ConcurrentHashMap<EncryptionOptions, SSLContext> cachedSslContexts = new ConcurrentHashMap<>();

    /**
     * List of files that trigger hot reloading of SSL certificates
     */
    private static volatile List<HotReloadableFile> hotReloadableFiles = ImmutableList.of();

    /**
     * Default initial delay for hot reloading
     */
    public static final int DEFAULT_HOT_RELOAD_INITIAL_DELAY_SEC = 600;

    /**
     * Default periodic check delay for hot reloading
     */
    public static final int DEFAULT_HOT_RELOAD_PERIOD_SEC = 600;

    /**
     * State variable to maintain initialization invariant
     */
    private static boolean isHotReloadingInitialized = false;

    /**
     * Helper class for hot reloading SSL Contexts
     */
    private static class HotReloadableFile
    {
        private final File file;
        private volatile long lastModTime;

        HotReloadableFile(String path)
        {
            file = new File(path);
            lastModTime = file.lastModified();
        }

        boolean shouldReload()
        {
            long curModTime = file.lastModified();
            boolean result = curModTime != lastModTime;
            lastModTime = curModTime;
            return result;
        }
    }


    public static SSLServerSocket getServerSocket(EncryptionOptions options, InetAddress address, int port) throws IOException
    {
        SSLContext ctx = createSSLContext(options, true);
        SSLServerSocket serverSocket = (SSLServerSocket) ctx.getServerSocketFactory().createServerSocket();
        try
        {
            serverSocket.setReuseAddress(true);
            String[] suites = filterCipherSuites(serverSocket.getSupportedCipherSuites(), options.cipher_suites);
            serverSocket.setEnabledCipherSuites(suites);
            serverSocket.setNeedClientAuth(options.require_client_auth);
            if (options.accepted_protocols.length > 0)
            {
                serverSocket.setEnabledProtocols(options.accepted_protocols);
            }
            serverSocket.bind(new InetSocketAddress(address, port), 500);
            return serverSocket;
        }
        catch (IllegalArgumentException | SecurityException | IOException e)
        {
            serverSocket.close();
            throw e;
        }
    }

    /** Create a socket and connect */
    public static SSLSocket getSocket(EncryptionOptions options, InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException
    {
        SSLContext ctx = createSSLContext(options, true);
        SSLSocket socket = (SSLSocket) ctx.getSocketFactory().createSocket(address, port, localAddress, localPort);
        try
        {
            String[] suites = filterCipherSuites(socket.getSupportedCipherSuites(), options.cipher_suites);
            socket.setEnabledCipherSuites(suites);
            if (options.accepted_protocols.length > 0)
            {
                socket.setEnabledProtocols(options.accepted_protocols);
            }
            return socket;
        }
        catch (IllegalArgumentException e)
        {
            socket.close();
            throw e;
        }
    }

    /** Create a socket and connect, using any local address */
    public static SSLSocket getSocket(EncryptionOptions options, InetAddress address, int port) throws IOException
    {
        SSLContext ctx = createSSLContext(options, true);
        SSLSocket socket = (SSLSocket) ctx.getSocketFactory().createSocket(address, port);
        try
        {
            String[] suites = filterCipherSuites(socket.getSupportedCipherSuites(), options.cipher_suites);
            socket.setEnabledCipherSuites(suites);
            if (options.accepted_protocols.length > 0)
            {
                socket.setEnabledProtocols(options.accepted_protocols);
            }
            return socket;
        }
        catch (IllegalArgumentException e)
        {
            socket.close();
            throw e;
        }
    }

    /** Just create a socket */
    public static SSLSocket getSocket(EncryptionOptions options) throws IOException
    {
        SSLContext ctx = createSSLContext(options, true);
        SSLSocket socket = (SSLSocket) ctx.getSocketFactory().createSocket();
        try
        {
            String[] suites = filterCipherSuites(socket.getSupportedCipherSuites(), options.cipher_suites);
            socket.setEnabledCipherSuites(suites);
            if (options.accepted_protocols.length > 0)
            {
                socket.setEnabledProtocols(options.accepted_protocols);
            }
            return socket;
        }
        catch (IllegalArgumentException e)
        {
            socket.close();
            throw e;
        }
    }

    @SuppressWarnings("resource")
    public static SSLContext createSSLContext(EncryptionOptions options, boolean buildTruststore) throws IOException
    {
        FileInputStream tsf = null;
        FileInputStream ksf = null;
        SSLContext ctx;
        try
        {
            ctx = SSLContext.getInstance(options.protocol);
            TrustManager[] trustManagers = null;

            if(buildTruststore)
            {
                tsf = new FileInputStream(options.truststore);
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(options.algorithm);
                KeyStore ts = KeyStore.getInstance(options.store_type);
                ts.load(tsf, options.truststore_password.toCharArray());
                tmf.init(ts);
                trustManagers = tmf.getTrustManagers();
            }

            ksf = new FileInputStream(options.keystore);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(options.algorithm);
            KeyStore ks = KeyStore.getInstance(options.store_type);
            ks.load(ksf, options.keystore_password.toCharArray());
            if (!checkedExpiry)
            {
                for (Enumeration<String> aliases = ks.aliases(); aliases.hasMoreElements(); )
                {
                    String alias = aliases.nextElement();
                    if (ks.getCertificate(alias).getType().equals("X.509"))
                    {
                        Date expires = ((X509Certificate) ks.getCertificate(alias)).getNotAfter();
                        if (expires.before(new Date()))
                            logger.warn("Certificate for {} expired on {}", alias, expires);
                    }
                }
                checkedExpiry = true;
            }
            kmf.init(ks, options.keystore_password.toCharArray());

            ctx.init(kmf.getKeyManagers(), trustManagers, null);

        }
        catch (Exception e)
        {
            throw new IOException("Error creating the initializing the SSL Context", e);
        }
        finally
        {
            FileUtils.closeQuietly(tsf);
            FileUtils.closeQuietly(ksf);
        }
        return ctx;
    }

    public static String[] filterCipherSuites(String[] supported, String[] desired)
    {
        if (Arrays.equals(supported, desired))
            return desired;
        List<String> ldesired = Arrays.asList(desired);
        ImmutableSet<String> ssupported = ImmutableSet.copyOf(supported);
        String[] ret = Iterables.toArray(Iterables.filter(ldesired, Predicates.in(ssupported)), String.class);
        if (desired.length > ret.length && logger.isWarnEnabled())
        {
            Iterable<String> missing = Iterables.filter(ldesired, Predicates.not(Predicates.in(Sets.newHashSet(ret))));
            logger.warn("Filtering out {} as it isn't supported by the socket", Iterables.toString(missing));
        }
        return ret;
    }

    public static SSLContext getOrCreateSslContext(EncryptionOptions options, boolean buildTruststore) throws IOException {
        SSLContext sslContext;

        sslContext = cachedSslContexts.get(options);
        if (sslContext != null)
            return sslContext;

        sslContext = createSSLContext(options, buildTruststore);

        SSLContext previous = cachedSslContexts.putIfAbsent(options, sslContext);
        if (previous == null)
            return sslContext;

        return previous;
    }


    /**
     * Performs a lightweight check whether the certificate files have been refreshed.
     *
     * @throws IllegalStateException if {@link #initHotReloading(EncryptionOptions.ServerEncryptionOptions, EncryptionOptions, boolean)}
     * is not called first
     */
    public static void checkCertFilesForHotReloading(EncryptionOptions.ServerEncryptionOptions serverOpts, EncryptionOptions clientOpts)
    {
        if (!isHotReloadingInitialized)
            throw new IllegalStateException("Hot reloading functionality has not been initialized.");

        logger.trace("Checking whether certificates have been updated");

        if (hotReloadableFiles.stream().anyMatch(HotReloadableFile::shouldReload))
        {
            logger.info("Server ssl certificates have been updated. Reseting the context for new peer connections.");
            try
            {
                validateSslCerts(serverOpts, clientOpts);
                cachedSslContexts.clear();
            }
            catch(Exception e)
            {
                logger.error("Failed to hot reload the SSL Certificates! Please check the certificate files.", e);
            }
        }
    }

    /**
     * Determines whether to hot reload certificates and schedules a periodic task for it.
     *
     * @param serverEncryptionOptions
     * @param clientEncryptionOptions
     */
    public static synchronized void initHotReloading(EncryptionOptions.ServerEncryptionOptions serverEncryptionOptions,
                                                     EncryptionOptions clientEncryptionOptions,
                                                     boolean force) throws IOException
    {
        if (isHotReloadingInitialized && !force)
            return;

        logger.debug("Initializing hot reloading SSLContext");

        validateSslCerts(serverEncryptionOptions, clientEncryptionOptions);

        List<HotReloadableFile> fileList = new ArrayList<>();

        if (serverEncryptionOptions.enabled)
        {
            fileList.add(new HotReloadableFile(serverEncryptionOptions.keystore));
            fileList.add(new HotReloadableFile(serverEncryptionOptions.truststore));
        }

        if (clientEncryptionOptions.enabled)
        {
            fileList.add(new HotReloadableFile(clientEncryptionOptions.keystore));
            fileList.add(new HotReloadableFile(clientEncryptionOptions.truststore));
        }

        hotReloadableFiles = ImmutableList.copyOf(fileList);

        if (!isHotReloadingInitialized)
        {
            ScheduledExecutors.scheduledTasks
                .scheduleWithFixedDelay(() -> checkCertFilesForHotReloading(
                                                DatabaseDescriptor.getServerEncryptionOptions(),
                                                DatabaseDescriptor.getClientEncryptionOptions()),
                                        DEFAULT_HOT_RELOAD_INITIAL_DELAY_SEC,
                                        DEFAULT_HOT_RELOAD_PERIOD_SEC, TimeUnit.SECONDS);
        }

        isHotReloadingInitialized = true;
    }

    /**
     * Sanity checks all certificates to ensure we can actually load them
     */
    public static void validateSslCerts(EncryptionOptions.ServerEncryptionOptions serverOpts, EncryptionOptions clientOpts) throws IOException
    {
        try
        {
            // Ensure we're able to create both server & client SslContexts
            if (serverOpts != null && serverOpts.enabled)
            {
                SSLContext ctx = createSSLContext(serverOpts, true);
                Preconditions.checkNotNull(ctx);
                ctx.createSSLEngine();
            }
        }
        catch (Exception e)
        {
            throw new IOException("Failed to create SSL context using server_encryption_options!", e);
        }

        try
        {
            // Ensure we're able to create both server & client SslContexts
            if (clientOpts != null && clientOpts.enabled)
            {
                SSLContext ctx = createSSLContext(clientOpts, clientOpts.require_client_auth);
                Preconditions.checkNotNull(ctx);
                ctx.createSSLEngine();
            }
        }
        catch (Exception e)
        {
            throw new IOException("Failed to create SSL context using client_encryption_options!", e);
        }
    }
}
