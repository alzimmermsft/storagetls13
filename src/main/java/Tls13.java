// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import com.azure.core.http.HttpClient;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.okhttp.OkHttpAsyncHttpClientBuilder;
import com.azure.core.util.Configuration;
import com.azure.core.util.CoreUtils;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import okhttp3.ConnectionSpec;
import okhttp3.OkHttpClient;
import okhttp3.TlsVersion;
import reactor.netty.http.Http11SslContextSpec;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.util.Collections;

public class Tls13 {
    private static final String CONNECTION_STRING = Configuration.getGlobalConfiguration()
        .get("STORAGE_CONNECTION_STRING");

    public static void main(String[] args) {
        // Print out the JVM version.
        // Throw an exception if it doesn't match what should be affected by the issue.
        printAndValidateJavaVersion();

        // First argument is the HTTP client to use.
        // Valid values are "netty" and "okhttp", if the value isn't provided, "netty" is used.
        // If the value is provided but isn't valid, the program will exit with an error.
        String httpClientToUse = CoreUtils.isNullOrEmpty(args) ? "netty" : args[0];
        if (!"netty".equals(httpClientToUse) && !"okhttp".equals(httpClientToUse)) {
            throw new RuntimeException("Invalid HTTP client provided. Valid values are 'netty' and 'okhttp'.");
        }

        // Second argument is whether to enable Fiddler proxying.
        // Valid values are "true" and "false", if the value isn't provided, "false" is used.
        boolean enableFiddler = args.length >= 2 && Boolean.parseBoolean(args[1]);

        // Given the issue is related to TLS v1.3 and TLS v1.3 not requiring the server to send 'close_notify' configure
        // the HttpClient to only use TLS v1.3.
        // This will also give an early out if the server sending request to doesn't support TLS v1.3.
        HttpClient httpClient;
        if ("okhttp".equals(httpClientToUse)) {
            httpClient = configureOkHttpClient(enableFiddler);
        } else {
            httpClient = configureNettyHttpClient(enableFiddler);
        }

        BlobServiceClient serviceClient = new BlobServiceClientBuilder()
            .httpClient(httpClient)
            .connectionString(CONNECTION_STRING)
            .buildClient();

        // The issue is related to threads being blocked when connections are closed.
        // To test this send a lot of requests to the server changing the container and blob often.
        sendRequests(serviceClient);

        // At the end of sending requests, dump the threads to see if any are blocked.
        // We are looking for threads being blocked on KeepAliveCache.
        // If there are threads blocked on KeepAliveCache, then the issue is still present.
        dumpThreads();
    }

    private static void printAndValidateJavaVersion() {
        Runtime.Version javaVersion = Runtime.version();
        System.out.println("Java version: " + javaVersion);
        if (javaVersion.feature() >= 20
            || (javaVersion.feature() == 11 && javaVersion.update() >= 18)
            || (javaVersion.feature() == 17 && javaVersion.update() >= 7)) {
            throw new RuntimeException("Only Java 11 before update 18, Java 12-16, Java 17 before update 7, and "
                + "Java 18-19 are affected by this issue.");
        }
    }

    private static HttpClient configureNettyHttpClient(boolean enableFiddler) {
        // Configure Reactor Netty to only use TLS v1.3.
        Http11SslContextSpec http11SslContextSpec = Http11SslContextSpec.forClient()
            .configure(sslContextBuilder -> sslContextBuilder.protocols("TLSv1.3"));

        NettyAsyncHttpClientBuilder builder = new NettyAsyncHttpClientBuilder(reactor.netty.http.client.HttpClient.create()
            .secure(spec -> spec.sslContext(http11SslContextSpec)));

        if (enableFiddler) {
            builder.proxy(new ProxyOptions(ProxyOptions.Type.HTTP, new InetSocketAddress("localhost", 8888)));
        }

        return builder.build();
    }

    private static HttpClient configureOkHttpClient(boolean enableFiddler) {
        // Configure OkHttp to only use TLS v1.3.
        ConnectionSpec connectionSpec = new ConnectionSpec.Builder(ConnectionSpec.MODERN_TLS)
            .tlsVersions(TlsVersion.TLS_1_3)
            .build();

        OkHttpAsyncHttpClientBuilder builder = new OkHttpAsyncHttpClientBuilder(new OkHttpClient.Builder()
            .connectionSpecs(Collections.singletonList(connectionSpec))
            .build());

        if (enableFiddler) {
            builder.proxy(new ProxyOptions(ProxyOptions.Type.HTTP, new InetSocketAddress("localhost", 8888)));
        }

        return builder.build();
    }

    private static void sendRequests(BlobServiceClient serviceClient) {
        for (int i = 0; i < 100; i++) {
            BlobContainerClient containerClient = serviceClient.createBlobContainer("container" + i);

            for (int j = 0; j < 100; j++) {
                containerClient.getBlobClient("blob" + j).getPageBlobClient()
                    .create(4096);
            }

            containerClient.delete();
        }
    }

    private static void dumpThreads() {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds());

        boolean foundBlockedThread = false;
        for (ThreadInfo threadInfo : threadInfos) {
            boolean threadBlockedOnKeepAliveCache = false;

            for (StackTraceElement stackTraceElement : threadInfo.getStackTrace()) {
                if (stackTraceElement.getClassName().contains("KeepAliveCache")) {
                    threadBlockedOnKeepAliveCache = true;
                    break;
                }
            }

            if (threadBlockedOnKeepAliveCache) {
                foundBlockedThread = true;
                System.out.println(threadInfo);
            }
        }

        if (!foundBlockedThread) {
            System.out.println("No blocked threads found.");
        } else {
            throw new RuntimeException("Found blocked threads.");
        }
    }
}
