// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import com.azure.core.util.Configuration;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

public class Tls13Track1 {
    private static final String CONNECTION_STRING = Configuration.getGlobalConfiguration()
        .get("STORAGE_CONNECTION_STRING");

    public static void main(String[] args) throws URISyntaxException, InvalidKeyException, InterruptedException {
        System.out.println("Using Track 1 implementation");

        // Print out the JVM version.
        // Throw an exception if it doesn't match what should be affected by the issue.
        printAndValidateJavaVersion();

        // Force HttpURLConnection to use TLS v1.3.
        System.setProperty("jdk.tls.client.protocols", "TLSv1.3");
        System.setProperty("https.protocols", "TLSv1.3");

        // Track 1 doesn't have the ability to configure the HTTP stack, so there is only one argument which is whether
        // to use Fiddler as a proxy.
        boolean enableFiddler = args.length >= 1 && Boolean.parseBoolean(args[0]);

        if (enableFiddler) {
            OperationContext.setDefaultProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", 8888)));
        }

        CloudStorageAccount account = CloudStorageAccount.parse(CONNECTION_STRING);
        CloudBlobClient serviceClient = account.createCloudBlobClient();

        // Send the first round of requests.
        sendRequests(serviceClient);

        // Sleep for a little bit.
        Thread.sleep(10000);

        // Send a second round of requests.
        sendRequests(serviceClient);

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

    private static void sendRequests(CloudBlobClient serviceClient) {
        try {
            for (CloudBlobContainer containerClient : serviceClient.listContainers()) {
                containerClient.deleteIfExists();
            }

            ForkJoinPool forkJoinPool = new ForkJoinPool();
            List<Callable<Void>> calls = new ArrayList<>();

            for (int i = 0; i < 10; i++) {
                String containerName = "container" + i;
                calls.add(() -> {
                    CloudBlobContainer containerClient = serviceClient.getContainerReference(containerName);
                    containerClient.create();

                    for (int j = 0; j < 100; j++) {
                        containerClient.getPageBlobReference("blob" + j).create(4096);
                    }

                    containerClient.delete();

                    return null;
                });
            }

            forkJoinPool.invokeAll(calls);
            forkJoinPool.shutdown();
            forkJoinPool.awaitTermination(5, TimeUnit.MINUTES);

            serviceClient.getContainerReference("container").delete();
        } catch (Exception ex) {
            System.out.println("Got an exception: " + ex.getMessage());
            System.out.println("Continuing execution to check for blocked threads.");
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
