package ru.vk.itmo.test.tveritinalexandr;

import ru.vk.itmo.ServiceConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MainServer {
    private static final Logger log = LoggerFactory.getLogger(MainServer.class);

    public static void main(String[] args) throws IOException {
        int initialPort = 8080;

        List<Integer> ports = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ports.add(initialPort);
            initialPort += 10;
        }

        List<String> clusterUrls = new ArrayList<>();
        for (int port : ports) {
            clusterUrls.add("http://localhost:" + port);
        }

        List<ServiceConfig> clusterConfs = new ArrayList<>();
        for (int port : ports) {
            ServiceConfig serviceConfig = new ServiceConfig(
                    port,
                    "http://localhost:" + port,
                    clusterUrls,
                    Path.of("/Users/tveritinaleksandr/IdeaProjects/2024-highload-dht/src/main/java/ru/vk/itmo/test/tveritinalexandr" +
                            "/data/step_3_and_next/" + port));
            clusterConfs.add(serviceConfig);
        }

        for (ServiceConfig serviceConfig : clusterConfs) {
            ServiceImpl instance = new ServiceImpl(serviceConfig);
            try {
                instance.start().get(1, TimeUnit.SECONDS);
                log.info("Node was started on port " + serviceConfig.selfPort());
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println("started");
    }
}
