package ru.vk.itmo.test.tveritinalexandr;

import ru.vk.itmo.ServiceConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

public final class MainServer {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        int port = 8080;
        String url = "http://localhost:" + port;
        ServiceConfig config = new ServiceConfig(
                port,
                url,
                Collections.singletonList(url),
                Path.of("/Users/tveritinaleksandr/IdeaProjects/2024-highload-dht/src/main/java/ru/vk/itmo/test/tveritinalexandr/data")
        );

        new ServiceImpl(config).start().get();
        System.out.println("started");
    }
}
