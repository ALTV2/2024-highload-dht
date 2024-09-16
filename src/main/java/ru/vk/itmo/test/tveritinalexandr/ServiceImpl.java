package ru.vk.itmo.test.tveritinalexandr;

import one.nio.async.CustomThreadFactory;
import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vk.itmo.Service;
import ru.vk.itmo.ServiceConfig;
import ru.vk.itmo.dao.Config;
import ru.vk.itmo.dao.Dao;
import ru.vk.itmo.dao.Entry;
import ru.vk.itmo.test.ServiceFactory;
import ru.vk.itmo.test.tveritinalexandr.dao.DaoImpl;
import ru.vk.itmo.test.viktorkorotkikh.LSMServerImpl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ServiceImpl implements Service {
//    private static final Logger log = LoggerFactory.getLogger(ServiceImpl.class);
    private static final long FLUSHING_THRESHOLD_BYTES = 1024 * 512;
    private static final int THREADS = Runtime.getRuntime().availableProcessors();
    private static final int QUEUE_SIZE = 1024;
    private final ServiceConfig serviceConfig;
    private Dao<MemorySegment, Entry<MemorySegment>> dao;
    private ServerImpl httpServer;
    private ExecutorService executor;

    public ServiceImpl(ServiceConfig serviceConfig) throws IOException {
        this.serviceConfig = serviceConfig;
    }

    private static HttpServerConfig adaptConfig(ServiceConfig serviceConfig) {
        HttpServerConfig httpServerConfig = new HttpServerConfig();
        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.reusePort = true;
        acceptorConfig.port = serviceConfig.selfPort();

        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        httpServerConfig.closeSessions = true;

        return httpServerConfig;
    }

    @Override
    public CompletableFuture<Void> start() {
        try {
            executor = new ThreadPoolExecutor(
                    THREADS,
                    THREADS,
                    1000,
                    SECONDS,
                    new ArrayBlockingQueue<>(QUEUE_SIZE),
                    new CustomThreadFactory("worker", true),
                    new ThreadPoolExecutor.AbortPolicy());
            httpServer = createServerInstance();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        httpServer.start();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> stop() throws IOException {
        httpServer.stop();
        shutDownAndAwaitTermination(executor);
        dao.close();
        return CompletableFuture.completedFuture(null);
    }

    private ServerImpl createServerInstance() throws IOException {
        dao = new DaoImpl(new Config(serviceConfig.workingDir(), FLUSHING_THRESHOLD_BYTES));
        return new ServerImpl(adaptConfig(serviceConfig), dao, executor);
    }

    private static void shutDownAndAwaitTermination(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, SECONDS)) {
                executor.shutdownNow();
                if (!executor.awaitTermination(60, SECONDS)) {
                    System.err.println("Pool did not terminate");
                }
            }
        } catch (InterruptedException ex) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @ServiceFactory(stage = 2)
    public static class FactoryImpl implements ServiceFactory.Factory {

        @Override
        public Service create(ServiceConfig config) {
            try {
                return new ServiceImpl(config);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

}
