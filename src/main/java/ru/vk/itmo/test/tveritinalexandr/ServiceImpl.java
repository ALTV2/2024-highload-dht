package ru.vk.itmo.test.tveritinalexandr;

import one.nio.async.CustomThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vk.itmo.Service;
import ru.vk.itmo.ServiceConfig;
import ru.vk.itmo.dao.Config;
import ru.vk.itmo.dao.Dao;
import ru.vk.itmo.dao.Entry;
import ru.vk.itmo.test.ServiceFactory;
import ru.vk.itmo.test.tveritinalexandr.dao.DaoImpl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ServiceImpl implements Service {
    private static final Logger log = LoggerFactory.getLogger(ServiceImpl.class);
    private static final long FLUSHING_THRESHOLD_BYTES = 1024 * 512;
    private static final int THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private static final int QUEUE_SIZE = 1024;
    private final ServiceConfig serviceConfig;
    private Dao<MemorySegment, Entry<MemorySegment>> dao;
    private ServerImpl httpServer;
    private ExecutorService executor;

    private boolean isStopped = false;

    public ServiceImpl(ServiceConfig serviceConfig) throws IOException {
        this.serviceConfig = serviceConfig;
    }

    @Override
    public synchronized CompletableFuture<Void> start() {
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
        isStopped = false;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> stop() throws IOException {
        if (isStopped) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            httpServer.stop();
            shutDownAndAwaitTermination(executor);
        } finally {
            dao.close();
        }

        isStopped = true;
        log.info("Node is stopped");

        return CompletableFuture.completedFuture(null);
    }

    private ServerImpl createServerInstance() throws IOException {
        dao = new DaoImpl(new Config(serviceConfig.workingDir(), FLUSHING_THRESHOLD_BYTES));
        return new ServerImpl(serviceConfig, dao, executor);
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

    @ServiceFactory(stage = 3)
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
