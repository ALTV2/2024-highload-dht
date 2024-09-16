package ru.vk.itmo.test.tveritinalexandr;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vk.itmo.dao.BaseEntry;
import ru.vk.itmo.dao.Dao;
import ru.vk.itmo.dao.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import static one.nio.http.Request.METHOD_DELETE;
import static one.nio.http.Request.METHOD_GET;
import static one.nio.http.Request.METHOD_PUT;

public class ServerImpl extends HttpServer {
    private static final Logger log = LoggerFactory.getLogger(ServerImpl.class);
    private static final String PATH_V0_ENTITY = "/v0/entity";

    private final Dao<MemorySegment, Entry<MemorySegment>> dao;
    private final ExecutorService executor;

    public ServerImpl(
            HttpServerConfig config,
            Dao<MemorySegment, Entry<MemorySegment>> dao,
            ExecutorService executor
    ) throws IOException {
        super(config);
        this.dao = dao;
        this.executor = executor;
    }

    @Path(PATH_V0_ENTITY)
    public Response entity(Request request, @Param(value = "id", required = false) String id) {
        if (id == null || id.isBlank()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        switch (request.getMethod()) {
            case METHOD_GET -> {
                MemorySegment key = MemorySegment.ofArray(Utf8.toBytes(id));
                Entry<MemorySegment> entry = dao.get(key);
                if (entry == null || entry.value() == null) return new Response(Response.NOT_FOUND, Response.EMPTY);
                return Response.ok(entry.value().toArray(ValueLayout.JAVA_BYTE));
            }
            case METHOD_PUT -> {
                MemorySegment key = MemorySegment.ofArray(Utf8.toBytes(id));
                MemorySegment value = MemorySegment.ofArray(request.getBody());

                dao.upsert(new BaseEntry<>(key, value));
                return new Response(Response.CREATED, Response.EMPTY);
            }
            case METHOD_DELETE -> {
                MemorySegment key = MemorySegment.ofArray(Utf8.toBytes(id));
                dao.upsert(new BaseEntry<>(key, null));
                return new Response(Response.ACCEPTED, Response.EMPTY);
            }
            default -> {
                return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
        }
    }

//    @Override
//    public synchronized void stop() {
//        super.stop();
//        try {
//            dao.close();
//        } catch (IOException e) {
//            throw new UncheckedIOException(e);
//        }
//    }


    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        try {
            executor.execute(() -> {
                try {
                    super.handleRequest(request, session);
                } catch (Exception e) {
                    log.error("Exception during handle Request", e);
                    try {
                        session.sendError(Response.INTERNAL_ERROR, null);
                    } catch (IOException ex) {
                        log.error("Exception while sending close connection", e);
                        session.scheduleClose();
                    }}});
        } catch (RejectedExecutionException e) {
            log.warn("Workers pool queue overflow", e);
            session.sendError(Response.SERVICE_UNAVAILABLE, null);
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }
}
