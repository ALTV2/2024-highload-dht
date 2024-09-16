package ru.vk.itmo.test.tveritinalexandr;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import one.nio.util.Hash;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vk.itmo.ServiceConfig;
import ru.vk.itmo.dao.BaseEntry;
import ru.vk.itmo.dao.Dao;
import ru.vk.itmo.dao.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import static one.nio.http.Request.METHOD_DELETE;
import static one.nio.http.Request.METHOD_GET;
import static one.nio.http.Request.METHOD_PUT;

public class ServerImpl extends HttpServer {
    private static final Logger log = LoggerFactory.getLogger(ServerImpl.class);
    private static final String PATH_V0_ENTITY = "/v0/entity";
    private final List<String> clusterNodes;
    private final String currentNode;
    private final HttpClient internalHttpClient;

    private final Dao<MemorySegment, Entry<MemorySegment>> dao;
    private final ExecutorService executor; //todo сделать разные executor-ы на лоакальный инвоук и удаленный (лекция 6 - time: 1:50:00)

    public ServerImpl(
            ServiceConfig config,
            Dao<MemorySegment, Entry<MemorySegment>> dao,
            ExecutorService executor
    ) throws IOException {
        super(adaptConfigForHttpServer(config));
        this.dao = dao;
        this.executor = executor;
        this.currentNode = config.selfUrl();
        this.clusterNodes = config.clusterUrls();
        this.internalHttpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(500))
                .executor(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    @Path(PATH_V0_ENTITY)
    public Response entity(Request request, @Param(value = "id", required = false) String id) {
        if (id == null || id.isBlank()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }

        String executorNode = getByNodeEntityId(id);

        if (executorNode.equals(currentNode)) {
            return invokeLocal(id, request);
        } else {
            try {
                return invokeRemote(executorNode, request);
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Thread interrupted");
                return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
            }
        }
    }

    private Response invokeRemote(String executorNode, Request request) throws IOException, InterruptedException {
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(executorNode + request.getURI()))
                .method(
                        request.getMethodName(),
                        request.getBody() == null
                                ? HttpRequest.BodyPublishers.noBody()
                                : HttpRequest.BodyPublishers.ofByteArray(request.getBody())
                ).build();
        HttpResponse<byte[]> httpResponse = internalHttpClient.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
        return new Response(Integer.toString(httpResponse.statusCode()), httpResponse.body());
    }

    private Response invokeLocal(String id, Request request) {
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

    private String getByNodeEntityId(String id) {
        Integer maxHash = null;
        int nodeId = 0;
        for (int i = 0; i < clusterNodes.size(); i++) {
            String url = clusterNodes.get(i);
            int result = Hash.murmur3(url + id); //todo calculate node Hash in init method at once
            if (maxHash == null || maxHash < result) {
                maxHash = result;
                nodeId = i;
            }
        }
        return clusterNodes.get(nodeId);
    }

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
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            log.warn("Workers pool queue overflow", e);
            session.sendError(Response.SERVICE_UNAVAILABLE, null);
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    private static HttpServerConfig adaptConfigForHttpServer(ServiceConfig serviceConfig) {
        HttpServerConfig httpServerConfig = new HttpServerConfig();
        httpServerConfig.selectors = Runtime.getRuntime().availableProcessors() / 2;

        AcceptorConfig acceptorConfig = new AcceptorConfig();
        acceptorConfig.reusePort = true;
        acceptorConfig.port = serviceConfig.selfPort();

        httpServerConfig.acceptors = new AcceptorConfig[]{acceptorConfig};
        httpServerConfig.closeSessions = true;

        return httpServerConfig;
    }
}
