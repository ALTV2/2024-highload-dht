package ru.vk.itmo.test.tveritinalexandr;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import one.nio.util.Hash;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vk.itmo.ServiceConfig;
import ru.vk.itmo.dao.Dao;
import ru.vk.itmo.dao.Entry;
import ru.vk.itmo.test.tveritinalexandr.dao.EntryWithTime;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static one.nio.http.Request.METHOD_DELETE;
import static one.nio.http.Request.METHOD_GET;
import static one.nio.http.Request.METHOD_PUT;

public class ServerImpl extends HttpServer {
    private static final Logger log = LoggerFactory.getLogger(ServerImpl.class);
    private static final String HEADER_REMOTE = "X-flag-remote-reference-server-to-node";
    private static final String HEADER_REMOTE_ONE_NIO = HEADER_REMOTE + ": true";

    private static final String HEADER_TIMESTAMP = "X-flag-remote-reference-server-to-node-time";
    private static final String HEADER_TIMESTAMP_ONE_NIO = HEADER_TIMESTAMP + ": ";

    private final List<String> clusterNodes;
    private final String currentNode;
    private final HttpClient internalHttpClient;

    private final Dao<MemorySegment, EntryWithTime<MemorySegment>> dao;
    private final ExecutorService executorLocal; //todo done -todo сделать разные executor-ы на лоакальный инвоук и удаленный (лекция 6 - time: 1:50:00)
    private final ExecutorService executorRemote; 

    public ServerImpl(
            ServiceConfig config,
            Dao<MemorySegment, EntryWithTime<MemorySegment>> dao,
            ExecutorService executorLocal,
            ExecutorService executorRemote
    ) throws IOException {
        super(adaptConfigForHttpServer(config));
        this.dao = dao;
        this.executorLocal = executorLocal;
        this.executorRemote = executorRemote;
        this.currentNode = config.selfUrl();
        this.clusterNodes = config.clusterUrls();
        this.internalHttpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(500))
                .executor(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }

    private HandleResult invokeRemote(String executorNode, Request request) throws IOException, InterruptedException {
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(executorNode + request.getURI()))
                .method(
                        request.getMethodName(),
                        request.getBody() == null
                                ? HttpRequest.BodyPublishers.noBody()
                                : HttpRequest.BodyPublishers.ofByteArray(request.getBody())
                )
                .header(HEADER_REMOTE, "true")
                .timeout(Duration.ofMillis(500))
                .build();
        HttpResponse<byte[]> httpResponse = internalHttpClient.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());

        Optional<String> header = httpResponse.headers().firstValue(HEADER_TIMESTAMP);
        long timestamp;
        if (header.isPresent()) {
            try {
                timestamp = Long.parseLong(header.get());
            } catch (Exception e) {
                log.error("Unparsed time");
                timestamp = 0;
            }
        } else {
            timestamp = 0;
        }
        return new HandleResult(httpResponse.statusCode(), httpResponse.body(), timestamp);
    }

    private HandleResult invokeLocal(String id, Request request) {
        long currentTimeMillis = System.currentTimeMillis();
        switch (request.getMethod()) {
            case METHOD_GET -> {
                MemorySegment key = MemorySegment.ofArray(Utf8.toBytes(id));
                EntryWithTime<MemorySegment> entry = dao.get(key);

                if (entry == null) {
                    return new HandleResult(HttpURLConnection.HTTP_NOT_FOUND, Response.EMPTY);
                }

                if (entry.value() == null) { //todo
                    return new HandleResult(HttpURLConnection.HTTP_NOT_FOUND, Response.EMPTY, entry.timeStamp());
                }
                return new HandleResult(HttpURLConnection.HTTP_OK, entry.value().toArray(ValueLayout.JAVA_BYTE), entry.timeStamp());
            }
            case METHOD_PUT -> {
                MemorySegment key = MemorySegment.ofArray(Utf8.toBytes(id));
                MemorySegment value = MemorySegment.ofArray(request.getBody());

                dao.upsert(new EntryWithTime<>(key, value, currentTimeMillis));
                return new HandleResult(HttpURLConnection.HTTP_CREATED, Response.EMPTY);
            }
            case METHOD_DELETE -> {
                MemorySegment key = MemorySegment.ofArray(Utf8.toBytes(id));
                dao.upsert(new EntryWithTime<>(key, null, currentTimeMillis));
                return new HandleResult(HttpURLConnection.HTTP_ACCEPTED, Response.EMPTY);            }
            default -> {
                return new HandleResult(HttpURLConnection.HTTP_BAD_METHOD, Response.EMPTY);            }
        }
    }

    private int[] getNodeIndexes(String id, int count) {
        assert count < 5;

        int[] result  = new int[count];
        int[] maxHashs = new int[count];

        for (int i = 0; i < count; i++) {
            String url = clusterNodes.get(i);
            int hash = Hash.murmur3(url + id);
            result[i] = i;
            maxHashs[i] = hash;

        }

        for (int i = count; i < clusterNodes.size(); i++) {
            String url = clusterNodes.get(i);
            int hash = Hash.murmur3(url + id);
            for (int j = 0; j < maxHashs.length; j++) {
                int maxHas = maxHashs[j];
                if (maxHas < hash) {
                    maxHashs[j] = hash;
                    result[j] = i;
                    break;
                }
            }
        }
        return result;
    }

    @Override
    public void handleRequest(Request request, HttpSession session) throws IOException {
        if (!"/v0/entity".equals(request.getPath())) {
            session.sendError(Response.BAD_REQUEST, null);
        }

        if (request.getMethod() != METHOD_GET
                && request.getMethod() != METHOD_PUT
                && request.getMethod() != METHOD_DELETE
        ) {
            session.sendError(Response.METHOD_NOT_ALLOWED, null);
        }

        String id = request.getParameter("id=");
        if (id == null || id.isBlank()) {
            session.sendError(Response.BAD_REQUEST, null);
        }

        if (request.getHeader(HEADER_REMOTE_ONE_NIO) != null) {
            executorLocal.execute(() -> {
                try {
                    HandleResult local = local(request, id);
                    Response response = new Response(String.valueOf(local.status()), local.data());
                    response.addHeader(HEADER_TIMESTAMP_ONE_NIO + local.timestamp());
                    session.sendResponse(response);
                } catch (Exception e) {
                    //todo duplicate
                    log.error("Exception during handle Request", e);
                    try {
                        session.sendResponse(new Response(Response.INTERNAL_ERROR, null));
                    } catch (IOException ex) {
                        log.error("Exception while sending close connection", e);
                        session.scheduleClose();
                    }
                }
            });
            return;
        }

        Integer ack = getInteger(request, "ack=", clusterNodes.size() / 2 + 1);
        Integer from = getInteger(request, "from=", clusterNodes.size());

        if (from <= 0 || from > clusterNodes.size() || ack > from || ack <= 0) {
            //todo another Ex
            session.sendError(Response.BAD_REQUEST, null);
        }

        int[] indexes = getNodeIndexes(id, from);
        MergeHandleResult mergeHandleResult = new MergeHandleResult(session, indexes.length, ack);
        for (int i = 0; i < indexes.length; i++) {
            int index = indexes[i];
            String executorNode =  clusterNodes.get(index);
            if (executorNode.equals(currentNode)) {
                handleAsync(executorLocal, i, mergeHandleResult, () -> local(request, id));
            } else {
                handleAsync(executorRemote, i, mergeHandleResult, () -> remote(request, executorNode));
            }
        }
    }

    private void handleAsync(ExecutorService executor, int index, MergeHandleResult mergeHandleResult, ERunnable runnable) {
        try {
            executor.execute(() -> {
                HandleResult handleResult;
                try {
                    handleResult = runnable.run();
                } catch (Exception e) {
                    log.error("Exception during handle Request", e); // todo
                    handleResult = new HandleResult(HttpURLConnection.HTTP_INTERNAL_ERROR, Response.EMPTY);
                }
                mergeHandleResult.add(index, handleResult);
            });
        } catch (Exception e) {
            mergeHandleResult.add(index, new HandleResult(HttpURLConnection.HTTP_INTERNAL_ERROR, Response.EMPTY));
        }
    }

    private HandleResult local(Request request, String id) {
        return invokeLocal(id, request);
    }

    private HandleResult remote(Request request, String executorNode) {
        try {
            return invokeRemote(executorNode, request);
        } catch (IOException e) {
            log.info("I/O exception while calling remote node", e);
            return new HandleResult(HttpURLConnection.HTTP_INTERNAL_ERROR, Response.EMPTY);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("Thread interrupted");
            return new HandleResult(HttpURLConnection.HTTP_UNAVAILABLE, Response.EMPTY);
        }
    }

    private Integer getInteger(Request request, String param, int defaultValue) {
        int ack;
        String ackStr = request.getParameter(param);
        if(ackStr == null || ackStr.isBlank()) {
            ack = defaultValue;
        } else {
            try {
                ack = Integer.parseInt(ackStr);
            } catch (Exception e) {
                throw new IllegalArgumentException("parse error");
            }
        }
        return ack;
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

    //Todo naming
    private interface ERunnable {
        HandleResult run() throws Exception;
    }
}
