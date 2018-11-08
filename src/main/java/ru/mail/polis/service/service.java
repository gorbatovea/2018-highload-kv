package ru.mail.polis.service;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import ru.mail.polis.LSMDao.LSMDao;
import ru.mail.polis.service.Utils.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.lang.Math.abs;
import static ru.mail.polis.service.Utils.*;

public class service extends HttpServer implements KVService {
    @NotNull
    private final LSMDao dao;

    @NotNull
    private final ArrayList<HttpClient> nodes = new ArrayList<>();

    @NotNull
    private  HttpClient me = null;

    private static final Logger logger = Logger.getLogger(service.class);

    private static final String SPLITTER = " ";

    public service(
            @NotNull HttpServerConfig config,
            @NotNull KVDao dao,
            @NotNull Set<String> topology) throws IOException {
        super(config);
        this.dao = (LSMDao) dao;
        String port = Integer.toString(config.acceptors[0].port);
        topology.stream().forEach(node -> {
            HttpClient client = new HttpClient(new ConnectionString(node));
            nodes.add(client);
            if (me == null) {
                if (node.split(":")[2].equals(port)) {
                    logger.info("Server on " + node);
                    me = client;
                } else logger.info("Node on " + node);
            } else logger.info("Node on " + node);
        });
    }


    @Path(STATUS_PATH)
    public void status(Request request, HttpSession session) throws IOException {
        if (request.getMethod() == Request.METHOD_GET) {
            session.sendResponse(Response.ok(Response.EMPTY));
        } else {
            session.sendError(Response.METHOD_NOT_ALLOWED, null);
        }
    }

    private RequestCondition buildRC(String replicas) {
        return replicas == null ?
                new RequestCondition(nodes.size()) :
                (replicas.isEmpty() ?
                        new RequestCondition(nodes.size()) :
                        new RequestCondition(replicas, nodes.size()));
    }

    private String getId(String id) {
        return id == null ? null : (id.isEmpty() ? null : id);
    }

    private String buildString(@NotNull String splitter, String... chunks) {
        StringBuilder builder = new StringBuilder();
        for (String s: chunks) {
            builder
                    .append(s)
                    .append(splitter);
        }
        return builder.toString();
    }

    @Path(ENTITY_PATH)
    public void entity(Request request, HttpSession session) throws IOException {
        try {

            this.logger.info(buildString(
                    SPLITTER,
                    REQUEST_FROM,
                    request.getHost(),
                    request.getPath(),
                    PROXIED,
                    Boolean.toString(request.getHeader(PROXY_HEADER)!= null)
            ));
            String id = getId(request.getParameter(ID_PARAM));
            if (id == null) {
                session.sendError(Response.BAD_REQUEST, null);
                return;
            }
            RequestCondition rc = buildRC(request.getParameter(REPLICAS_PARAM));
            switch (request.getMethod()) {
                case Request.METHOD_PUT: {
                    Response response = upsert(id, request.getBody(), request.getHeader(PROXY_HEADER) != null, rc);
                    session.sendResponse(response);
                    this.logger.info(buildString(SPLITTER, RESPONSE_TO, request.getHost(), Integer.toString(response.getStatus())));
                    break;
                }
                case Request.METHOD_GET: {
                    Response response = get(id, request.getHeader(PROXY_HEADER) != null, rc);
                    session.sendResponse(response);
                    this.logger.info(buildString(SPLITTER, RESPONSE_TO, request.getHost(), Integer.toString(response.getStatus())));
                    break;
                }
                case Request.METHOD_DELETE: {
                    Response response = remove(id, request.getHeader(PROXY_HEADER) != null, rc);
                    session.sendResponse(response);
                    this.logger.info(buildString(SPLITTER, RESPONSE_TO, request.getHost(), Integer.toString(response.getStatus())));
                    break;
                }
                default: {
                    Response response = buildResponse(Response.METHOD_NOT_ALLOWED, null, null);
                    this.logger.info(buildString(
                            SPLITTER,
                            RESPONSE_TO,
                            request.getHost(),
                            Integer.toString(response.getStatus())
                            ));
                    session.sendResponse(response);
                    break;
                }
            }
        } catch (IllegalArgumentException iAE) {
            logger.error("Illegal argument found in request " + iAE);
            Response response = buildResponse(Response.BAD_REQUEST, null, null);
            this.logger.info(buildString(SPLITTER, RESPONSE_TO, request.getHost(), request.getHeader("PORT"), Integer.toString(response.getStatus())));
            session.sendResponse(response);
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        this.logger.info("Request from " + request.getHost() + " " + request.getMethod());
        Response response = buildResponse(Response.BAD_REQUEST, null, null);
        this.logger.info("Response to " + request.getHost() + " " + response.getStatus());
        session.sendResponse(response);
    }

    private Response upsert(@NotNull final String id, @NotNull final byte[] value, @NotNull final boolean proxied, final RequestCondition rc) {
        if (proxied) {
            try {
                dao.upsert(id.getBytes(), value);
                return new Response(Response.CREATED, Response.EMPTY);
            } catch (IOException iOE) {
                this.logger.error("Can't upsert value into dao(proxied) " + iOE.getClass());
                return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            }
        } else {
            ArrayList<HttpClient> requestedNodes = getNodes(id, rc);
            int ack = 0;
            for (HttpClient node : requestedNodes) {
                try {
                    if (node == me) {
                        dao.upsert(id.getBytes(), value);
                        ack++;
                    } else if (sendProxied(HttpMethod.PUT, node, id, value).getStatus() == HTTP_CODE_CREATED) {
                        ack++;
                    }
                } catch (Exception e) { this.logger.error("Exeption during user's upsert request " + e); }
            }
            //making response to user
            return ack >= rc.getAck() ?
                    new Response(Response.CREATED, Response.EMPTY) :
                    new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    @NotNull
    private Response remove (@NotNull final String id, @NotNull final boolean proxied, final RequestCondition rc) {
        if (proxied) {
            //coordinator request
            try {
            dao.remove(id.getBytes());
            return new Response(Response.ACCEPTED, Response.EMPTY);
            } catch (IOException iOE) {
                this.logger.error("Storage exceptions occurs during removeLocal " + iOE);
                return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            }
        } else {
            //user request
            ArrayList<HttpClient> requestedNodes = getNodes(id, rc);
            int ack = 0;
            for (HttpClient node : requestedNodes) {
                try {
                    if (node == me) {
                        //self remove
                        dao.remove(id.getBytes());
                        ack++;
                    } else {
                        //requesting others nodes
                        if (sendProxied(HttpMethod.DELETE, node, id, null).getStatus() == HTTP_CODE_ACCEPTED) ack++;
                    }
                } catch (Exception e) {
                    this.logger.error("Exception during user's remove request " + e);
                }
            }
            //making response to user
            return ack >= rc.getAck() ?
                    new Response(Response.ACCEPTED, Response.EMPTY) :
                    new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response get(@NotNull final String id, @NotNull final boolean proxied, final RequestCondition rc) {
        if (proxied) {
            //coordinator request
            return getLocal(id);
        } else {
            //user request
            ArrayList<HttpClient> requestedNodes = getNodes(id, rc);
            RespAnalyzer analyzer = new RespAnalyzer(rc.getAck());
            for (HttpClient node : requestedNodes) {
                try {
                    if (node == me) {
                        //local get
                        analyzer.put(getLocal(id), node);
                    } else {
                        //requesting others node
                        analyzer.put(sendProxied(HttpMethod.GET, node, id, null), node);
                    }
                } catch(Exception e){
                    this.logger.error("Exception during user's get request " + e);
                    analyzer.put(new Response(Response.INTERNAL_ERROR, Response.EMPTY), node);
                }
            }
            //making response to user
            return analyzer.getResponse();
        }
    }

    @NotNull
    private Response getLocal(@NotNull String id) {
        try {
            LSMDao.Value value = dao.getWithMeta(id.getBytes());
            if (value.getValue() != null) {
                //200 Response with value.timestamp()
                return buildResponse(Response.OK, value.getValue(), value.getTimeStamp());
            } else {
                //404 Response with value.timestamp()
                return buildResponse(Response.NOT_FOUND, Response.EMPTY, value.getTimeStamp());
            }

        } catch (NoSuchElementException nSEE) {
            //404 Response with LONG.MIN_VALUE
            this.logger.error("Storage exceptions occurs during getLocal " + nSEE);
            return buildResponse(Response.NOT_FOUND, Response.EMPTY, Long.MIN_VALUE);
        }catch (IOException iOE) {
            this.logger.error("Storage exceptions occurs during getLocal " + iOE);
            return buildResponse(Response.INTERNAL_ERROR, Response.EMPTY, null);
        }
    }

    @NotNull
    private Response sendProxied(HttpMethod method,
                                 @NotNull HttpClient node,
                                 @NotNull String id,
                                 @Nullable byte[] body) throws Exception{
        String request = new StringBuilder()
                .append(ENTITY_PATH)
                .append(PARAMS_SYMBOL)
                .append(ID_PARAM)
                .append(id)
                .toString();
        switch (method) {
            case PUT: return node.put(request, body, PROXY_HEADER);
            case DELETE: return node.delete(request, PROXY_HEADER);
            case GET: return node.get(request, PROXY_HEADER);
            default: return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    @NotNull
    private Response buildResponse(String respType, byte[] body, Long timeStamp) {
        Response response = body == null ?
                new Response(respType, Response.EMPTY) :
                new Response(respType, body);
        if (timeStamp != null) {
            String header =
                    new StringBuilder()
                            .append(TIMESTAMP_HEADER)
                            .append(Long.toString(timeStamp))
                            .toString();
            response.addHeader(header);
        }
        return response;
    }

    private ArrayList<HttpClient> getNodes(@NotNull String id, final RequestCondition rc) throws IllegalArgumentException{
        if (id.isEmpty()) throw new IllegalArgumentException();
        int base = abs(id.hashCode()) % rc.getFrom();
        ArrayList<HttpClient> result = new ArrayList<>();
        for(int i = 0; i < rc.getFrom(); i++){
            result.add(nodes.get(base));
            base = abs((base + 1)) % rc.getFrom();
        }
        return result;
    }
}
