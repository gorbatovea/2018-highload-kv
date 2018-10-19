package ru.mail.polis.service;

import com.sun.xml.internal.bind.v2.model.core.ID;
import javafx.util.Pair;
import one.nio.http.*;
import one.nio.net.ConnectionString;
import org.apache.http.HttpResponse;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.LSMDao.LSMDao;
import sun.invoke.empty.Empty;

import static java.lang.Math.abs;

public class service extends HttpServer implements KVService {
    private final int HTTP_CODE_OK = 200;
    private final int HTTP_CODE_CREATED = 201;
    private final int HTTP_CODE_ACCEPTED = 202;
    private final int HTTP_CODE_NOT_FOUND = 404;

    private final String PROXY_HEADER = "Proxied: True";
    private final String TIMESTAMP_HEADER = "TIMESTAMP: ";

    private final String ENTITY_PATH = "/v0/entity";
    private final String STATUS_PATH = "/v0/status";

    private final String PARAMS_SYMBOL = "?";

    private final String ID_PARAM = "id=";
    private final String REPLICAS_PARAM = "replicas=";

    // 404 value
    private final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    @NotNull
    private final LSMDao dao;

    @NotNull
    private final ArrayList<HttpClient> nodes = new ArrayList<>();

    @NotNull
    private  HttpClient me;

    private RequestCondition requestCondition;

    public service(
            @NotNull HttpServerConfig config,
            @NotNull KVDao dao,
            @NotNull Set<String> topology) throws IOException {
        super(config);
        this.dao = (LSMDao) dao;
        String port = Integer.toString(config.acceptors[0].port);
        topology.forEach(node -> {
            HttpClient client = new HttpClient(new ConnectionString(node));
            nodes.add(client);
            if (me == null) {
                if (node.split(":")[2].equals(port)) {
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

    @Path(ENTITY_PATH)
    public void entity(Request request, HttpSession session) throws IOException {
        try {
            if (request.getParameter(ID_PARAM) == null) {
                session.sendError(Response.BAD_REQUEST, null);
            } else if (request.getParameter(ID_PARAM).isEmpty()) {
                session.sendError(Response.BAD_REQUEST, null);
            }

            if (request.getParameter(REPLICAS_PARAM) == null) {
                requestCondition = new RequestCondition(nodes.size());
            } else if (request.getParameter(REPLICAS_PARAM).isEmpty()) {
                requestCondition = new RequestCondition(nodes.size());
            } else {
                requestCondition = new RequestCondition(request.getParameter(REPLICAS_PARAM));
            }

            switch (request.getMethod()) {
                case Request.METHOD_PUT: {
                    /*dao.upsert(request.getParameter(ID_PARAM).getBytes(), request.getBody());
                    session.sendResponse(new Response(Response.CREATED, Response.EMPTY));*/
                    session.sendResponse(
                            upsert(
                                    request.getParameter(ID_PARAM),
                                    request.getBody(),
                                    request.getHeader(PROXY_HEADER) != null
                    ));
                    break;
                }
                case Request.METHOD_GET: {
                    /*session.sendResponse(new Response(Response.OK, dao.get(request.getParameter(ID_PARAM).getBytes())));
                    return;*/
                    session.sendResponse(
                            get(
                                    request.getParameter(ID_PARAM),
                                    request.getHeader(PROXY_HEADER) != null));
                    break;
                }
                case Request.METHOD_DELETE: {
                    /*dao.remove(request.getParameter(ID_PARAM).getBytes());
                    session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
                    return;*/
                    session.sendResponse(
                            remove(
                                    request.getParameter(ID_PARAM),
                                    request.getHeader(PROXY_HEADER) != null));
                    break;
                }
                default: {
                    Response response = buildResponse(Response.METHOD_NOT_ALLOWED, null, null);
                    logger.info(buildString(SPLITTER, RESPONSE_TO, request.getHost(), Integer.toString(response.getStatus())));
                    session.sendResponse(response);
                    break;
                }
            }
        }catch (NoSuchElementException nSEE) {
            session.sendError(Response.NOT_FOUND, null);
            nSEE.printStackTrace();

        } catch (IllegalArgumentException iAE) {
            session.sendError(Response.BAD_REQUEST, null);
            iAE.printStackTrace();
        } catch (Exception e) {
            session.sendError(Response.INTERNAL_ERROR, null);
            e.printStackTrace();
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendError(Response.NOT_FOUND, null);
    }

    @Override
    public synchronized void stop() {
        try {
            this.dao.close();
        } catch (IOException iOE){
            iOE.printStackTrace();
        }
        super.stop();
    }

    private Response upsert(
            @NotNull final String id,
            @NotNull final byte[] value,
            @NotNull final boolean proxied) {
        if (proxied) {
            //coordinator request
            try {
                dao.upsert(id.getBytes(), value);
                return new Response(Response.CREATED, Response.EMPTY);
            } catch (IOException iOE) {
                logger.error(iOE.getClass());
                return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            }
        } else {
            //user request
            ArrayList<HttpClient> requestedNodes = getNodes(id);
            //acknowledges
            int ack = 0;
            for (HttpClient node : requestedNodes) {
                if (node == me) {
                    //self put
                    try {
                        dao.upsert(id.getBytes(), value);
                        ack++;
                    } catch (IOException iOE) {
                        iOE.printStackTrace();
                    }
                } else {
                    //requesting others node
                    try {
                        Response response =
                                node.put(new StringBuilder()
                                        .append(ENTITY_PATH)
                                        .append(PARAMS_SYMBOL)
                                        .append(ID_PARAM)
                                        .append(id)
                                        .toString(),
                                value,
                                PROXY_HEADER);
                        if (response.getStatus() == HTTP_CODE_CREATED) ack++;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            //making response to user
            if (ack >= requestCondition.getAck()) {
                return new Response(Response.CREATED, Response.EMPTY);
            }
            else {
                return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
            }
        }
    }


    private Response get(@NotNull final String id,
                         @NotNull final boolean proxied) {
        if (proxied) {
            //coordinator request
            Response response = null;
            try {
                LSMDao.Value value = dao.getWithMeta(id.getBytes());
                if (value.getValue() != null) {
                    response = new Response(Response.OK, value.getValue());
                } else {
                    response = new Response(Response.NOT_FOUND, Response.EMPTY);
                }
                response.addHeader(
                        new StringBuilder()
                                .append(TIMESTAMP_HEADER)
                                .append(value.getTimeStamp())
                                .toString()
                );
            } catch (NoSuchElementException nSEE) {
                //404 Response with LONG.MIN_VALUE
                response = new Response(Response.NOT_FOUND, Response.EMPTY);
                response.addHeader(
                        new StringBuilder()
                                .append(TIMESTAMP_HEADER)
                                .append(Long.toString(Long.MIN_VALUE))
                                .toString()
                );
            }catch (IOException iOE) {
                iOE.printStackTrace();
                return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
            }
            return response;
        } else {
            //user request
            ArrayList<HttpClient> requestedNodes = getNodes(id);
            RespAnalyzer analyzer = new RespAnalyzer(requestCondition.getAck());


            for (HttpClient node : requestedNodes) {

                if (node == me) {
                    //self get
                    Response response = null;
                    try {
                        LSMDao.Value value = dao.getWithMeta(id.getBytes());
                        if (value.getValue() == null) {
                            //404 Response with timestamp
                            response = new Response(Response.NOT_FOUND, Response.EMPTY);
                            response.addHeader(
                                    new StringBuilder()
                                            .append(TIMESTAMP_HEADER)
                                            .append(value.getTimeStamp())
                                            .toString()
                            );

                        } else {
                            //200 Response with timestamp
                            response = new Response(Response.OK, value.getValue());
                            response.addHeader(
                                    new StringBuilder()
                                            .append(TIMESTAMP_HEADER)
                                            .append(value.getTimeStamp())
                                            .toString()
                            );
                        }
                    } catch (NoSuchElementException nSEE) {
                        //404 Response with LONG.MIN_VALUE
                        response = new Response(Response.NOT_FOUND, Response.EMPTY);
                        response.addHeader(
                                new StringBuilder()
                                        .append(TIMESTAMP_HEADER)
                                        .append(Long.toString(Long.MIN_VALUE))
                                        .toString()
                        );
                        nSEE.printStackTrace();
                    } catch (IOException iOE) {
                        response = new Response(Response.INTERNAL_ERROR, Response.EMPTY);
                        iOE.printStackTrace();
                    }
                    analyzer.put(response);
                } else {
                    //requesting others node
                    try {
                        analyzer.put(
                                node.get(
                                        new StringBuilder()
                                                .append(node)
                                                .append(ENTITY_PATH)
                                                .append(PARAMS_SYMBOL)
                                                .append(ID_PARAM)
                                                .append(id)
                                                .toString(),
                                        PROXY_HEADER
                                ));
                    } catch (Exception e) {
                        analyzer.put(new Response(Response.INTERNAL_ERROR, Response.EMPTY));
                        e.printStackTrace();
                    }
                }
            }
            //making response to user
            System.out.println("Response " + analyzer.getResponse().getStatus());
            return analyzer.getResponse();
        }
    }

    private Response remove (@NotNull final String id,
                             @NotNull final boolean proxied) {
        if (proxied) {
            //coordinator request
            dao.remove(id.getBytes());
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            //user request
            ArrayList<HttpClient> requestedNodes = getNodes(id);
            //acknowledges
            int ack = 0;
            for (HttpClient node : requestedNodes) {
                if (node == me) {
                    //self remove
                    try {
                        dao.remove(id.getBytes());
                        ack++;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    //requesting others node
                    try {
                        Response response =
                                node.put(new StringBuilder()
                                                .append(ENTITY_PATH)
                                                .append(PARAMS_SYMBOL)
                                                .append(ID_PARAM)
                                                .append(id)
                                                .toString(),
                                        PROXY_HEADER);
                        if (response.getStatus() == HTTP_CODE_ACCEPTED) ack++;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            //making response to user
            if (ack >= requestCondition.getAck()) {
                return new Response(Response.ACCEPTED, Response.EMPTY);
            }
            else {
                return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
            }
        }
    }

    private ArrayList<HttpClient> getNodes(String id) {
        int base = abs(id.hashCode()) % requestCondition.getFrom();
        ArrayList<HttpClient> result = new ArrayList<>();
        for(int i = 0; i < requestCondition.getFrom(); i++){
            result.add(nodes.get(base));
            base = abs((base + 1)) % requestCondition.getFrom();
        }
        return result;
    }

    private class RespAnalyzer {
        final int condition;
        private final Map<ByteBuffer, Pair<Long, LinkedList<Response>>> okResponses = new HashMap<>();
        private Pair <Long, LinkedList<Response>> notFoundResponses = new Pair<>(Long.MIN_VALUE, new LinkedList<>());

        public RespAnalyzer(int condition) {
            System.out.println("RC:" + condition);
            this.condition = condition;
        }

        public void put(Response response) {
            System.out.println("Putting response " + response.getStatus());
            switch (response.getStatus()) {
                case HTTP_CODE_OK: {
                    ByteBuffer buffer = ByteBuffer.wrap(response.getBody());
                    long timeStamp = Long.parseLong(response.getHeader(TIMESTAMP_HEADER));
                    Pair<Long, LinkedList<Response>> pair = okResponses.get(buffer);
                    if (pair == null) {
                        pair = new Pair<>(timeStamp, new LinkedList<>());
                        pair.getValue().add(response);
                        okResponses.put(buffer, pair);
                    } else {
                        if (pair.getKey() < timeStamp) {
                            Pair<Long, LinkedList<Response>> newPair = new Pair<>(timeStamp, pair.getValue());
                            newPair.getValue().add(response);
                        }  else {
                            pair.getValue().add(response);
                        }
                    }
                    break;
                }
                case HTTP_CODE_NOT_FOUND: {
                    long timeStamp = Long.parseLong(response.getHeader(TIMESTAMP_HEADER));
                    if (notFoundResponses.getKey() < timeStamp) {
                        Pair<Long, LinkedList<Response>> newPair = new Pair<>(timeStamp, notFoundResponses.getValue());
                        newPair.getValue().add(response);
                        notFoundResponses = newPair;
                    } else {
                        notFoundResponses.getValue().add(response);
                    }
                    break;
                }
            }
        }

        public Response getResponse() {
            Pair<Long, LinkedList<Response>> okPair = null;
            int okAmount = 0;
            for (Pair<Long, LinkedList<Response>> pair : okResponses.values()) {
                if (okPair == null) {
                    okPair = pair;
                    okAmount = pair.getValue().size();
                }
                else if (okPair.getKey() < pair.getKey()) {
                    okPair = pair;
                    okAmount = pair.getValue().size();
                }
            }
            if (okPair == null) {
                if (notFoundResponses.getValue().getFirst() == null) throw new IllegalStateException();
                else {
                    if (notFoundResponses.getValue().size() >= condition) {
                        return new Response(Response.NOT_FOUND, Response.EMPTY);
                    } else {
                        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
                    }
                }
            }

            if (okPair.getKey() > notFoundResponses.getKey()) {
                //Ok Response
                if (okAmount >= condition) {
                    return new Response(Response.OK, okPair.getValue().getFirst().getBody());
                } else {
                    return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
                }
            } else {
                //Not Found Response
                System.out.println("404: " + notFoundResponses.getValue().size());
                if (notFoundResponses.getValue().size() >= condition) {
                    return new Response(Response.NOT_FOUND, Response.EMPTY);
                } else {
                    return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
                }
            }
        }
    }


    private class RequestCondition {
        private final int ack;
        private final int from;

        public RequestCondition(@NotNull final String replicas) {
            String[] parts = replicas.split("/");
            if (parts.length != 2)
                throw new IllegalArgumentException();
            int ack = Integer.parseInt(parts[0]);
            int from = Integer.parseInt(parts[1]);
            if (ack < 1 || ack > from || from > nodes.size())
                throw new IllegalArgumentException();
            this.ack = ack;
            this.from = from;
        }

        public RequestCondition(int amount) {
            this.ack = amount;
            this.from = amount;
        }

        public int getAck() {
            return ack;
        }

        public int getFrom() {
            return from;
        }
    }
}
