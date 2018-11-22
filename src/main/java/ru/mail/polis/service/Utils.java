package ru.mail.polis.service;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;

public class Utils {
    public static final int HTTP_CODE_OK = 200;
    public static final int HTTP_CODE_CREATED = 201;
    public static final int HTTP_CODE_ACCEPTED = 202;
    public static final int HTTP_CODE_NOT_FOUND = 404;

    public static final String PROXY_HEADER = "Proxied: True";
    public static final String TIMESTAMP_HEADER = "TIMESTAMP: ";

    public static final String ENTITY_PATH = "/v0/entity";
    public static final String STATUS_PATH = "/v0/status";

    public static final String PARAMS_SYMBOL = "?";

    public static final String ID_PARAM = "id=";
    public static final String REPLICAS_PARAM = "replicas=";

    public static final String REQUEST_FROM = "Request from";
    public static final String RESPONSE_TO = "Response to";
    public static final String PROXIED = "proxied";
    public static final String SPLITTER = " ";

    public enum HttpMethod {
        PUT,
        DELETE,
        GET
    }

    public static class RespAnalyzer {
        private final int condition;
        private final ArrayList<ResponseHolder> responses = new ArrayList<>();
        private ResponseHolder freshestResponse = new ResponseHolder(null, null, Long.MIN_VALUE);

        public RespAnalyzer(int condition) {
            this.condition = condition;
        }

        public void put(
                @NotNull final Response response,
                @NotNull final HttpClient client) throws IllegalArgumentException{
            try{
                if (response.getStatus() == HTTP_CODE_OK || response.getStatus() == HTTP_CODE_NOT_FOUND) {
                    Long timeStamp = Long.parseLong(response.getHeader(TIMESTAMP_HEADER)); //could occurs exceptrion
                    ResponseHolder holder = new ResponseHolder(response, client, timeStamp);
                    if (holder.getTimeStamp() >= freshestResponse.getTimeStamp()) freshestResponse = holder;
                    this.responses.add(holder);
                }

            } catch (Exception e) {
                e.printStackTrace();
                throw new IllegalArgumentException();
            }
        }

        public Response getResponse() {
            if (this.responses.size() >= this.condition) {
                 return this.freshestResponse.getResponse();
            } else {
                return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
            }
        }

        private class ResponseHolder {
            Response response;
            HttpClient client;
            Long timeStamp;

            public ResponseHolder(Response response, HttpClient client, Long timeStamp) {
                this.response = response;
                this.client = client;
                this.timeStamp = timeStamp;
            }

            public Response getResponse() {
                return response;
            }

            public void setResponse(Response response) {
                this.response = response;
            }

            public HttpClient getClient() {
                return client;
            }

            public void setClient(HttpClient client) {
                this.client = client;
            }

            public Long getTimeStamp() {
                return timeStamp;
            }

            public void setTimeStamp(Long timeStamp) {
                this.timeStamp = timeStamp;
            }
        }
    }

    public static class RequestCondition {
        private final int ack;
        private final int from;

        public RequestCondition(@NotNull final String replicas, int nodesAmount) throws IllegalArgumentException{
            String[] parts = replicas.split("/");
            if (parts.length != 2)
                throw new IllegalArgumentException();
            int ack = Integer.parseInt(parts[0]);
            int from = Integer.parseInt(parts[1]);
            if (ack < 1 || ack > from || from > nodesAmount)
                throw new IllegalArgumentException();
            this.ack = ack;
            this.from = from;
        }

        public RequestCondition(int nodes) {
            this.ack = (nodes / 2) + 1;
            this.from = nodes;
        }

        public int getAck() {
            return ack;
        }

        public int getFrom() {
            return from;
        }
    }
}
