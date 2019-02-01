package ru.mail.polis.service;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;

import static ru.mail.polis.service.Patterns.TIMESTAMP_HEADER;

public class ResponseAnalyzer {
    private final int condition;
    private final ArrayList<ResponseHolder> responses = new ArrayList<>();
    private ResponseHolder freshestResponse = new ResponseHolder(null, null, Long.MIN_VALUE);

    public ResponseAnalyzer(int condition) {
        this.condition = condition;
    }

    public void put(
            @NotNull final Response response,
            @NotNull final HttpClient client) throws IllegalArgumentException{
        try{
            if (response.getStatus() == HttpCode.OK.code || response.getStatus() == HttpCode.NOT_FOUND.code) {
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