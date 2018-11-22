package ru.mail.polis.service;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;

public class Utils {

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

}
