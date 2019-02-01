package ru.mail.polis.service;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;

import static one.nio.http.Request.*;
import static one.nio.http.Request.VERB_CONNECT;
import static one.nio.http.Request.VERB_PATCH;

public class Patterns {

    public static final String PROXY_HEADER = "Proxied: True";
    public static final String TIMESTAMP_HEADER = "Timestamp: ";
    public static final String CLASSNAME_HEADER = "Classname: ";

    public static final String STATUS_PATH = "/v0/status";
    public static final String ENTITY_PATH = "/v0/entity";
    public static final String APPLY_PATH = "/v0/apply";

    public static final String PARAMS_SYMBOL = "?";

    public static final String ID_PARAM = "id=";
    public static final String REPLICAS_PARAM = "replicas=";

    public static final String REQUEST_FROM = "Request from";
    public static final String RESPONSE_TO = "Response to";
    public static final String PROXIED = "proxied";
    public static final String SCRIPT_NAME = "Script:";
    public static final String SPLITTER = " ";

    public static final byte[][] VERBS = {
            new byte[0],
            VERB_GET,
            VERB_POST,
            VERB_HEAD,
            VERB_OPTIONS,
            VERB_PUT,
            VERB_DELETE,
            VERB_TRACE,
            VERB_CONNECT,
            VERB_PATCH
    };

}
