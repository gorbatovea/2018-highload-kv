package ru.mail.polis.service;

public class HttpMethod {

    public static String map(final int method) {
        return new String(Patterns.VERBS[method]);
    }

}
