package ru.mail.polis.service;

public enum HttpCode {
    OK(200),
    CREATED(201),
    ACCEPTED(202),
    NOT_FOUND(404);

    public final int code;

    HttpCode(int code) {
        this.code = code;
    }


}
