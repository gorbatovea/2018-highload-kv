package ru.mail.polis.service;

import org.jetbrains.annotations.NotNull;

public class RequestCondition {
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

