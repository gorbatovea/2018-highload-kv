package ru.mail.polis.loader;

import java.io.Serializable;

public class ByteClass implements Serializable {
    private final String name;
    private final byte[] bytes;

    public ByteClass(final String name, final byte[] bytes) {
        this.name = name;
        this.bytes = bytes;
    }

    public String getName() {
        return name;
    }

    public byte[] getBytes() {
        return bytes;
    }
}
