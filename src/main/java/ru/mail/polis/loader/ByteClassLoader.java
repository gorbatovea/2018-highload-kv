package ru.mail.polis.loader;

public class ByteClassLoader extends ClassLoader {

    public Class findClass(final ByteClass byteClass) {
        return defineClass(byteClass.getName(), byteClass.getBytes(), 0, byteClass.getBytes().length);
    }

}
