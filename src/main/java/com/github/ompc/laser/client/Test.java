package com.github.ompc.laser.client;

import java.nio.ByteBuffer;

/**
 * Created by vlinux on 14-10-3.
 */
public class Test {

    public static void main(String... args) {

        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.putInt(4);
        buffer.putInt(4);

        System.out.println(buffer.position());

        buffer.flip();

        System.out.println(buffer.limit());

        buffer.getInt();

        System.out.println(buffer.position());
        System.out.println(buffer.position());

        buffer.compact();
        System.out.println(buffer.remaining());

    }

}
