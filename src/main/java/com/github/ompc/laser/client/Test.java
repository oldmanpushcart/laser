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
        buffer.flip();
        System.out.println(buffer.remaining());
        buffer.compact();

        buffer.flip();
        System.out.println(buffer.remaining());

        buffer.compact();
        System.out.println(buffer.remaining());

        buffer.compact();
        System.out.println(buffer.remaining());


    }

}
