package com.github.ompc.laser.common.channel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * GZIP压缩实现
 * Created by vlinux on 14-10-9.
 */
public class GZIPByteBufferCompress implements ByteBufferCompress {

    @Override
    public void compress(ByteBuffer src, int len, ByteBuffer dst) throws IOException {
        final byte[] unCompressData = new byte[len];
        src.get(unCompressData);
        final byte[] compressData;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
             final GZIPOutputStream gzipos = new GZIPOutputStream(baos)) {
            gzipos.write(unCompressData);
            gzipos.finish();
            gzipos.flush();
            compressData = baos.toByteArray();
        }
        dst.putInt(compressData.length);
        dst.put(compressData);
    }

    @Override
    public void unCompress(ByteBuffer src, int len, ByteBuffer dst) throws IOException {
        final byte[] compressData = new byte[len];
        src.get(compressData);
        final byte[] unCompressData;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
             final GZIPInputStream gzipis = new GZIPInputStream(new ByteArrayInputStream(compressData))) {
            final byte[] buffer = new byte[1024];
            int n;
            while ((n = gzipis.read(buffer)) >= 0) {
                baos.write(buffer, 0, n);
            }
            unCompressData = baos.toByteArray();
        }
        dst.put(unCompressData);
    }

}