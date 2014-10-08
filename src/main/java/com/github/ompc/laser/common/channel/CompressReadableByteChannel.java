package com.github.ompc.laser.common.channel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * 实现GZIP压缩协议的ReadableByteChannel
 * Created by vlinux on 14-10-9.
 */
public class CompressReadableByteChannel implements ReadableByteChannel {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ReadableByteChannel readableByteChannel;
    private final ByteBufferCompress compress = new GZIPByteBufferCompress();

    private final ByteBuffer compressBuffer;
    private final ByteBuffer unCompressBuffer;
//    private final byte[] compressData;

    private DecodeState state = DecodeState.READ_LEN;
    private int compressLength;


    public CompressReadableByteChannel(ReadableByteChannel readableByteChannel, int size) {
        this.readableByteChannel = readableByteChannel;
        compressBuffer = ByteBuffer.allocate(size+Integer.BYTES);
        unCompressBuffer = ByteBuffer.allocate(size);
//        compressData = new byte[size];
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int count = 0;

        readableByteChannel.read(compressBuffer);
        compressBuffer.flip();

        boolean hasMore = true;
        MAIL_LOOP:
        while(hasMore) {
            hasMore = false;
            switch (state) {

                case READ_LEN : {
                    if( compressBuffer.remaining() < Integer.BYTES ) {
                        break;
                    }
                    compressLength = compressBuffer.getInt();
                    state = DecodeState.READ_DATA;
                }

                case READ_DATA: {
                    if( compressBuffer.remaining() < compressLength ) {
                        break;
                    }
//                    final byte[] compressData = new byte[compressLength];
//                    compressBuffer.get(compressData);
//                    final byte[] unCompressData = unCompress(compressData, 1024);
//                    unCompressBuffer.put(unCompressData);
                    compress.unCompress(compressBuffer, compressLength, unCompressBuffer);
                    unCompressBuffer.flip();
                    state = DecodeState.UN_COMPRESS;
                }

                case UN_COMPRESS : {
                    if( !dst.hasRemaining() ) {
                        break MAIL_LOOP;
                    }
                    while( dst.hasRemaining()
                            && unCompressBuffer.hasRemaining()) {
                        dst.put(unCompressBuffer.get());
                        count++;
                    }
                    if( !unCompressBuffer.hasRemaining() ) {
                        state = DecodeState.READ_LEN;
                        unCompressBuffer.compact();
                    }
                    hasMore = true;
                }

            }//switch

        }//while:hasMore

        compressBuffer.compact();



        return count;
    }

    @Override
    public boolean isOpen() {
        return readableByteChannel.isOpen();
    }

    @Override
    public void close() throws IOException {
        log.info("readableByteChannel was close");
        readableByteChannel.close();
    }

    enum DecodeState {
        READ_LEN,
        READ_DATA,
        UN_COMPRESS
    }

}
