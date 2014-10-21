package com.github.ompc.laser.common.atomic;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 散列Integer计数器
 * Created by vlinux on 14/10/22.
 */
public final class HashIntegerCounter {

    private final AtomicInteger[] integers = new AtomicInteger[Runtime.getRuntime().availableProcessors()];
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();


    public HashIntegerCounter() {

        for( int index=0; index<integers.length; index++ ) {
            integers[index] = new AtomicInteger();
        }

    }

    /**
     * 原子自增
     * @param expect 期待当前值
     * @return CAS更新结果
     */
    public final boolean increment(int expect) {
        final int length = integers.length;
        final int hashExpect = expect/length;
        return integers[expect % length].compareAndSet(hashExpect,hashExpect+1);
    }

    /**
     * 计算总数
     * @return 但前计数总数
     */
    public final int get() {
        int count = 0;
        lock.readLock().lock();
        try {
            for(AtomicInteger integer : integers) {
                count += integer.get();
            }
        } finally {
            lock.readLock().unlock();
        }
        return count;
    }

    /**
     * 计数器清0
     */
    public final void reset() {
        lock.writeLock().lock();
        try {
            for(AtomicInteger integer : integers) {
                integer.set(0);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

}
