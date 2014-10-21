package com.github.ompc.laser.common.atomic;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 散列原子计数器<br/>
 * 解决计数器CAS更新过热的问题
 * Created by vlinux on 14/10/21.
 */
public final class HashAtomicInteger {

    private AtomicInteger[] integers = new AtomicInteger[Runtime.getRuntime().availableProcessors()];

    public HashAtomicInteger() {
        for (int index = 0; index < integers.length; index++) {
            integers[index] = new AtomicInteger();
        }
    }

    /**
     * 获取计数器值
     *
     * @return 当前计数器值
     */
    public final int get() {
        int total = 0;
        for (AtomicInteger integer : integers) {
            total += integer.get();
        }
        return total;
    }

    /**
     * CAS更新原子计数器
     *
     * @param hash   HASH值
     * @param expect 期待值
     * @param update 更新值
     * @return 本次CAS更新是否成功
     */
    public final boolean compareAndSet(int hash, int expect, int update) {
        return integers[hash % integers.length].compareAndSet(expect, update);
    }

    /**
     * 重置计数器
     */
    public final void reset() {
        for (AtomicInteger integer : integers) {
            integer.set(0);
        }
    }

}
