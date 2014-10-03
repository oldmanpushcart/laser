package com.github.ompc.laser.common.networking;

import com.github.ompc.laser.common.LaserConstant;

import static com.github.ompc.laser.common.LaserConstant.PRO_MC;

/**
 * 协议基类
 * Created by vlinux on 14-9-29.
 */
public abstract class Protocol {

    private final int type;

    protected Protocol(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

}
