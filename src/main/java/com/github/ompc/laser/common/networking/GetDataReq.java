package com.github.ompc.laser.common.networking;

import static com.github.ompc.laser.common.LaserConstant.PRO_REQ_GETDATA;

/**
 * 请求报文
 * Created by vlinux on 14-9-29.
 */
public class GetDataReq extends Protocol {

    public GetDataReq() {
        super(PRO_REQ_GETDATA);
    }

}
