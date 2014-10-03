package com.github.ompc.laser.common.networking;

import static com.github.ompc.laser.common.LaserConstant.PRO_RESP_GETEOF;

/**
 * 返回结束报文
 * Created by vlinux on 14-9-29.
 */
public class GetEofResp extends Protocol {

    public GetEofResp() {
        super(PRO_RESP_GETEOF);
    }

}
