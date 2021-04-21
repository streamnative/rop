package com.tencent.tdmq.handlers.rocketmq.inner.consumer;

import java.nio.ByteBuffer;
import java.util.List;
import lombok.Data;
import org.apache.rocketmq.store.GetMessageResult;

/**
 * @author xiaolongran@tencent.com
 * @date 2021/4/21 5:53 下午
 */
@Data
public class RopGetMessageResult {

    private GetMessageResult getMessageResult;
    private List<ByteBuffer> byteBufferList;

    public RopGetMessageResult(GetMessageResult getMessageResult, List<ByteBuffer> byteBufferList) {
        this.getMessageResult = getMessageResult;
        this.byteBufferList = byteBufferList;
    }
}
