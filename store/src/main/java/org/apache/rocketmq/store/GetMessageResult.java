/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class GetMessageResult {

    //查询消息时，最底层都是mappedFile 支持的查询，他查询时返回给外层是一个 SelectMappedBufferResult。
    //mappedFile 没查询一次，都会refCount ++ ，通过 SelectMappedBufferResult 持有的 mappedFile，完成资源释放的句柄
    private final List<SelectMappedBufferResult> messageMapedList =
        new ArrayList<SelectMappedBufferResult>(100);

    //该list内存储消息，每一条消息都被转成 ByteBuffer 表示了
    private final List<ByteBuffer> messageBufferList = new ArrayList<ByteBuffer>(100);

    //查询结果的状态
    private GetMessageStatus status;
    //客户端下次再向当向Queue 拉消息时，使用的offset
    private long nextBeginOffset;
    //当前queue最小的offset
    private long minOffset;
    //当前queue最大的offset
    private long maxOffset;

    //消息中byte大小
    private int bufferTotalSize = 0;

    //服务器建议客户端下次到该queue拉消息时，使用 主/从 节点
    private boolean suggestPullingFromSlave = false;

    private int msgCount4Commercial = 0;

    public GetMessageResult() {
    }

    public GetMessageStatus getStatus() {
        return status;
    }

    public void setStatus(GetMessageStatus status) {
        this.status = status;
    }

    public long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(long minOffset) {
        this.minOffset = minOffset;
    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public List<SelectMappedBufferResult> getMessageMapedList() {
        return messageMapedList;
    }

    public List<ByteBuffer> getMessageBufferList() {
        return messageBufferList;
    }

    public void addMessage(final SelectMappedBufferResult mapedBuffer) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(
            mapedBuffer.getSize() / BrokerStatsManager.SIZE_PER_COUNT);
    }

    public void release() {
        for (SelectMappedBufferResult select : this.messageMapedList) {
            select.release();
        }
    }

    public int getBufferTotalSize() {
        return bufferTotalSize;
    }

    public void setBufferTotalSize(int bufferTotalSize) {
        this.bufferTotalSize = bufferTotalSize;
    }

    public int getMessageCount() {
        return this.messageMapedList.size();
    }

    public boolean isSuggestPullingFromSlave() {
        return suggestPullingFromSlave;
    }

    public void setSuggestPullingFromSlave(boolean suggestPullingFromSlave) {
        this.suggestPullingFromSlave = suggestPullingFromSlave;
    }

    public int getMsgCount4Commercial() {
        return msgCount4Commercial;
    }

    public void setMsgCount4Commercial(int msgCount4Commercial) {
        this.msgCount4Commercial = msgCount4Commercial;
    }

    @Override
    public String toString() {
        return "GetMessageResult [status=" + status + ", nextBeginOffset=" + nextBeginOffset + ", minOffset="
            + minOffset + ", maxOffset=" + maxOffset + ", bufferTotalSize=" + bufferTotalSize
            + ", suggestPullingFromSlave=" + suggestPullingFromSlave + "]";
    }

}
