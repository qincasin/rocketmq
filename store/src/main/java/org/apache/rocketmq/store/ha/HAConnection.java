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
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class HAConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 上层对象
    private final HAService haService;
    // master与slave 之间会话通信的SocketChannel
    private final SocketChannel socketChannel;
    // 客户端地址
    private final String clientAddr;
    // 写数据服务
    private WriteSocketService writeSocketService;
    // 读数据服务
    private ReadSocketService readSocketService;

    // 默认值：-1，它是在slave上报过 本地的 进度 之后，被赋值。它>=0了之后，同步数据的逻辑才会运行。
    // 为什么？因为master它不知道slave节点  当前消息存储进度在哪，它就没办法去给slave推送数据嘛。
    private volatile long slaveRequestOffset = -1;

    // 保存最新的slave上报的 offset 信息，slaveAckOffset 之前的数据，都可以认为 slave 已经全部同步完成了
    // 对应的“生产者线程”需要被唤醒！
    private volatile long slaveAckOffset = -1;



    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        // 设置socket读写缓冲区 为 64kb 大小
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        this.socketChannel.socket().setSendBufferSize(1024 * 64);
        // 创建读写服务
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);
        // 自增
        this.haService.getConnectionCount().incrementAndGet();
    }

    public void start() {
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    class ReadSocketService extends ServiceThread {
        // 1mb
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        // 多路复用器
        private final Selector selector;
        // master 与 slave 之间的会话socketChannel
        private final SocketChannel socketChannel;

        // slave 向 master 传输的帧格式：
        // [long][long][long][long].....
        // slave向master上报的是 slave本地的同步进度，这个同步进度是一个long值！

        // 读取缓冲区，1mb
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        // 缓冲区处理位点
        private int processPosition = 0;
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            // socketChannel注册到多路复用器，关注“OP_READ”事件
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 在多路复用器最长阻塞1秒钟
                    this.selector.select(1000);

                    // 两种情况：1. 事件就绪（可读事件） 2. 阻塞超时

                    // 读数据服务 核心方法
                    boolean ok = this.processReadEvent();

                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }

                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;

                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        // 长时间未发生 通信的话，跳出循环，结束 HAConnection连接
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            // 设置ServiceThread状态为 stopped
            this.makeStop();

            // 将读服务对应的 写服务 也设置线程状态为 stopped
            writeSocketService.makeStop();

            // 移除 当前 HAConnection 对象，从 haService
            haService.removeConnection(HAConnection.this);
            // 减一
            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                // socket关闭
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }

        /**
         * 处理读事件
         * 返回boolean ，true 正常  false　socket半关闭状态，需要上层重建当前HAConnection对象
         */
        private boolean processReadEvent() {
            // 循环控制变量，当连续从socket读取失败 3 次（未加载到数据），那么跳出循环
            int readSizeZeroTimes = 0;

            // 条件成立：说明byteBufferRead已经全部使用完了，没有剩余的空间了..
            if (!this.byteBufferRead.hasRemaining()) {
                // 相当于清理操作，其实将 pos = 0
                this.byteBufferRead.flip();
                // 处理位点信息 归0
                this.processPosition = 0;
            }


            while (this.byteBufferRead.hasRemaining()) {
                try {

                    // 到socket读缓冲区加载数据，readSize 表示加载的数据量
                    int readSize = this.socketChannel.read(this.byteBufferRead);

                    if (readSize > 0) {
                        // CASE1：加载成功
                        readSizeZeroTimes = 0;
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();

                        // 条件成立：说明 byteBufferRead 中可读数据 最少包含一个数据帧
                        if ((this.byteBufferRead.position() - this.processPosition) >= 8) {

                            // pos 表示byteBufferRead 可读帧数据中，最后一个帧数据 （前面的帧不要了？ 是，不要了！）
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);

                            // 读取最后一帧数据，slave端当前的同步进度信息
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            // 更新处理位点
                            this.processPosition = pos;

                            // 赋值
                            HAConnection.this.slaveAckOffset = readOffset;

                            // 条件成立：slaveRequestOffset == -1 ，这个时候是给 slaveRequestOffset 赋值的逻辑！
                            // slaveRequestOffset 在 写数据服务要使用的
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }
                            // 唤醒阻塞的 “生产者线程”
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        // CASE2：加载失败，读缓冲区没有数据可加载..

                        if (++readSizeZeroTimes >= 3) {
                            // 一般从这里跳出循环
                            break;
                        }
                    } else {
                        // CASE3：socket半关闭状态，需要上层关闭当前HAConnection连接对象
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    class WriteSocketService extends ServiceThread {
        // 多路复用器
        private final Selector selector;
        // master 与 slave 之间的会话socketChannel
        private final SocketChannel socketChannel;

        // master 与 slave 传输的数据格式：
        // {[phyOffset][size][data...]}{[phyOffset][size][data...]}{[phyOffset][size][data...]}
        // phyOffset：数据区间的开始偏移量，并不表示某一条具体的消息，表示的数据块开始的偏移量位置。
        // size：同步的数据块的大小
        // data：数据块，最大32kb，可能包含多条消息的数据。

        // 协议头大小：12
        private final int headerSize = 8 + 4;
        // 帧头的缓冲区
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);

        // 下一次传输同步数据的位置信息，非常重要！(master需要知道 为给 当前slave 同步的位点)
        private long nextTransferFromWhere = -1;

        // mappedFile 的查询封装对象，内部有 byteBuffer
        private SelectMappedBufferResult selectMappedBufferResult;

        // 上一轮数据是否传输完毕
        private boolean lastWriteOver = true;
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            // socketChannel注册到多路复用器，关注“OP_WRITE”事件
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 在多路复用器阻塞，最长1秒钟
                    this.selector.select(1000);

                    // 1. socket写缓冲区 有空间可写了  2. 阻塞超时

                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        // 休眠一段时间...
                        Thread.sleep(10);
                        continue;
                    }


                    if (-1 == this.nextTransferFromWhere) {
                        // 初始化 nextTransferFromWhere 的逻辑..

                        if (0 == HAConnection.this.slaveRequestOffset) {
                            // slave是一个全新节点..走这里赋值
                            // 从最后一个正在顺序写的 mappedFile 开始同步数据

                            // 获取master 最大的offset
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();

                            // 计算 maxOffset 归属的 mappedFile 文件的 开始offset
                            masterOffset =
                                    masterOffset
                                            - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                            .getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            // 一般从这里赋值
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr
                                + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }


                    if (this.lastWriteOver) {
                        // 上一轮 待发送数据 全部发送完成

                        long interval =
                                HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                .getHaSendHeartbeatInterval()) {

                            // 发送一个 header 数据包，当做心跳 维持长连接

                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    } else {
                        // 上一轮的 待发送数据 未全部发送..

                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }


                    // 到commitLog中查询 nextTransferFromWhere 开始位置的数据，返回 smbr 对象
                    SelectMappedBufferResult selectResult =
                            HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);


                    if (selectResult != null) {
                        // size 有可能很大..
                        int size = selectResult.getSize();

                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            // 将size 设置为 32k
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        // 构建header使用
                        long thisOffset = this.nextTransferFromWhere;

                        // 增加size，下一轮传输 跳过本帧数据
                        this.nextTransferFromWhere += size;

                        // 设置byteBuffer 可访问数据区间 为 [pos, size]
                        selectResult.getByteBuffer().limit(size);

                        // 赋值给写数据服务的 selectMappedBufferResult
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        // lastWriteOver 表示 上一轮是否处理完成..
                        this.lastWriteOver = this.transferData();
                    } else {

                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            HAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();

            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            this.makeStop();

            readSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }


        /**
         * 同步数据到slave节点
         * 返回值boolean： true 表示本轮数据全部同步完成（header + smbr）  false 表示本轮同步未完成（header | smbr 其中一个未同步完成 都会返回false）
         */
        private boolean transferData() throws Exception {
            // 控制循环的变量，当写失败连续3次时，跳出循环
            int writeSizeZeroTimes = 0;


            // 帧头数据 发送操作
            // Write Header
            while (this.byteBufferHeader.hasRemaining()) {// 条件成立：说明 byteBufferHeader有待读取的数据

                // 向socket写缓冲区 写数据，writeSize 写成功的数据量
                int writeSize = this.socketChannel.write(this.byteBufferHeader);

                if (writeSize > 0) {
                    // CASE：写成功
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();

                } else if (writeSize == 0) {
                    // CASE：写失败

                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }


            // selectMappedBufferResult 保存的是本轮待同步给slave的数据
            if (null == this.selectMappedBufferResult) {// 心跳包
                // 判断心跳包有没有 全部发送完成..
                return !this.byteBufferHeader.hasRemaining();
            }



            // 归零
            writeSizeZeroTimes = 0;



            // Write Body
            if (!this.byteBufferHeader.hasRemaining()) {// 只有Header写成功之后，才进行写body逻辑

                // 循环条件:hasRemaining() 返回true 表示 smbr 内有待处理数据.. 正常情况在while条件内结束循环
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {

                    // 向socket写缓冲区写数据,writeSize表示 本次写的字节数量
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());


                    if (writeSize > 0) {
                        // CASE:写成功，不代表 selectMappedBufferResult 中的数据全部写完成..

                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        // CASE：写失败，因为socket写缓冲区写满了..
                        if (++writeSizeZeroTimes >= 3) {
                            // 跳出
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }


            // 返回值boolean： true 表示本轮数据全部同步完成（header + smbr）  false 表示本轮同步未完成（header | smbr 其中一个未同步完成 都会返回false）
            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();


            // 条件成立：说明 本轮 smbr 内的数据 全部发送完成了..
            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                // 释放
                this.selectMappedBufferResult.release();
                // 设置为Null
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
