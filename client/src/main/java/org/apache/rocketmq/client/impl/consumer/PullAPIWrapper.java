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
package org.apache.rocketmq.client.impl.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class PullAPIWrapper {
    private final InternalLogger log = ClientLogger.getLog();
    private final MQClientInstance mQClientFactory;
    private final String consumerGroup;
    private final boolean unitMode;
    //key：messageQueue，value：long 推荐拉消息使用的主机id
    private ConcurrentMap<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable =
        new ConcurrentHashMap<MessageQueue, AtomicLong>(32);
    private volatile boolean connectBrokerByUser = false;
    private volatile long defaultBrokerId = MixAll.MASTER_ID;
    private Random random = new Random(System.currentTimeMillis());
    //过滤hook
    private ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    public PullAPIWrapper(MQClientInstance mQClientFactory, String consumerGroup, boolean unitMode) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.unitMode = unitMode;
    }

    /**
     *  预处理拉消息 结果，主要将服务器端 指定mq 的拉消息下一次的推荐节点 保存到 pullFromWhichNodeTable 中，
     *  以及消费客户端过滤
     */
    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult,
        final SubscriptionData subscriptionData) {
        PullResultExt pullResultExt = (PullResultExt) pullResult;

        //更新pullFromWhichNodeTable 内该mq的 下次查询推荐 brokerId 的值
        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());

        if (PullStatus.FOUND == pullResult.getPullStatus()) {
            //使用缓冲区 表示 messageBinary
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
            //解码 ....
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);


            //msgListFilterAgain 客户端再次过滤后的list
            List<MessageExt> msgListFilterAgain = msgList;
            //客户端按照tag 值进行过滤
            if (!subscriptionData.getTagsSet().isEmpty() && !subscriptionData.isClassFilterMode()) {
                msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

            //客户端执行hook 过滤
            if (this.hasHook()) {
                FilterMessageContext filterMessageContext = new FilterMessageContext();
                filterMessageContext.setUnitMode(unitMode);
                filterMessageContext.setMsgList(msgListFilterAgain);
                this.executeHook(filterMessageContext);
            }

            for (MessageExt msg : msgListFilterAgain) {
                String traFlag = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (Boolean.parseBoolean(traFlag)) {
                    msg.setTransactionId(msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
                }
                //填写三个property字段，，，队列最小offset min offset 、 队列最大offset max offset 、消息归属 brokerName
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MIN_OFFSET,
                    Long.toString(pullResult.getMinOffset()));
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MAX_OFFSET,
                    Long.toString(pullResult.getMaxOffset()));
                msg.setBrokerName(mq.getBrokerName());
            }

            //将再次过滤后的消息list 保存到 pullResult
            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        //将pullResult 的messageBinary 设置为null， help gc
        pullResultExt.setMessageBinary(null);

        //返回预处理完的 pullResult
        return pullResult;
    }

    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        } else {
            suggest.set(brokerId);
        }
    }

    public boolean hasHook() {
        return !this.filterMessageHookList.isEmpty();
    }

    public void executeHook(final FilterMessageContext context) {
        if (!this.filterMessageHookList.isEmpty()) {
            for (FilterMessageHook hook : this.filterMessageHookList) {
                try {
                    hook.filterMessage(context);
                } catch (Throwable e) {
                    log.error("execute hook error. hookName={}", hook.hookName());
                }
            }
        }
    }

    /**
     * @param mq                            消息消费队列
     * @param subExpression                 过滤表达式 一般是null
     * @param expressionType                表达式类型
     * @param subVersion                    表达式类型 一般是 tag
     * @param offset                        客户端版本
     * @param maxNums                       本次拉消息offset
     * @param sysFlag                       拉消息最多消息条数控制限制
     * @param commitOffset                  系统标记，FLAG_COMMIT_OFFSET FLAG_SUSPEND FLAG_SUBSCRIPTION FLAG_CLASS_FILTER
     * @param brokerSuspendMaxTimeMillis    当前消息队列 commitlog日志中当前的最新偏移量（内存中）
     * @param timeoutMillis                 允许的broker 暂停的时间，毫秒为单位，默认为15s --- 控制服务器长轮训时 最长hold 的时间
     * @param communicationMode             超时时间 默认30s   网络调用 超时时间
     * @param pullCallback                  网络调用 通讯方式 默认 异步
     */
    public PullResult pullKernelImpl(
        final MessageQueue mq,
        final String subExpression,
        final String expressionType,
        final long subVersion,
        final long offset,
        final int maxNums,
        final int sysFlag,
        final long commitOffset,
        final long brokerSuspendMaxTimeMillis,
        final long timeoutMillis,
        final CommunicationMode communicationMode,
        final PullCallback pullCallback
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        //查询指定brokerName 的地址信息
        FindBrokerResult findBrokerResult =
                //参数1：brokerName
                //参数2：this.recalculatePullFromWhichNode(mq) ; 可能是0 ，也可能是1
                //参数3：继续当前broker？  false
            this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                this.recalculatePullFromWhichNode(mq), false);
        //没有找到 则先从NameServer 拉取路由信息，再重新获取broker地址
        if (null == findBrokerResult) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult =
                this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                    this.recalculatePullFromWhichNode(mq), false);
        }

        if (findBrokerResult != null) {
            //走到这里说明 找到了 broker 地址了
            {
                // check version
                if (!ExpressionType.isTagType(expressionType)
                    && findBrokerResult.getBrokerVersion() < MQVersion.Version.V4_1_0_SNAPSHOT.ordinal()) {
                    //Rocketmq 在4.1.0 之后才支持的非tag 过滤方式
                    throw new MQClientException("The broker[" + mq.getBrokerName() + ", "
                        + findBrokerResult.getBrokerVersion() + "] does not upgrade to support for filter message by " + expressionType, null);
                }
            }
            int sysFlagInner = sysFlag;

            //条件成立：findBrokerResult 表示的主机为 slave 节点，slave 不存储 offset 信息
            if (findBrokerResult.isSlave()) {
                //将 sysFlag 标记位中 CommitOffset 的位 设置为 0
                sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
            }

            //构建拉取消息请求数据对象 PullMessageRequestHeader  并且初始化值，将业务参数 全部封装进去
            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            //设置 消费组
            requestHeader.setConsumerGroup(this.consumerGroup);
            //设置topic
            requestHeader.setTopic(mq.getTopic());
            //设置队列id
            requestHeader.setQueueId(mq.getQueueId());
            //队列offset
            requestHeader.setQueueOffset(offset);
            //
            requestHeader.setMaxMsgNums(maxNums);
            requestHeader.setSysFlag(sysFlagInner);
            //commit offset
            requestHeader.setCommitOffset(commitOffset);
            //设置暂停超时毫秒
            requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
            //
            requestHeader.setSubscription(subExpression);
            requestHeader.setSubVersion(subVersion);
            requestHeader.setExpressionType(expressionType);

            //获取broker地址信息，
            String brokerAddr = findBrokerResult.getBrokerAddr();
            //是否支持类过滤
            if (PullSysFlag.hasClassFilterFlag(sysFlagInner)) {
                //计算从哪个过滤服务器拉取
                brokerAddr = computePullFromWhichFilterServer(mq.getTopic(), brokerAddr);
            }

            //使用 mQClientAPIImpl 异步拉取
            //参数1：brokerAddr 本次拉消息请求的服务器地址
            //参数2：requestHeader 拉消息业务参数封装对象
            //参数3：timeoutMillis 网络调用超时限制 30s
            //参数4：communicationMode RPC调用模式，这里是异步模式
            //参数5：pullCallback  拉消息结果处理对象
            PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(
                brokerAddr,
                requestHeader,
                timeoutMillis,
                communicationMode,
                pullCallback);

            return pullResult;
        }
        //执行到这里说明 broker name 没有找到， 直接就报错了。

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    //参数1：拉消息的队列
    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        if (this.isConnectBrokerByUser()) {
            return this.defaultBrokerId;
        }

        //获取 该mq 推荐的 主机 id
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }

        //返回主节点id 0
        return MixAll.MASTER_ID;
    }

    private String computePullFromWhichFilterServer(final String topic, final String brokerAddr)
        throws MQClientException {
        ConcurrentMap<String, TopicRouteData> topicRouteTable = this.mQClientFactory.getTopicRouteTable();
        if (topicRouteTable != null) {
            TopicRouteData topicRouteData = topicRouteTable.get(topic);
            List<String> list = topicRouteData.getFilterServerTable().get(brokerAddr);

            if (list != null && !list.isEmpty()) {
                return list.get(randomNum() % list.size());
            }
        }

        throw new MQClientException("Find Filter Server Failed, Broker Addr: " + brokerAddr + " topic: "
            + topic, null);
    }

    public boolean isConnectBrokerByUser() {
        return connectBrokerByUser;
    }

    public void setConnectBrokerByUser(boolean connectBrokerByUser) {
        this.connectBrokerByUser = connectBrokerByUser;

    }

    public int randomNum() {
        int value = random.nextInt();
        if (value < 0) {
            value = Math.abs(value);
            if (value < 0)
                value = 0;
        }
        return value;
    }

    public void registerFilterMessageHook(ArrayList<FilterMessageHook> filterMessageHookList) {
        this.filterMessageHookList = filterMessageHookList;
    }

    public long getDefaultBrokerId() {
        return defaultBrokerId;
    }

    public void setDefaultBrokerId(long defaultBrokerId) {
        this.defaultBrokerId = defaultBrokerId;
    }
}
