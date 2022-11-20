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

/**
 * ![消费者-服务器-Store状态码对照表](https://raw.githubusercontent.com/qincasin/oss/main/uPic/20221120/消费者-服务器-Store 状态码对照表.jpg)
 */
public enum GetMessageStatus {

    //查询出来的MSG
    FOUND,

    // 未查询到消息：服务端消息过滤tagCode
    NO_MATCHED_MESSAGE,

    //查询时 正好赶上CommitLog 清理过期文件，导致查询失败
    MESSAGE_WAS_REMOVING,

    //未查询
    OFFSET_FOUND_NULL,


    //pullRequest.offset 越界 maxOffset
    OFFSET_OVERFLOW_BADLY,

    //pullRequest.offset == CQ.maxOffset
    OFFSET_OVERFLOW_ONE,

    //pullRequest.offset 越界 minbOffset
    OFFSET_TOO_SMALL,

    //不存在这种情况
    NO_MATCHED_LOGIC_QUEUE,

    //空队列，创建队列也是因为查询到导致的
    NO_MESSAGE_IN_QUEUE,
}
