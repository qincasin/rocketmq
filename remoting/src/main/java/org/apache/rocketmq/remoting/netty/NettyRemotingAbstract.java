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
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.common.ServiceThread;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

public abstract class NettyRemotingAbstract {

    /**
     * Remoting logger instance.
     */
    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    /**
     * Semaphore to limit maximum number of on-going one-way requests, which protects system memory footprint.
     */
    protected final Semaphore semaphoreOneway;

    /**
     * Semaphore to limit maximum number of on-going asynchronous requests, which protects system memory footprint.
     */
    protected final Semaphore semaphoreAsync;

    /**
     * This map caches all on-going requests.
     */
    protected final ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable =
        new ConcurrentHashMap<Integer, ResponseFuture>(256);

    /**
     * This container holds all processors per request code, aka, for each incoming request, we may look up the
     * responding processor in this map to handle the request.
     */
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
        new HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>>(64);

    /**
     * Executor to feed netty events to user defined {@link ChannelEventListener}.
     */
    protected final NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

    /**
     * The default request processor to use in case there is no exact match in {@link #processorTable} per request code.
     */
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessor;

    /**
     * SSL context via which to create {@link SslHandler}.
     */
    protected volatile SslContext sslContext;

    /**
     * custom rpc hooks
     */
    protected List<RPCHook> rpcHooks = new ArrayList<RPCHook>();


    static {
        NettyLogger.initNettyLogger();
    }

    /**
     * Constructor, specifying capacity of one-way and asynchronous semaphores.
     *
     * @param permitsOneway Number of permits for one-way requests.
     * @param permitsAsync Number of permits for asynchronous requests.
     */
    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }

    /**
     * Custom channel event listener.
     *
     * @return custom channel event listener if defined; null otherwise.
     */
    public abstract ChannelEventListener getChannelEventListener();

    /**
     * Put a netty event to the executor.
     *
     * @param event Netty event instance.
     */
    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecutor.putNettyEvent(event);
    }

    /**
     * Entry of incoming command processing.
     *
     * <p>
     * <strong>Note:</strong>
     * The incoming remoting command may be
     * <ul>
     * <li>An inquiry request from a remote peer component;</li>
     * <li>A response to a previous request issued by this very participant.</li>
     * </ul>
     * </p>
     *
     * @param ctx Channel handler context.
     * @param msg incoming remoting command.
     * @throws Exception if there were any error while processing the incoming command.
     */
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        final RemotingCommand cmd = msg;
        if (cmd != null) {
            switch (cmd.getType()) {
                case REQUEST_COMMAND:  //客户端发起的请求，走这里
                    processRequestCommand(ctx, cmd);
                    break;
                case RESPONSE_COMMAND:  //客户端响应给服务器的数据， 走这里
                    //response 类型时
                    processResponseCommand(ctx, cmd);
                    break;
                default:
                    break;
            }
        }
    }

    protected void doBeforeRpcHooks(String addr, RemotingCommand request) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook: rpcHooks) {
                rpcHook.doBeforeRequest(addr, request);
            }
        }
    }

    protected void doAfterRpcHooks(String addr, RemotingCommand request, RemotingCommand response) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook: rpcHooks) {
                rpcHook.doAfterResponse(addr, request, response);
            }
        }
    }


    /**
     * Process incoming request command issued by remote peer.
     *
     * @param ctx channel handler context.
     * @param cmd request command.
     */
    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessor : matched;
        //获取 请求id
        final int opaque = cmd.getOpaque();

        if (pair != null) {
            //封装了一个 runnable 核心逻辑入口， 在这里
            Runnable run = new Runnable() {
                @Override
                public void run() {
                    try {
                        //RPC Hook before 逻辑 ...
                        doBeforeRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd);
                        //callback，封装响应客户端的逻辑
                        final RemotingResponseCallback callback = new RemotingResponseCallback() {
                            @Override
                            public void callback(RemotingCommand response) {
                                //执行 rpc hook的 after 方法
                                doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(ctx.channel()), cmd, response);
                                //条件成立， 说明客户端 发来的请求，是需要结果的
                                if (!cmd.isOnewayRPC()) {
                                    if (response != null) {
                                        //将请求id 设置到 response.Opaque中，客户端根据 opaque的值在responseFuture找到 responseFuture 完成 结果的交互
                                        // (IO线程的结果写到 responseFuture ，
                                        // 然后将业务线程唤醒，然后业务线程去responseFuture中拿结果
                                        // )
                                        response.setOpaque(opaque);
                                        //设置当前线程是 响应类型
                                        response.markResponseType();
                                        try {
                                            //将数据交给 netty IO线程 ，完成数据的些和刷
                                            ctx.writeAndFlush(response);
                                        } catch (Throwable e) {
                                            log.error("process request over, but response failed", e);
                                            log.error(cmd.toString());
                                            log.error(response.toString());
                                        }
                                    } else {
                                        //啥也不干，可以达到长轮训的目的
                                    }
                                }
                            }
                        };


                        //nameserver 使用的是 DefaultRequestProcessor，是一个AsyncNettyRequestProcessor 自雷
                        if (pair.getObject1() instanceof AsyncNettyRequestProcessor) {
                            // DefaultRequestProcessor
                            AsyncNettyRequestProcessor processor = (AsyncNettyRequestProcessor)pair.getObject1();
                            /**
                             * 参数1  ctx
                             * 参数2 cmd 客户端 数据封装对象
                             * 参数3 callback，封装 响应客户端的逻辑
                             */
                            processor.asyncProcessRequest(ctx, cmd, callback);
                        } else {
                            NettyRequestProcessor processor = pair.getObject1();
                            RemotingCommand response = processor.processRequest(ctx, cmd);
                            callback.callback(response);
                        }
                    } catch (Throwable e) {
                        log.error("process request exception", e);
                        log.error(cmd.toString());

                        if (!cmd.isOnewayRPC()) {
                            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                                RemotingHelper.exceptionSimpleDesc(e));
                            response.setOpaque(opaque);
                            ctx.writeAndFlush(response);
                        }
                    }
                }
            };

            //拒绝策略
            if (pair.getObject1().rejectRequest()) {
                final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                    "[REJECTREQUEST]system busy, start flow control for a while");
                response.setOpaque(opaque);
                ctx.writeAndFlush(response);
                return;
            }

            try {
                //将runnable 和 ch，请求 cmd 一起封装成 RequestTask 对象
                final RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);
                //注意：这里 从IO线程 切换到 业务线程了 ！！！
                //获取处理器对应的线程池，将task 提交， task执行时，会执行runnable.run()方法
                pair.getObject2().submit(requestTask);
            } catch (RejectedExecutionException e) {
                if ((System.currentTimeMillis() % 10000) == 0) {
                    log.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                        + ", too many requests and system thread pool busy, RejectedExecutionException "
                        + pair.getObject2().toString()
                        + " request code: " + cmd.getCode());
                }

                if (!cmd.isOnewayRPC()) {
                    final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                        "[OVERLOAD]system busy, start flow control for a while");
                    response.setOpaque(opaque);
                    ctx.writeAndFlush(response);
                }
            }
        } else {
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response =
                RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setOpaque(opaque);
            ctx.writeAndFlush(response);
            log.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
        }
    }

    /**
     * Process response from remote peer to the previous issued requests.
     *
     * @param ctx channel handler context.
     * @param cmd response command instance.
     */
    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        //这里仍然会把opaque获取到，后面会释放
        final int opaque = cmd.getOpaque();
        final ResponseFuture responseFuture = responseTable.get(opaque);
        if (responseFuture != null) {
            //设置客户端 cmd
            responseFuture.setResponseCommand(cmd);
            //将responseFuture从映射表中移除
            responseTable.remove(opaque);


            //
            if (responseFuture.getInvokeCallback() != null) {
                //回调 对象处理 结果。
                executeInvokeCallback(responseFuture);
            } else {
                //设置future 结果，这一步 会调用 countDownLatch.countDown(); 将 同步调用的业务线程 唤醒，并且重新赋值给 responseCommand
                responseFuture.putResponse(cmd);
                //如果存在信号量的话 ，释放信号量
                responseFuture.release();
            }
        } else {
            log.warn("receive response, but not matched any request, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            log.warn(cmd.toString());
        }
    }

    /**
     * execute callback in callback executor. if callback executor is null, run directly in current thread
     */
    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        boolean runInThisThread = false;
        ExecutorService executor = this.getCallbackExecutor();
        if (executor != null) {
            try {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            responseFuture.executeInvokeCallback();
                        } catch (Throwable e) {
                            log.warn("execute callback in executor exception, and callback throw", e);
                        } finally {
                            responseFuture.release();
                        }
                    }
                });
            } catch (Exception e) {
                runInThisThread = true;
                log.warn("execute callback in executor exception, maybe executor busy", e);
            }
        } else {
            runInThisThread = true;
        }

        if (runInThisThread) {
            try {
                responseFuture.executeInvokeCallback();
            } catch (Throwable e) {
                log.warn("executeInvokeCallback Exception", e);
            } finally {
                responseFuture.release();
            }
        }
    }



    /**
     * Custom RPC hook.
     * Just be compatible with the previous version, use getRPCHooks instead.
     */
    @Deprecated
    protected RPCHook getRPCHook() {
        if (rpcHooks.size() > 0) {
            return rpcHooks.get(0);
        }
        return null;
    }

    /**
     * Custom RPC hooks.
     *
     * @return RPC hooks if specified; null otherwise.
     */
    public List<RPCHook> getRPCHooks() {
        return rpcHooks;
    }


    /**
     * This method specifies thread pool to use while invoking callback methods.
     *
     * @return Dedicated thread pool instance if specified; or null if the callback is supposed to be executed in the
     * netty event-loop thread.
     */
    public abstract ExecutorService getCallbackExecutor();

    /**
     * <p>
     * This method is periodically invoked to scan and expire deprecated request.
     * </p>
     */
    public void scanResponseTable() {
        final List<ResponseFuture> rfList = new LinkedList<ResponseFuture>();
        Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();

            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                rep.release();
                it.remove();
                rfList.add(rep);
                log.warn("remove timeout request, " + rep);
            }
        }

        for (ResponseFuture rf : rfList) {
            try {
                executeInvokeCallback(rf);
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
        final long timeoutMillis)
        throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        //请求id
        final int opaque = request.getOpaque();

        try {
            //参数1 客户端ch
            //参数2 请求id
            //参数3 超时时间
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis, null, null);
            //加入到映射表内
            //key opaque
            this.responseTable.put(opaque, responseFuture);
            //获取客户端地址信息
            final SocketAddress addr = channel.remoteAddress();
            //将数据写入到 客户端 ch (这个地方 意思是 从TailHandler 开始 一直往其他Handler走，一直压入到出站缓冲区里面，然后再执行flush操作，最终将数据压入到socket的写缓冲区里面)
            channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                // 这里注册了一个监听事件
                @Override
                public void operationComplete(ChannelFuture f) throws Exception {
                    if (f.isSuccess()) {
                        //表示请求 写成功了
                        responseFuture.setSendRequestOK(true);
                        //这里写成功后直接就return了，那么映射表中的数据怎么办呢？
                        //其实有两个入口可以将映射表中的数据清除掉：1.finally 中会移除 ；
                        // 2.prepareSharableHandlers()方法中的serverHandler在响应回client端时(server响应给client类型时(RESPONSE_COMMAND的类型时))，会将映射表进行移除;具体代码在NettyRemotingAbstract#processResponseCommand()方法中
                        //第二种，正常同步wait后，就会执行到 NettyRemotingAbstract#processResponseCommand()方法，这个时候就会自动移除掉映射表中的值
                        return;
                    } else {
                        //写失败了
                        responseFuture.setSendRequestOK(false);
                    }
                    //执行到这里 说明 写失败了

                    //将当前请求的 responseFuture 从 映射表 移处
                    responseTable.remove(opaque);
                    //设置失败原因
                    responseFuture.setCause(f.cause());
                    responseFuture.putResponse(null);
                    log.warn("send a request command to channel <" + addr + "> failed.");
                }
            });

            //业务线程 在这里 进入 挂起状态了
            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            //线程执行到这里 ，有两种情况
            //1.正常情况， 客户端 返回数据了，  IO线程将 业务线程 唤醒
            //2.超时情况
            if (null == responseCommand) {
                //执行进这里， 说明超时，  或者  其他 异常情况   --> 直接抛异常
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(RemotingHelper.parseSocketAddressAddr(addr), timeoutMillis,
                        responseFuture.getCause());
                } else {
                    throw new RemotingSendRequestException(RemotingHelper.parseSocketAddressAddr(addr), responseFuture.getCause());
                }
            }

            //返回客户端的 请求结果
            return responseCommand;
        } finally {
            //移处 映射表 入口1
            this.responseTable.remove(opaque);
        }
    }

    /**
     * 服务器 主动向 客户端 发起请求时  使用方法  (异步)
     * @param channel 客户端ch
     * @param request 请求对象
     * @param timeoutMillis 超时时长
     * @param invokeCallback 请求结果 回调处理对象
     * @throws InterruptedException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis,
        final InvokeCallback invokeCallback)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        //获取开始时间
        long beginStartTime = System.currentTimeMillis();
        //请求id
        final int opaque = request.getOpaque();
        //获取信号量  信号量减一
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            //执行到这里  说明 当前线程 可以发起请求， 服务器 请求客户端的 并发 并未达到 上限

            //once 对象 封装了 释放信号量的操作
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
            /**
             * 什么时候释放？
             * 请求结果回来了才会释放，或者是请求结果失败了才会释放
             */

            //计算执行到这一步  为止  耗费的时间
            long costTime = System.currentTimeMillis() - beginStartTime;

            //条件成立 说明已经超时了  就不用在发起 RPC了
            if (timeoutMillis < costTime) {
                once.release();
                throw new RemotingTimeoutException("invokeAsyncImpl call timeout");
            }

            // 执行到这一步 说明 请求执行 未超时，正常情况

            // 参数1 客户端ch
            // 参数2 请求id
            // 参数3 剩余的 超时时间
            // 参数4 回调处理对象
            // 参数5 信号量释放操作封装对象
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis - costTime, invokeCallback, once);

            //将responseFuture 加入响应映射表 ，key 其实就是 RequestId

            this.responseTable.put(opaque, responseFuture);
            try {
                //1，业务线程 将数据 交给 netty  ，netty IO 线程 接管 写 和 刷 数据的操作
                //2. 注册 写刷 操作 监听器， 监听器 由 IO线程回调

                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        if (f.isSuccess()) {
                            //写刷成功， 设置responseFuture 发生状态为 true
                            responseFuture.setSendRequestOK(true);
                            return;
                        }
                        requestFail(opaque);
                        log.warn("send a request command to channel <{}> failed.", RemotingHelper.parseChannelRemoteAddr(channel));
                    }
                });
            } catch (Exception e) {
                responseFuture.release();
                log.warn("send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel) + "> Exception", e);
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            //执行到这里 说明获取 信号量失败   说明 当前服务器并发比较高
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            } else {
                String info =
                    String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                        timeoutMillis,
                        this.semaphoreAsync.getQueueLength(),
                        this.semaphoreAsync.availablePermits()
                    );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
        //执行完后，异步没有任何返回
    }

    private void requestFail(final int opaque) {
        ResponseFuture responseFuture = responseTable.remove(opaque);
        if (responseFuture != null) {
            responseFuture.setSendRequestOK(false);
            responseFuture.putResponse(null);
            try {
                //调用 回调处理器对象，异步 使用CallbackExecutor，公共线程池。 publicExecutor
                executeInvokeCallback(responseFuture);
            } catch (Throwable e) {
                log.warn("execute callback in requestFail, and callback throw", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * mark the request of the specified channel as fail and to invoke fail callback immediately
     * @param channel the channel which is close already
     */
    protected void failFast(final Channel channel) {
        Iterator<Entry<Integer, ResponseFuture>> it = responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> entry = it.next();
            if (entry.getValue().getProcessChannel() == channel) {
                Integer opaque = entry.getKey();
                if (opaque != null) {
                    requestFail(opaque);
                }
            }
        }
    }

    /**
     * 单向的  不关注 结果。
     * @param channel
     * @param request
     * @param timeoutMillis
     * @throws InterruptedException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis)
        throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        //打了个标签   设置标签  对端 检查标记 就可以知道 这个请求 是个 单向请求
        request.markOnewayRPC();
        //申请信号量
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            //1.将数据 交给 ch，这里数据发送的逻辑 由 netty 线程完成
            //2. 添加 写 刷 数据操作的  监听器 ， 由 netty Id 线程 回调
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture f) throws Exception {
                        //执行完操作后，直接释放
                        once.release();
                        if (!f.isSuccess()) {
                            log.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                        }
                    }
                });
            } catch (Exception e) {
                once.release();
                log.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            } else {
                String info = String.format(
                    "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                    timeoutMillis,
                    this.semaphoreOneway.getQueueLength(),
                    this.semaphoreOneway.availablePermits()
                );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    class NettyEventExecutor extends ServiceThread {
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<NettyEvent>();
        private final int maxSize = 10000;

        public void putNettyEvent(final NettyEvent event) {
            if (this.eventQueue.size() <= maxSize) {
                this.eventQueue.add(event);
            } else {
                log.warn("event queue size[{}] enough, so drop this event {}", this.eventQueue.size(), event.toString());
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            while (!this.isStopped()) {
                try {
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return NettyEventExecutor.class.getSimpleName();
        }
    }
}
