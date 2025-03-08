package vn.zalopay.benchmark;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.testelement.ThreadListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.zalopay.benchmark.constant.GrpcSamplerConstant;
import vn.zalopay.benchmark.core.ClientCaller;
import vn.zalopay.benchmark.core.config.GrpcRequestConfig;
import vn.zalopay.benchmark.core.protobuf.ProtocInvoker;
import vn.zalopay.benchmark.core.specification.GrpcResponse;
import vn.zalopay.benchmark.util.ExceptionUtils;

import java.nio.charset.StandardCharsets;

public class GRPCSampler extends AbstractSampler implements ThreadListener, TestStateListener {

    private static final Logger log = LoggerFactory.getLogger(GRPCSampler.class);
    private static final long serialVersionUID = 232L;

    public static final String METADATA = "GRPCSampler.metadata";
    public static final String LIB_FOLDER = "GRPCSampler.libFolder";
    public static final String PROTO_FOLDER = "GRPCSampler.protoFolder";
    public static final String HOST = "GRPCSampler.host";
    public static final String PORT = "GRPCSampler.port";
    public static final String FULL_METHOD = "GRPCSampler.fullMethod";
    public static final String REQUEST_JSON = "GRPCSampler.requestJson";
    public static final String DEADLINE = "GRPCSampler.deadline";
    public static final String TLS = "GRPCSampler.tls";
    public static final String TLS_DISABLE_VERIFICATION = "GRPCSampler.tlsDisableVerification";
    public static final String CHANNEL_SHUTDOWN_AWAIT_TIME = "GRPCSampler.channelAwaitTermination";
    public static final String CHANNEL_MAX_INBOUND_MESSAGE_SIZE =
            "GRPCSampler" + ".maxInboundMessageSize";
    public static final String CHANNEL_MAX_INBOUND_METADATA_SIZE =
            "GRPCSampler.maxInboundMetadataSize";
    private transient ClientCaller clientCaller;
    private GrpcRequestConfig grpcRequestConfig;

    public GRPCSampler() {
        super();
        trace("init GRPCSampler");
    }

    /**
     * @return a string for the sampleResult Title
     */
    private String getTitle() {
        return this.getName();
    }

    private void trace(String s) {
        String threadName = Thread.currentThread().getName();
        log.debug("{} ({}) {} {}", threadName, getTitle(), s, this);
    }

    private void initGrpcConfigRequest() {
        if (grpcRequestConfig == null) {
            log.info("初始化gRPC配置 - 主机: {}, 端口: {}, 方法: {}, TLS: {}", 
                    getHost(), getPort(), getFullMethod(), isTls());
            grpcRequestConfig =
                    GrpcRequestConfig.builder()
                            .hostPort(getHostPort())
                            .protoFolder(getProtoFolder())
                            .libFolder(getLibFolder())
                            .fullMethod(getFullMethod())
                            .tls(isTls())
                            .tlsDisableVerification(isTlsDisableVerification())
                            .awaitTerminationTimeout(getChannelShutdownAwaitTime())
                            .maxInboundMessageSize(getChannelMaxInboundMessageSize())
                            .maxInboundMetadataSize(getChannelMaxInboundMetadataSize())
                            .build();
            log.debug("gRPC配置详情 - 最大入站消息大小: {}, 最大入站元数据大小: {}, 等待终止超时: {}",
                    getChannelMaxInboundMessageSize(), 
                    getChannelMaxInboundMetadataSize(),
                    getChannelShutdownAwaitTime());
        }
    }

    private void initGrpcClient() {
        if (clientCaller == null) {
            log.info("创建新的gRPC客户端调用器");
            clientCaller = new ClientCaller(grpcRequestConfig);
        } else {
            log.debug("重用现有的gRPC客户端调用器");
        }
    }

    @Override
    public SampleResult sample(Entry ignored) {
        SampleResult sampleResult = new SampleResult();
        sampleResult.setSampleLabel(getName());

        if (!initGrpcRequestSampler(sampleResult)) {
            return sampleResult;
        }

        // Initiate a GRPC request
        processGrpcRequestSampler(sampleResult);

        return sampleResult;
    }

    @Override
    public void clear() {
        super.clear();
    }

    @Override
    public void threadStarted() {
        log.debug("\ttestStarted: {}", whoAmI());
    }

    @Override
    public void threadFinished() {
        log.debug("\ttestEnded: {}", whoAmI());
        if (clientCaller != null) {
            clientCaller.shutdownNettyChannel();
            clientCaller = null;
        }
        // clear state of grpc config for rerun with new config in GUI mode
        if (grpcRequestConfig != null) {
            grpcRequestConfig = null;
        }
    }

    private String whoAmI() {
        return Thread.currentThread().getName()
                + "@"
                + Integer.toHexString(hashCode())
                + "-"
                + getName();
    }

    private boolean initGrpcRequestSampler(SampleResult sampleResult) {
        try {
            initGrpcInCurrentThread(sampleResult);
        } catch (Exception e) {
            log.error(ExceptionUtils.getPrintExceptionToStr(e, null), "UTF-8");
            generateErrorResultInInitGRPCRequest(sampleResult, e);
            return false;
        }
        return true;
    }

    private void generateErrorResultInInitGRPCRequest(SampleResult sampleResult, Exception e) {
        sampleResult.setSuccessful(false);
        sampleResult.setResponseCode(" 400");
        sampleResult.setDataType(SampleResult.TEXT);
        sampleResult.setResponseMessage(GrpcSamplerConstant.CLIENT_EXCEPTION_MSG);
        sampleResult.setResponseData(ExceptionUtils.getPrintExceptionToStr(e, null), "UTF-8");
    }

    private void processGrpcRequestSampler(SampleResult sampleResult) {
        log.info("开始处理gRPC请求 - 线程: {}, 方法: {}, 主机: {}", 
                Thread.currentThread().getName(), getFullMethod(), getHostPort());
        try {
            log.info("准备发送gRPC请求 - 请求JSON: {}, 元数据: {}",
                    getRequestJson(), getMetadata());
            GrpcResponse grpcResponse = clientCaller.call(getDeadline());
            log.info("gRPC调用完成，响应状态: {}, 响应数据: {}",
                    grpcResponse.isSuccess(), 
                    grpcResponse.getGrpcMessageString());
            sampleResult.sampleEnd();
            sampleResult.setDataType(SampleResult.TEXT);
            if (grpcResponse.isSuccess() || (!grpcResponse.getGrpcMessageString().isEmpty() && !grpcResponse.getGrpcMessageString().equals("[]"))) {
                if(grpcResponse.isSuccess()){
                    log.info("gRPC调用结果grpcResponse.isSuccess()=true");
                }else{
                    log.info("gRPC调用结果grpcResponse.getGrpcMessageString()有有效的返回数据");
                }
                generateSuccessResult(grpcResponse, sampleResult);
            } else {
                generateErrorResult(grpcResponse, sampleResult);
            }
        } catch (Exception e) {
            log.error("gRPC请求处理过程中发生异常 - 异常类型: {}, 消息: {}", 
                    e.getClass().getName(), e.getMessage(), e);
            throw e;
        }
    }

    private void generateSuccessResult(GrpcResponse grpcResponse, SampleResult sampleResult) {
        log.debug("生成成功响应结果");
        sampleResult.setSuccessful(true);
        sampleResult.setResponseCodeOK();
        sampleResult.setResponseMessage(" success");
        sampleResult.setResponseData(
                grpcResponse.getGrpcMessageString().getBytes(StandardCharsets.UTF_8));
    }

    private void generateErrorResult(GrpcResponse grpcResponse, SampleResult sampleResult) {
        Throwable throwable = grpcResponse.getThrowable();
        log.error("生成错误响应结果 - 异常类型: {}, 异常消息: {}", 
                throwable.getClass().getName(), throwable.getMessage());
        if (throwable instanceof StatusRuntimeException) {
            Status status = ((StatusRuntimeException) throwable).getStatus();
            log.error("gRPC状态异常 - 状态码: {}, 描述: {}, 原因: {}", 
                    status.getCode(), status.getDescription(), status.getCause());
        }
        log.error("完整异常堆栈:", throwable);
        sampleResult.setSuccessful(false);
        sampleResult.setResponseCode(" 500");
        boolean isRuntimeException = throwable instanceof StatusRuntimeException;
        if (isRuntimeException) {
            generateStatusRuntimeExceptionResponseData(sampleResult, throwable);
        } else {
            generateExceptionInInvokeSendGrpcResponseData(sampleResult, throwable);
        }
    }

    private void generateStatusRuntimeExceptionResponseData(
            SampleResult sampleResult, Throwable throwable) {
        String responseMessage = " ";
        String responseData = "";
        Status status = ((StatusRuntimeException) throwable).getStatus();
        Status.Code code = status.getCode();
        responseMessage += code.value() + " " + code.name();
        responseData = status.getDescription();
        if (responseData == null) {
            responseData = responseMessage;
            log.warn("Status description为null，使用responseMessage作为响应数据");
        }
        log.debug("StatusRuntimeException响应 - 状态码: {}, 描述: {}", code, responseData);
        sampleResult.setResponseMessage(responseMessage);
        sampleResult.setResponseData(responseData, "UTF-8");
    }

    private void generateExceptionInInvokeSendGrpcResponseData(
            SampleResult sampleResult, Throwable throwable) {
        String responseMessage = " ";
        responseMessage += ExceptionUtils.getPrintExceptionToStr(throwable, 0);
        log.error("非StatusRuntimeException异常 - 异常类型: {}, 消息: {}", 
                 throwable.getClass().getName(), responseMessage);
        sampleResult.setResponseMessage(responseMessage);
        sampleResult.setResponseData(responseMessage, "UTF-8");
    }

    private void initGrpcInCurrentThread(SampleResult sampleResult) {
        log.info("初始化当前线程的gRPC配置 - 线程: {}", Thread.currentThread().getName());
        try {
            initGrpcConfigRequest();
            initGrpcClient();
            String grpcRequest = clientCaller.buildRequestAndMetadata(getRequestJson(), getMetadata());
            log.debug("gRPC请求构建完成 - 方法: {}, 元数据: {}", getFullMethod(), getMetadata());
            sampleResult.setSamplerData(grpcRequest);
            sampleResult.setRequestHeaders(clientCaller.getMetadataString());
            sampleResult.sampleStart();
        } catch (Exception e) {
            log.error("初始化gRPC配置失败", e);
            throw e;
        }
    }

    /** GETTER AND SETTER */
    public String getMetadata() {
        return getPropertyAsString(METADATA);
    }

    public void setMetadata(String metadata) {
        setProperty(METADATA, metadata);
    }

    public String getLibFolder() {
        return getPropertyAsString(LIB_FOLDER);
    }

    public void setLibFolder(String libFolder) {
        setProperty(LIB_FOLDER, libFolder);
    }

    public String getProtoFolder() {
        return getPropertyAsString(PROTO_FOLDER);
    }

    public void setProtoFolder(String protoFolder) {
        setProperty(PROTO_FOLDER, protoFolder);
    }

    public String getFullMethod() {
        return getPropertyAsString(FULL_METHOD);
    }

    public void setFullMethod(String fullMethod) {
        setProperty(FULL_METHOD, fullMethod);
    }

    public String getRequestJson() {
        return getPropertyAsString(REQUEST_JSON);
    }

    public void setRequestJson(String requestJson) {
        setProperty(REQUEST_JSON, requestJson);
    }

    public String getDeadline() {
        return getPropertyAsString(DEADLINE);
    }

    public void setDeadline(String deadline) {
        setProperty(DEADLINE, deadline);
    }

    public boolean isTls() {
        return getPropertyAsBoolean(TLS);
    }

    public void setTls(boolean tls) {
        setProperty(TLS, tls);
    }

    public boolean isTlsDisableVerification() {
        return getPropertyAsBoolean(TLS_DISABLE_VERIFICATION);
    }

    public void setTlsDisableVerification(boolean tlsDisableVerification) {
        setProperty(TLS_DISABLE_VERIFICATION, tlsDisableVerification);
    }

    public String getHost() {
        return getPropertyAsString(HOST);
    }

    public void setHost(String host) {
        setProperty(HOST, host);
    }

    public String getPort() {
        return getPropertyAsString(PORT);
    }

    public void setPort(String port) {
        setProperty(PORT, port);
    }

    public int getChannelShutdownAwaitTime() {
        return getPropertyAsInt(CHANNEL_SHUTDOWN_AWAIT_TIME, 5000);
    }

    public void setChannelShutdownAwaitTime(String awaitShutdownTime) {
        setProperty(CHANNEL_SHUTDOWN_AWAIT_TIME, awaitShutdownTime);
    }

    public int getChannelMaxInboundMessageSize() {
        return getPropertyAsInt(CHANNEL_MAX_INBOUND_MESSAGE_SIZE, 4194304);
    }

    public void setChannelMaxInboundMessageSize(String channelMaxInboundMessageSize) {
        setProperty(CHANNEL_MAX_INBOUND_MESSAGE_SIZE, channelMaxInboundMessageSize);
    }

    public int getChannelMaxInboundMetadataSize() {
        return getPropertyAsInt(CHANNEL_MAX_INBOUND_METADATA_SIZE, 8192);
    }

    public void setChannelMaxInboundMetadataSize(String channelMaxInboundMetadataSize) {
        setProperty(CHANNEL_MAX_INBOUND_METADATA_SIZE, channelMaxInboundMetadataSize);
    }

    private String getHostPort() {
        return getHost() + ":" + getPort();
    }

    @Override
    public void testStarted() {
        log.info("testStarted");
    }

    @Override
    public void testStarted(String s) {
        log.info("testStarted {}", s);
    }

    @Override
    public void testEnded() {
        log.info("testEnded");
        ProtocInvoker.cleanTempFolderForGeneratingProtoc();
    }

    @Override
    public void testEnded(String s) {
        log.info("testEnded {}", s);
        ProtocInvoker.cleanTempFolderForGeneratingProtoc();
    }
}
