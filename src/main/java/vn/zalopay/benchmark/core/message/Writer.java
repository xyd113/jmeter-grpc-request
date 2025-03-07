package vn.zalopay.benchmark.core.message;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.zalopay.benchmark.core.specification.GrpcResponse;

public class Writer<T extends Message> implements StreamObserver<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Writer.class);

    private final JsonFormat.Printer jsonPrinter;
    private final GrpcResponse grpcResponse;

    Writer(JsonFormat.Printer jsonPrinter, GrpcResponse grpcResponse) {
        this.jsonPrinter = jsonPrinter.preservingProtoFieldNames().includingDefaultValueFields();
        this.grpcResponse = grpcResponse;
    }

    /** Creates a new Writer which writes the messages it sees to the supplied Output. */
    public static <T extends Message> Writer<T> create(
            GrpcResponse grpcResponse, JsonFormat.TypeRegistry registry) {
        return new Writer<>(JsonFormat.printer().usingTypeRegistry(registry), grpcResponse);
    }

    @Override
    public void onCompleted() {
        LOGGER.info("Writer On completed gRPC message: {}", grpcResponse.getGrpcMessageString());
    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.error("Writer onError:", throwable);
        grpcResponse.setSuccess(false);
        grpcResponse.setThrowable(throwable);
    }

    @Override
    public void onNext(T message) {
        try {
            LOGGER.info("Writer onNext success: {}", jsonPrinter.print(message));
            grpcResponse.setSuccess(true);
            grpcResponse.storeGrpcMessage(jsonPrinter.print(message));
        } catch (InvalidProtocolBufferException e) {
            LOGGER.warn(e.getMessage());
            LOGGER.error("Writer onNext catch:", e);
            grpcResponse.storeGrpcMessage(message.toString());
        }
    }
}
