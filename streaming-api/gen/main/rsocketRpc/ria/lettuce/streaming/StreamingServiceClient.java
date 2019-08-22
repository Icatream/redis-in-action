package ria.lettuce.streaming;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.opentracing.Tracer;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.rpc.annotations.internal.Generated;
import io.rsocket.rpc.annotations.internal.GeneratedMethod;
import io.rsocket.rpc.annotations.internal.ResourceType;
import io.rsocket.rpc.frames.Metadata;
import io.rsocket.rpc.metrics.Metrics;
import io.rsocket.rpc.tracing.Tag;
import io.rsocket.rpc.tracing.Tracing;
import io.rsocket.util.ByteBufPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@javax.annotation.Generated(
  value = "by RSocket RPC proto compiler",
  comments = "Source: ria/lettuce/streaming/streaming.proto")
@Generated(
  type = ResourceType.CLIENT,
  idlClass = StreamingService.class)
public final class StreamingServiceClient implements StreamingService {
    private final RSocket rSocket;
    private final Function<? super Publisher<Subscription>, ? extends Publisher<Subscription>> requestReply;
    private final Function<? super Publisher<Subscription>, ? extends Publisher<Subscription>> requestStream;
    private final Function<Map<String, String>, Function<? super Publisher<Subscription>, ? extends Publisher<Subscription>>> requestReplyTrace;
    private final Function<Map<String, String>, Function<? super Publisher<Subscription>, ? extends Publisher<Subscription>>> requestStreamTrace;

    public StreamingServiceClient(RSocket rSocket) {
        this.rSocket = rSocket;
        this.requestReply = Function.identity();
        this.requestStream = Function.identity();
        this.requestReplyTrace = Tracing.trace();
        this.requestStreamTrace = Tracing.trace();
    }

    public StreamingServiceClient(RSocket rSocket, MeterRegistry registry) {
        this.rSocket = rSocket;
        this.requestReply = Metrics.timed(registry, "rsocket.client", "service", StreamingService.SERVICE, "method", StreamingService.METHOD_REQUEST_REPLY);
        this.requestStream = Metrics.timed(registry, "rsocket.client", "service", StreamingService.SERVICE, "method", StreamingService.METHOD_REQUEST_STREAM);
        this.requestReplyTrace = Tracing.trace();
        this.requestStreamTrace = Tracing.trace();
    }

    public StreamingServiceClient(RSocket rSocket, Tracer tracer) {
        this.rSocket = rSocket;
        this.requestReply = Function.identity();
        this.requestStream = Function.identity();
        this.requestReplyTrace = Tracing.trace(tracer, StreamingService.METHOD_REQUEST_REPLY, Tag.of("rsocket.service", StreamingService.SERVICE), Tag.of("rsocket.rpc.role", "client"), Tag.of("rsocket.rpc.version", ""));
        this.requestStreamTrace = Tracing.trace(tracer, StreamingService.METHOD_REQUEST_STREAM, Tag.of("rsocket.service", StreamingService.SERVICE), Tag.of("rsocket.rpc.role", "client"), Tag.of("rsocket.rpc.version", ""));
    }

    public StreamingServiceClient(RSocket rSocket, MeterRegistry registry, Tracer tracer) {
        this.rSocket = rSocket;
        this.requestReply = Metrics.timed(registry, "rsocket.client", "service", StreamingService.SERVICE, "method", StreamingService.METHOD_REQUEST_REPLY);
        this.requestStream = Metrics.timed(registry, "rsocket.client", "service", StreamingService.SERVICE, "method", StreamingService.METHOD_REQUEST_STREAM);
        this.requestReplyTrace = Tracing.trace(tracer, StreamingService.METHOD_REQUEST_REPLY, Tag.of("rsocket.service", StreamingService.SERVICE), Tag.of("rsocket.rpc.role", "client"), Tag.of("rsocket.rpc.version", ""));
        this.requestStreamTrace = Tracing.trace(tracer, StreamingService.METHOD_REQUEST_STREAM, Tag.of("rsocket.service", StreamingService.SERVICE), Tag.of("rsocket.rpc.role", "client"), Tag.of("rsocket.rpc.version", ""));
    }

    @GeneratedMethod(returnTypeClass = Subscription.class)
    public Mono<Subscription> requestReply(ApiReq message) {
        return requestReply(message, Unpooled.EMPTY_BUFFER);
    }

    @Override
    @GeneratedMethod(returnTypeClass = Subscription.class)
    public Mono<Subscription> requestReply(ApiReq message, ByteBuf metadata) {
        Map<String, String> map = new HashMap<>();
        return Mono.defer(
          () -> {
              final ByteBuf data = serialize(message);
              final ByteBuf tracing = Tracing.mapToByteBuf(ByteBufAllocator.DEFAULT, map);
              final ByteBuf metadataBuf = Metadata.encode(ByteBufAllocator.DEFAULT, StreamingService.SERVICE, StreamingService.METHOD_REQUEST_REPLY, tracing, metadata);
              tracing.release();
              metadata.release();
              return rSocket.requestResponse(ByteBufPayload.create(data, metadataBuf));
          })
          .map(deserializer(Subscription.parser()))
          .transform(requestReply)
          .transform(requestReplyTrace.apply(map));
    }

    @GeneratedMethod(returnTypeClass = Subscription.class)
    public Flux<Subscription> requestStream(ApiReq message) {
        return requestStream(message, Unpooled.EMPTY_BUFFER);
    }

    @Override
    @GeneratedMethod(returnTypeClass = Subscription.class)
    public Flux<Subscription> requestStream(ApiReq message, ByteBuf metadata) {
        Map<String, String> map = new HashMap<>();
        return Flux.defer(
          () -> {
              final ByteBuf data = serialize(message);
              final ByteBuf tracing = Tracing.mapToByteBuf(ByteBufAllocator.DEFAULT, map);
              final ByteBuf metadataBuf = Metadata.encode(ByteBufAllocator.DEFAULT, StreamingService.SERVICE, StreamingService.METHOD_REQUEST_STREAM, tracing, metadata);
              tracing.release();
              metadata.release();
              return rSocket.requestStream(ByteBufPayload.create(data, metadataBuf));
          })
          .map(deserializer(Subscription.parser()))
          .transform(requestStream)
          .transform(requestStreamTrace.apply(map));
    }

    private static ByteBuf serialize(final MessageLite message) {
        int length = message.getSerializedSize();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(length);
        try {
            message.writeTo(CodedOutputStream.newInstance(byteBuf.internalNioBuffer(0, length)));
            byteBuf.writerIndex(length);
            return byteBuf;
        } catch (Throwable t) {
            byteBuf.release();
            throw new RuntimeException(t);
        }
    }

    private static <T> Function<Payload, T> deserializer(final Parser<T> parser) {
        return payload -> {
            try {
                CodedInputStream is = CodedInputStream.newInstance(payload.getData());
                return parser.parseFrom(is);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            } finally {
                payload.release();
            }
        };
    }
}
