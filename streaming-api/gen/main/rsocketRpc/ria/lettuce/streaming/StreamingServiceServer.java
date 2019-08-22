package ria.lettuce.streaming;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.rsocket.Payload;
import io.rsocket.rpc.AbstractRSocketService;
import io.rsocket.rpc.annotations.internal.Generated;
import io.rsocket.rpc.annotations.internal.ResourceType;
import io.rsocket.rpc.frames.Metadata;
import io.rsocket.rpc.metrics.Metrics;
import io.rsocket.rpc.tracing.Tag;
import io.rsocket.rpc.tracing.Tracing;
import io.rsocket.util.ByteBufPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Optional;
import java.util.function.Function;

@javax.annotation.Generated(
  value = "by RSocket RPC proto compiler",
  comments = "Source: ria/lettuce/streaming/streaming.proto")
@Generated(type = ResourceType.SERVICE,
  idlClass = StreamingService.class)
@Named(value = "StreamingServiceServer")
public final class StreamingServiceServer extends AbstractRSocketService {
    private final StreamingService service;
    private final Tracer tracer;
    private final Function<? super Publisher<Payload>, ? extends Publisher<Payload>> requestReply;
    private final Function<? super Publisher<Payload>, ? extends Publisher<Payload>> requestStream;
    private final Function<SpanContext, Function<? super Publisher<Payload>, ? extends Publisher<Payload>>> requestReplyTrace;
    private final Function<SpanContext, Function<? super Publisher<Payload>, ? extends Publisher<Payload>>> requestStreamTrace;

    @Inject
    public StreamingServiceServer(StreamingService service, Optional<MeterRegistry> registry, Optional<Tracer> tracer) {
        this.service = service;
        if (!registry.isPresent()) {
            this.requestReply = Function.identity();
            this.requestStream = Function.identity();
        } else {
            this.requestReply = Metrics.timed(registry.get(), "rsocket.server", "service", StreamingService.SERVICE, "method", StreamingService.METHOD_REQUEST_REPLY);
            this.requestStream = Metrics.timed(registry.get(), "rsocket.server", "service", StreamingService.SERVICE, "method", StreamingService.METHOD_REQUEST_STREAM);
        }

        if (!tracer.isPresent()) {
            this.tracer = null;
            this.requestReplyTrace = (ignored) -> Function.identity();
            this.requestStreamTrace = (ignored) -> Function.identity();
        } else {
            this.tracer = tracer.get();
            this.requestReplyTrace = Tracing.traceAsChild(this.tracer, StreamingService.METHOD_REQUEST_REPLY, Tag.of("rsocket.service", StreamingService.SERVICE), Tag.of("rsocket.rpc.role", "server"), Tag.of("rsocket.rpc.version", ""));
            this.requestStreamTrace = Tracing.traceAsChild(this.tracer, StreamingService.METHOD_REQUEST_STREAM, Tag.of("rsocket.service", StreamingService.SERVICE), Tag.of("rsocket.rpc.role", "server"), Tag.of("rsocket.rpc.version", ""));
        }

    }

    @Override
    public String getService() {
        return StreamingService.SERVICE;
    }

    @Override
    public Class<?> getServiceClass() {
        return service.getClass();
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        return Mono.error(new UnsupportedOperationException("Fire and forget not implemented."));
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        try {
            ByteBuf metadata = payload.sliceMetadata();
            SpanContext spanContext = Tracing.deserializeTracingMetadata(tracer, metadata);
            switch (Metadata.getMethod(metadata)) {
                case StreamingService.METHOD_REQUEST_REPLY: {
                    CodedInputStream is = CodedInputStream.newInstance(payload.getData());
                    return service.requestReply(ApiReq.parseFrom(is), metadata)
                      .map(serializer)
                      .transform(requestReply)
                      .transform(requestReplyTrace.apply(spanContext));
                }
                default: {
                    return Mono.error(new UnsupportedOperationException());
                }
            }
        } catch (Throwable t) {
            return Mono.error(t);
        } finally {
            payload.release();
        }
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        try {
            ByteBuf metadata = payload.sliceMetadata();
            SpanContext spanContext = Tracing.deserializeTracingMetadata(tracer, metadata);
            switch (Metadata.getMethod(metadata)) {
                case StreamingService.METHOD_REQUEST_STREAM: {
                    CodedInputStream is = CodedInputStream.newInstance(payload.getData());
                    return service.requestStream(ApiReq.parseFrom(is), metadata)
                      .map(serializer)
                      .transform(requestStream)
                      .transform(requestStreamTrace.apply(spanContext));
                }
                default: {
                    return Flux.error(new UnsupportedOperationException());
                }
            }
        } catch (Throwable t) {
            return Flux.error(t);
        } finally {
            payload.release();
        }
    }

    @Override
    public Flux<Payload> requestChannel(Payload payload, Flux<Payload> publisher) {
        return Flux.error(new UnsupportedOperationException("Request-Channel not implemented."));
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return Flux.error(new UnsupportedOperationException("Request-Channel not implemented."));
    }

    private static final Function<MessageLite, Payload> serializer = message -> {
        int length = message.getSerializedSize();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer(length);
        try {
            message.writeTo(CodedOutputStream.newInstance(byteBuf.internalNioBuffer(0, length)));
            byteBuf.writerIndex(length);
            return ByteBufPayload.create(byteBuf);
        } catch (Throwable t) {
            byteBuf.release();
            throw new RuntimeException(t);
        }
    };

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
