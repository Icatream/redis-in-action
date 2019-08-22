package ria.lettuce.streaming;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import io.micrometer.core.instrument.MeterRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import io.rsocket.rpc.AbstractRSocketService;
import io.rsocket.rpc.annotations.internal.Generated;
import io.rsocket.rpc.annotations.internal.ResourceType;
import io.rsocket.rpc.frames.Metadata;
import io.rsocket.rpc.metrics.Metrics;
import io.rsocket.util.ByteBufPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Optional;
import java.util.function.Function;

@javax.annotation.Generated(
  value = "by RSocket RPC proto compiler (version 0.2.18)",
  comments = "Source: ria/lettuce/streaming/streaming.proto")
@Generated(
  type = ResourceType.SERVICE,
  idlClass = BlockingStreamingService.class)
@Named(value = "BlockingStreamingServiceServer")
public final class BlockingStreamingServiceServer extends AbstractRSocketService {
    private final BlockingStreamingService service;
    private final Scheduler scheduler;
    private final Function<? super Publisher<Payload>, ? extends Publisher<Payload>> requestReply;
    private final Function<? super Publisher<Payload>, ? extends Publisher<Payload>> requestStream;

    @Inject
    public BlockingStreamingServiceServer(BlockingStreamingService service, Optional<Scheduler> scheduler, Optional<MeterRegistry> registry) {
        this.scheduler = scheduler.orElse(Schedulers.elastic());
        this.service = service;
        if (!registry.isPresent()) {
            this.requestReply = Function.identity();
            this.requestStream = Function.identity();
        } else {
            this.requestReply = Metrics.timed(registry.get(), "rsocket.server", "service", BlockingStreamingService.SERVICE_ID, "method", BlockingStreamingService.METHOD_REQUEST_REPLY);
            this.requestStream = Metrics.timed(registry.get(), "rsocket.server", "service", BlockingStreamingService.SERVICE_ID, "method", BlockingStreamingService.METHOD_REQUEST_STREAM);
        }

    }

    @Override
    public String getService() {
        return BlockingStreamingService.SERVICE_ID;
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
            switch (Metadata.getMethod(metadata)) {
                case StreamingService.METHOD_REQUEST_REPLY: {
                    CodedInputStream is = CodedInputStream.newInstance(payload.getData());
                    ApiReq message = ApiReq.parseFrom(is);
                    return Mono.fromSupplier(() -> service.requestReply(message, metadata))
                      .map(serializer)
                      .transform(requestReply)
                      .subscribeOn(scheduler);
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
            switch (Metadata.getMethod(metadata)) {
                case BlockingStreamingService.METHOD_REQUEST_STREAM: {
                    CodedInputStream is = CodedInputStream.newInstance(payload.getData());
                    ApiReq message = ApiReq.parseFrom(is);
                    return Flux.defer(() -> Flux.fromIterable(service.requestStream(message, metadata))
                      .map(serializer)
                      .transform(requestStream))
                      .subscribeOn(scheduler);
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
