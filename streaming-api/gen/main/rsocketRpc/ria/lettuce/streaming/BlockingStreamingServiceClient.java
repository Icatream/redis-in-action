package ria.lettuce.streaming;

import io.micrometer.core.instrument.MeterRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.RSocket;
import io.rsocket.rpc.BlockingIterable;
import io.rsocket.rpc.annotations.internal.Generated;
import io.rsocket.rpc.annotations.internal.GeneratedMethod;
import io.rsocket.rpc.annotations.internal.ResourceType;
import reactor.core.publisher.Flux;
import reactor.util.concurrent.Queues;

@javax.annotation.Generated(
  value = "by RSocket RPC proto compiler (version 0.2.18)",
  comments = "Source: ria/lettuce/streaming/streaming.proto")
@Generated(
  type = ResourceType.CLIENT,
  idlClass = BlockingStreamingService.class)
public final class BlockingStreamingServiceClient implements BlockingStreamingService {
    private final StreamingServiceClient delegate;

    public BlockingStreamingServiceClient(RSocket rSocket) {
        this.delegate = new StreamingServiceClient(rSocket);
    }

    public BlockingStreamingServiceClient(RSocket rSocket, MeterRegistry registry) {
        this.delegate = new StreamingServiceClient(rSocket, registry);
    }

    @GeneratedMethod(returnTypeClass = Subscription.class)
    public Subscription requestReply(ApiReq message) {
        return requestReply(message, Unpooled.EMPTY_BUFFER);
    }

    @Override
    @GeneratedMethod(returnTypeClass = Subscription.class)
    public Subscription requestReply(ApiReq message, ByteBuf metadata) {
        return delegate.requestReply(message, metadata).block();
    }

    @GeneratedMethod(returnTypeClass = Subscription.class)
    public BlockingIterable<Subscription> requestStream(ApiReq message) {
        return requestStream(message, Unpooled.EMPTY_BUFFER);
    }

    @Override
    @GeneratedMethod(returnTypeClass = Subscription.class)
    public BlockingIterable<Subscription> requestStream(ApiReq message, ByteBuf metadata) {
        Flux stream = delegate.requestStream(message, metadata);
        return new BlockingIterable<>(stream, Queues.SMALL_BUFFER_SIZE, Queues.small());
    }

}

