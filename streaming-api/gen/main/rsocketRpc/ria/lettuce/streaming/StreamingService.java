package ria.lettuce.streaming;

import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.Generated;

/**
 *
 */
@Generated(
  value = "by RSocket RPC proto compiler",
  comments = "Source: ria/lettuce/streaming/streaming.proto")
public interface StreamingService {
    String SERVICE = "ria.lettuce.streaming.StreamingService";
    String METHOD_REQUEST_REPLY = "RequestReply";
    String METHOD_REQUEST_STREAM = "RequestStream";

    /**
     *
     */
    Mono<Subscription> requestReply(ApiReq message, ByteBuf metadata);

    /**
     *
     */
    Flux<Subscription> requestStream(ApiReq message, ByteBuf metadata);
}
