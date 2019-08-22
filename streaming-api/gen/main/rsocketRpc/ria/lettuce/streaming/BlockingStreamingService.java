package ria.lettuce.streaming;

import io.netty.buffer.ByteBuf;

import javax.annotation.Generated;

/**
 *
 */
@Generated(value = "by RSocket RPC proto compiler (version 0.2.18)",
  comments = "Source: ria/lettuce/streaming/streaming.proto")
public interface BlockingStreamingService {
    String SERVICE_ID = "ria.lettuce.streaming.StreamingService";
    String METHOD_REQUEST_REPLY = "RequestReply";
    String METHOD_REQUEST_STREAM = "RequestStream";

    /**
     *
     */
    Subscription requestReply(ApiReq message, ByteBuf metadata);

    /**
     *
     */
    Iterable<Subscription> requestStream(ApiReq message, ByteBuf metadata);
}
