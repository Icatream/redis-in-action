package ria.lettuce.streaming;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

/**
 * @author YaoXunYu
 * created on 06/04/19
 */
public class StreamingAPIClient {
    private final Mono<RSocket> client = RSocketFactory.connect()
      .transport(TcpClientTransport.create("localhost", 8080))
      .start()
      .cache();

    public Mono<Void> fireAndForget(String path, RestfulMetadataRouter.HttpMethod method, Object data) {
        return client.flatMap(rSocket -> rSocket.fireAndForget(createPayload(path, method, data)));
    }

    public Mono<Payload> requestResponse(String path, RestfulMetadataRouter.HttpMethod method, Object data) {
        return client.flatMap(rSocket -> rSocket.requestResponse(createPayload(path, method, data)));
    }

    public Flux<Payload> requestStream(String path, RestfulMetadataRouter.HttpMethod method, Object data) {
        return client.flatMapMany(rSocket -> rSocket.requestStream(createPayload(path, method, data)));
    }

    public Flux<Payload> requestChannel(String path, RestfulMetadataRouter.HttpMethod method, Publisher<?> dataPublisher) {
        return Flux.from(dataPublisher)
          .map(data -> createPayload(path, method, data))
          .transform(payloadFlux -> client.flatMapMany(rSocket -> rSocket.requestChannel(payloadFlux)));
    }

    public Mono<Void> metadataPush(String path, RestfulMetadataRouter.HttpMethod method, Object data) {
        return client.flatMap(rSocket -> rSocket.metadataPush(createPayload(path, method, data)));
    }

    public void dispose(){
        client.doOnNext(Disposable::dispose)
          .block();
    }

    private Payload createPayload(String path, RestfulMetadataRouter.HttpMethod method, Object data) {
        return DefaultPayload.create(fakeDataWriter(data), createMetadata(path, method));
    }

    private ByteBuf createMetadata(String path, RestfulMetadataRouter.HttpMethod method) {
        return ByteBufAllocator.DEFAULT.buffer()
          .writeByte(method.flag)
          .writeBytes(path.getBytes(StandardCharsets.UTF_8));
    }

    private ByteBuf fakeDataWriter(Object data) {
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
        if (data instanceof String) {
            String s = (String) data;
            return buf.writeBytes(s.getBytes(StandardCharsets.UTF_8));
        } else {
            return buf.writeBytes(String.valueOf(data).getBytes(StandardCharsets.UTF_8));
        }
    }
}
