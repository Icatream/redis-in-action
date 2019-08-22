package ria.lettuce.streaming.simple;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * @author YaoXunYu
 * created on 05/30/19
 */
public class Server {

    private final Disposable server;

    public Server() {
        this.server = RSocketFactory.receive()
          .acceptor((setupPayload, reactiveSocket) -> Mono.just(new RSocketImpl()))
          .transport(TcpServerTransport.create("localhost", 8080))
          .start()
          .subscribe();
    }

    public void dispose() {
        this.server.dispose();
    }

    private static class RSocketImpl extends AbstractRSocket {

        @Override
        public Mono<Void> fireAndForget(Payload payload) {
            System.out.println("Metadata: " + payload.getMetadataUtf8());
            System.out.println("Server received: " + payload.getDataUtf8());
            return Mono.empty();
        }

        @Override
        public Mono<Payload> requestResponse(Payload payload) {
            System.out.println("Metadata: " + payload.getMetadataUtf8());
            System.out.println("Server received: " + payload.getDataUtf8());
            return Mono.just(DefaultPayload.create("pong"));
        }

        @Override
        public Flux<Payload> requestStream(Payload payload) {
            System.out.println("Server received: " + payload.getDataUtf8());
            return Flux.range(10000, 5)
              .delayElements(Duration.ofSeconds(1))
              .map(i -> DefaultPayload.create(i.toString()));
        }

        @Override
        public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
            return Flux.from(payloads)
              .map(payload -> {
                  ByteBuf source = payload.data();
                  ByteBuf reverse = ByteBufAllocator.DEFAULT.buffer(source.capacity());
                  int i = source.readableBytes();
                  while (i > 0) {
                      reverse.writeByte(source.getByte(--i));
                  }
                  payload.release();
                  return reverse;
              })
              .map(DefaultPayload::create);
        }

        @Override
        public Mono<Void> metadataPush(Payload payload) {
            System.out.println("Metadata: " + payload.getMetadataUtf8());
            System.out.println("Server received: " + payload.getDataUtf8());

            //this return means nothing, do something before this line
            //@see RSocketServer line-291
            return Mono.empty();
        }
    }
}
