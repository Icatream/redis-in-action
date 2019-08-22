package ria.lettuce.streaming.simple;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/**
 * @author YaoXunYu
 * created on 05/31/19
 */
public class Client {
    private final Mono<RSocket> reference;

    public Client() {
        this.reference = RSocketFactory.connect()
          .transport(TcpClientTransport.create("localhost", 8080))
          .start()
          .cache();
    }

    public Mono<String> call(String string) {
        return reference
          .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create(string)))
          .map(Payload::getDataUtf8);
    }

    public void dispose() {
        this.reference
          .doOnNext(Disposable::dispose)
          .block();
    }
}
