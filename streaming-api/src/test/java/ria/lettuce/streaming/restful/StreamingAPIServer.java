package ria.lettuce.streaming.restful;

import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/**
 * @author YaoXunYu
 * created on 06/03/19
 */
public class StreamingAPIServer {

    private static final Logger logger = LoggerFactory.getLogger(StreamingAPIServer.class);
    private final Disposable server;

    public StreamingAPIServer() {
        this.server = RSocketFactory.receive()
          .errorConsumer(throwable -> logger.error("Streaming API Server Error", throwable))
          .acceptor((setupPayload, reactiveSocket) -> Mono.just(new RSocketImpl()))
          .transport(TcpServerTransport.create("localhost", 8080))
          .start()
          .subscribe();
    }

    public void dispose() {
        this.server.dispose();
    }

    public static void main(String[] args) throws InterruptedException {
        RSocketFactory.receive()
          .errorConsumer(throwable -> logger.error("Bad request", throwable))
          .acceptor((setupPayload, reactiveSocket) -> Mono.just(new RSocketImpl()))
          .transport(TcpServerTransport.create("localhost", 8080))
          .start()
          .block();
        //Thread.currentThread().join();
    }
}
