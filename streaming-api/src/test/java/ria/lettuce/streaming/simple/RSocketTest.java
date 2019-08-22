package ria.lettuce.streaming.simple;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;

/**
 * @author YaoXunYu
 * created on 05/31/19
 */
public class RSocketTest {

    private static Server server;
    private static RSocket rSocket;

    @BeforeClass
    public static void setUpClass() {
        server = new Server();
        rSocket = RSocketFactory.connect()
          .transport(TcpClientTransport.create("localhost", 8080))
          .start()
          .block();
    }

    @AfterClass
    public static void tearDownClass() {
        server.dispose();
    }

    @Test
    public void ping() {
        rSocket.requestResponse(DefaultPayload.create("ping", "meta"))
          .doOnNext(payload -> System.out.println(payload.getDataUtf8()))
          .block();
    }

    @Test
    public void fireAndForget() {
        Flux.range(0, 5)
          .delayElements(Duration.ofSeconds(1))
          .doOnNext(i -> System.out.println("FireAndForget: " + i))
          .flatMap(i -> rSocket.fireAndForget(DefaultPayload.create(i.toString(), "empty meta")))
          .blockLast();
    }

    @Test
    public void stream() {
        rSocket.requestStream(DefaultPayload.create("ping"))
          .map(Payload::getDataUtf8)
          .doOnNext(System.out::println)
          .blockLast();
    }

    @Test
    public void channel() {
        rSocket.requestChannel(Flux.just("abcdefg", "hijklmn")
          .map(DefaultPayload::create))
          .map(Payload::getDataUtf8)
          .doOnNext(System.out::println)
          .blockLast();
    }

    @Test
    public void metadataPush() {
        Flux.range(0, 5)
          .delayElements(Duration.ofSeconds(1))
          .doOnNext(i -> System.out.println("MetadataPush: " + i))
          .flatMap(i -> rSocket.metadataPush(DefaultPayload.create(i.toString(), "meta")))
          .blockLast();
    }
}
