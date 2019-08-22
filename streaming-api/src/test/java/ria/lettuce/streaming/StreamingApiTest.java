package ria.lettuce.streaming;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * @author YaoXunYu
 * created on 08/22/19
 */
public class StreamingApiTest {

    private static final int PORT = 8001;
    private static final String CHANNEL = "C1";
    private static DefaultStreamingService service;
    private static StreamingServiceServer server;

    @BeforeClass
    public static void setUp() {
        RedisClient client = RedisClient.create(RedisURI.create("localhost", 6379));
        service = new DefaultStreamingService(client);
        server = new StreamingServiceServer(service, Optional.empty(), Optional.empty());
        service.subscribe(CHANNEL)
          .then(RSocketFactory.receive()
            .acceptor((setup, sendingSocket) -> Mono.just(server))
            .transport(TcpServerTransport.create(PORT))
            .start())
          .block();
    }

    @AfterClass
    public static void tearDown() {
        service.close();
    }

    @Test
    public void test0() throws InterruptedException {
        StreamingServiceClient c1 = client();
        StreamingServiceClient c2 = client();
        c1.requestReply(ApiReq.newBuilder()
          .setIdentification("sample1")
          .setId(2)
          .setFilter(ApiReq.Filter.SAMPLE)
          .build())
          .doOnNext(sub -> resPrinter.accept("client1", sub))
          .subscribe();

        c2.requestStream(ApiReq.newBuilder()
          .setIdentification("location1")
          .setId(16)
          .setFilter(ApiReq.Filter.LOCATION)
          .build())
          .doOnNext(sub -> resPrinter.accept("client2", sub))
          .subscribe();

        TimeUnit.SECONDS.sleep(15);
    }

    private static StreamingServiceClient client() {
        return RSocketFactory.connect()
          .transport(TcpClientTransport.create(PORT))
          .start()
          .map(StreamingServiceClient::new)
          .block();
    }

    private static BiConsumer<String, Subscription> resPrinter = (name, sub) -> {
        System.out.println(name + ":channel:" + sub.getChannel());
        System.out.println(name + ":message:" + sub.getMessage());
    };
}
