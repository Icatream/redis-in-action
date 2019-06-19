package ria.lettuce.streaming;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.publisher.Mono;

/**
 * @author YaoXunYu
 * created on 06/04/19
 */
public class T {

    private static StreamingAPIServer server;
    private static StreamingAPIClient client;

    @BeforeClass
    public static void setUp() {
        //server = new StreamingAPIServer();
        client = new StreamingAPIClient();
    }

    @AfterClass
    public static void tearDown() {
        //server.dispose();
        client.dispose();
    }

    @Test
    public void fireAndForget() {
        client.fireAndForget("/pong", RestfulMetadataRouter.HttpMethod.GET, "name=Apple&sex=female&age=16")
          .then(Mono.just(1)
            .doOnNext(System.out::println))
          .block();
    }

    @Test
    public void requestResponse() {
        client.requestResponse("/pong", RestfulMetadataRouter.HttpMethod.GET, "name=Apple&sex=female&age=16")
          .doOnNext(payload -> {
              System.out.println(payload.getDataUtf8());
          })
          .block();
    }

    @Test
    public void metadataPush() {
        client.metadataPush("/pong", RestfulMetadataRouter.HttpMethod.GET, "name=Apple&sex=female&age=16")
          .then(Mono.just(1)
            .doOnNext(System.out::println))
          .block();
    }

}
