package lettuce;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lettuce.chapter.Chapter05;

/**
 * @author YaoXunYu
 * created on 04/17/2019
 */
public class Main {

    public static void main(String[] args) {
        RedisClient client = RedisClient.create(RedisURI.create("localhost", 6379));
        StatefulRedisConnection<String, String> conn = client.connect();
        RedisReactiveCommands<String, String> commands = conn.reactive();
        Chapter05 c = new Chapter05(commands);
        /*c.updateCounter("test", 2)
            .doOnNext(System.out::println)
            .block();*/
        System.out.println(System.currentTimeMillis());
        c.updateStats("c1", "info", 5)
            .doOnNext(l -> {
                l.forEach(i-> System.out.println(i));
            })
            .block();
    }
}
