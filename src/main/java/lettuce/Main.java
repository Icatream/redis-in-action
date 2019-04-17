package lettuce;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;

/**
 * @author YaoXunYu
 * created on 04/17/2019
 */
public class Main {

    public static void main(String[] args) {
        RedisClient client = RedisClient.create(RedisURI.create("localhost", 6379));
        StatefulRedisConnection<String, String> conn = client.connect();
        RedisReactiveCommands<String, String> commands = conn.reactive();

    }
}
