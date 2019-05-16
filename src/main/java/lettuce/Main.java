package lettuce;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lettuce.chapter.Chapter06;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * @author YaoXunYu
 * created on 04/17/2019
 */
public class Main {

    public static void main(String[] args) {
        RedisClient client = RedisClient.create(RedisURI.create("localhost", 6379));
        StatefulRedisConnection<String, String> conn = client.connect();
        RedisReactiveCommands<String, String> commands = conn.reactive();
        Chapter06 c = new Chapter06(commands);
    }

    private static void readLineTest(RedisReactiveCommands<String, String> commands, Chapter06 c) {
        //commands.append("k", "abcde\n\nfg\nhij\nklmn\nopqrst\nuvwxyz\n").block();

        long size = 5;
        AtomicLong l = new AtomicLong();
        Mono.fromSupplier(() -> l.getAndAccumulate(size, (pv, v) -> pv + v))
            .flatMap(pos -> commands.getrange("k", pos, pos + size - 1))
            .repeat()
            .takeWhile(s -> !"".equals(s))
            .doOnNext(s -> System.out.println("Get from redis: " + s))
            .scan(Tuples.of(Stream.empty(), ""), c.accumulator)
            .doOnNext(tuple -> System.out.println("Tuple(2): " + tuple.getT2()))
            .flatMap(tuple -> Flux.fromStream(tuple.getT1()))
            .doOnNext(s -> System.out.println("After: " + s))
            .blockLast();
    }
}
