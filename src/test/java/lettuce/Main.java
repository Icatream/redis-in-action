package lettuce;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.codec.CompressionCodec;
import io.lettuce.core.codec.StringCodec;
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
        //RedisReactiveCommands<String, String> c = client.connect().reactive();
        //readLineTest(commands);
        RedisReactiveCommands<String, String> c = client.connect(CompressionCodec.valueCompressor(StringCodec.UTF8, CompressionCodec.CompressionType.GZIP)).reactive();
        //c.append("gz","abcde\\n\\nfg\\nhij\\nklmn\\nopqrst\\nuvwxyz\\n").block();
        /*c.get("gz")
            .doOnNext(System.out::println)
            .block();*/

        c.getrange("gz",0,100)
            .doOnNext(System.out::println)
            .block();
    }

    private static void readLineTest(RedisReactiveCommands<String, String> commands) {
        //commands.append("k", "abcde\n\nfg\nhij\nklmn\nopqrst\nuvwxyz\n").block();
        long size = 5;
        AtomicLong l = new AtomicLong();
        Mono.fromSupplier(() -> l.getAndAccumulate(size, (pv, v) -> pv + v))
            .flatMap(pos -> commands.getrange("k", pos, pos + size - 1))
            .repeat()
            .takeWhile(s -> !"".equals(s))
            .doOnNext(s -> System.out.println("Get from redis: " + s))
            .scan(Tuples.of(Stream.empty(), ""), Supports.accumulator)
            .doOnNext(tuple -> System.out.println("Tuple(2): " + tuple.getT2()))
            .flatMap(tuple -> Flux.fromStream(tuple.getT1()))
            .doOnNext(s -> System.out.println("After: " + s))
            .blockLast();
    }
}
