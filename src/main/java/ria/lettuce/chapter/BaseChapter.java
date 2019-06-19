package ria.lettuce.chapter;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.codec.CompressionCodec;
import io.lettuce.core.codec.StringCodec;
import reactor.core.publisher.Mono;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author YaoXunYu
 * created on 04/17/2019
 */
public abstract class BaseChapter {

    protected final RedisReactiveCommands<String, String> comm;

    protected BaseChapter(RedisReactiveCommands<String, String> comm) {
        this.comm = comm;
    }

    protected final Mono<String> uploadScript(String path) {
        return Mono.fromCallable(() -> new String(Files.readAllBytes(Paths.get(ClassLoader.getSystemResource(path).toURI()))))
          .flatMap(comm::scriptLoad)
          .cache();
    }

    public static RedisReactiveCommands<String, String> getGZipCommands(RedisClient client) {
        return client.connect(CompressionCodec.valueCompressor(StringCodec.UTF8, CompressionCodec.CompressionType.GZIP)).reactive();
    }
}
