package lettuce.chapter;

import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lettuce.enums.Severity;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static lettuce.key.BaseKey.SEPARATOR;
import static lettuce.key.C02Key.RECENT;

/**
 * @author YaoXunYu
 * created on 04/17/2019
 */
public class Chapter05 extends BaseChapter {
    public Chapter05(RedisReactiveCommands<String, String> comm) {
        super(comm);
    }

    //instead of using pipeline in lettuce, using lua script
    public Mono<String> logRecent(String name, Severity severity, String message) {
        String destination = RECENT + SEPARATOR + name + SEPARATOR + severity.value;
        String msg = LocalDateTime.now() + " " + message;
        return comm.lpush(destination, msg)
            .then(comm.ltrim(destination, 0, 99));
    }

}
