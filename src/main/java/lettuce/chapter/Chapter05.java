package lettuce.chapter;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lettuce.enums.Severity;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static lettuce.key.BaseKey.SEPARATOR;
import static lettuce.key.C02Key.RECENT;
import static lettuce.key.C05Key.COMMON;

/**
 * @author YaoXunYu
 * created on 04/17/2019
 */
public class Chapter05 extends BaseChapter {

    private final Mono<String> logCommonSHA1;

    public Chapter05(RedisReactiveCommands<String, String> comm) {
        super(comm);
        try {
            URL logCommon = ClassLoader.getSystemResource("lua/LogCommon.lua");
            String logCommonLua = new String(Files.readAllBytes(Paths.get(logCommon.toURI())));
            logCommonSHA1 = comm.scriptLoad(logCommonLua)
                .cache();
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    //instead of using pipeline in lettuce, using lua script
    public Mono<String> logRecent(String name, Severity severity, String message) {
        String destination = RECENT + name + SEPARATOR + severity.value;
        String msg = LocalDateTime.now() + " " + message;
        return comm.lpush(destination, msg)
            .then(comm.ltrim(destination, 0, 99));
    }

    //TODO 未测试
    public Mono<Boolean> logCommon(String name, String message, Severity severity, long timeout) {
        String lDes = COMMON + name + SEPARATOR + severity.value;
        String mDes = RECENT + name + SEPARATOR + severity.value;
        long hour = LocalDateTime.now()
            .withNano(0).withSecond(0).withMinute(0)
            .atZone(ZoneOffset.systemDefault()).toEpochSecond();
        return logCommonSHA1.flatMap(sha -> comm.evalsha(sha,
            ScriptOutputType.BOOLEAN,
            new String[]{lDes, mDes},
            String.valueOf(hour),
            message)
            .single()
            .map(b -> (Boolean) b));
    }

}
