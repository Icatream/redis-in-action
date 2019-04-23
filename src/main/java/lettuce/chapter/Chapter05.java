package lettuce.chapter;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lettuce.enums.Severity;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static lettuce.key.BaseKey.SEPARATOR;
import static lettuce.key.C02Key.RECENT;
import static lettuce.key.C05Key.*;

/**
 * @author YaoXunYu
 * created on 04/17/2019
 */
public class Chapter05 extends BaseChapter {

    private final Mono<String> logCommonSHA1;
    private final Mono<String> timeSpecificCounterSHA1;
    private final Mono<String> cleanCounterSHA1;
    private final Mono<String> updateStatsSHA1;

    public Chapter05(RedisReactiveCommands<String, String> comm) {
        super(comm);
        try {
            URL logCommon = ClassLoader.getSystemResource("lua/LogCommon.lua");
            String logCommonLua = new String(Files.readAllBytes(Paths.get(logCommon.toURI())));
            logCommonSHA1 = comm.scriptLoad(logCommonLua)
                .cache();
            URL timeSpecificCounter = ClassLoader.getSystemResource("lua/TimeSpecificCounter.lua");
            String timeSpecificCounterLua = new String(Files.readAllBytes(Paths.get(timeSpecificCounter.toURI())));
            timeSpecificCounterSHA1 = comm.scriptLoad(timeSpecificCounterLua)
                .cache();
            URL cleanCounter = ClassLoader.getSystemResource("lua/CleanCounter.lua");
            String cleanCounterLua = new String(Files.readAllBytes(Paths.get(cleanCounter.toURI())));
            cleanCounterSHA1 = comm.scriptLoad(cleanCounterLua)
                .cache();
            URL updateStats = ClassLoader.getSystemResource("lua/UpdateStats.lua");
            String updateStatsLua = new String(Files.readAllBytes(Paths.get(updateStats.toURI())));
            updateStatsSHA1 = comm.scriptLoad(updateStatsLua)
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

    public Mono<Boolean> updateCounter(String name, int count) {
        long now = LocalDateTime.now()
            .atZone(ZoneOffset.systemDefault()).toEpochSecond();
        return timeSpecificCounterSHA1.flatMap(sha -> comm.evalsha(sha,
            ScriptOutputType.BOOLEAN,
            new String[]{KNOWN, COUNT},
            String.valueOf(now),
            name,
            String.valueOf(count))
            .single()
            .map(b -> (Boolean) b));
    }

    public Mono<Map<String, String>> getCounter(String name, int precision) {
        String hashK = COUNT + precision + ":" + name;
        return comm.hgetall(hashK);
    }

    public Flux<Boolean> cleanCounters() {
        String sampleCount = "100";
        AtomicInteger passes = new AtomicInteger();
        return cleanCounterSHA1.flatMapMany(sha -> Mono.fromSupplier(() -> Tuples.of(passes.getAndIncrement(),
            LocalDateTime.now().atZone(ZoneOffset.systemDefault()).toEpochSecond()))
            .flatMap(tuple -> comm.evalsha(sha,
                ScriptOutputType.BOOLEAN,
                new String[]{KNOWN, COUNT},
                String.valueOf(tuple.getT1()),
                String.valueOf(tuple.getT2()),
                sampleCount)
                .single()
                .map(b -> (Boolean) b))
            .repeat())
            .delayElements(Duration.ofSeconds(60));
    }

    public Mono<Boolean> updateStats(String context, String type, int value) {
        String des = STATS + context + SEPARATOR + type;
        long hour = LocalDateTime.now()
            .withNano(0).withSecond(0).withMinute(0)
            .atZone(ZoneOffset.systemDefault()).toEpochSecond();
        return updateStatsSHA1.flatMap(sha -> comm.evalsha(sha,
            ScriptOutputType.BOOLEAN,
            new String[]{des},
            String.valueOf(hour),
            String.valueOf(value))
            .single()
            .map(b -> (Boolean) b));
    }
}
