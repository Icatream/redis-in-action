package lettuce.chapter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.*;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lettuce.enums.Severity;
import lettuce.pojo.City;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

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

    private final ObjectMapper mapper = new ObjectMapper();

    public Chapter05(RedisReactiveCommands<String, String> comm) {
        super(comm);
        logCommonSHA1 = uploadScript("lua/LogCommon.lua");
        timeSpecificCounterSHA1 = uploadScript("lua/TimeSpecificCounter.lua");
        cleanCounterSHA1 = uploadScript("lua/CleanCounter.lua");
        updateStatsSHA1 = uploadScript("lua/UpdateStats.lua");
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
        return logCommonSHA1.flatMap(sha1 -> comm.evalsha(sha1,
            ScriptOutputType.BOOLEAN,
            new String[]{lDes, mDes},
            String.valueOf(hour),
            message)
            .single()
            .cast(Boolean.class));
    }

    public Mono<Boolean> updateCounter(String name, int count) {
        long now = Instant.now().getEpochSecond();
        return timeSpecificCounterSHA1.flatMap(sha1 -> comm.evalsha(sha1,
            ScriptOutputType.BOOLEAN,
            new String[]{KNOWN, COUNT},
            String.valueOf(now),
            name,
            String.valueOf(count))
            .single()
            .cast(Boolean.class));
    }

    public Mono<Map<String, String>> getCounter(String name, int precision) {
        String hashK = COUNT + precision + ":" + name;
        return comm.hgetall(hashK);
    }

    public Flux<Boolean> cleanCounters() {
        String sampleCount = "100";
        AtomicInteger passes = new AtomicInteger();
        return cleanCounterSHA1.flatMapMany(sha1 -> Mono.fromSupplier(() -> Tuples.of(passes.getAndIncrement(),
            Instant.now().getEpochSecond()))
            .flatMap(tuple -> comm.evalsha(sha1,
                ScriptOutputType.BOOLEAN,
                new String[]{KNOWN, COUNT},
                String.valueOf(tuple.getT1()),
                String.valueOf(tuple.getT2()),
                sampleCount)
                .single()
                .cast(Boolean.class))
            .repeat())
            .delayElements(Duration.ofSeconds(60));
    }

    @SuppressWarnings("unchecked")
    public Mono<List<String>> updateStats(String context, String type, int value) {
        String des = STATS + context + SEPARATOR + type;
        long hour = LocalDateTime.now()
            .withNano(0).withSecond(0).withMinute(0)
            .atZone(ZoneOffset.systemDefault()).toEpochSecond();
        return updateStatsSHA1.flatMap(sha1 -> comm.evalsha(sha1,
            ScriptOutputType.MULTI,
            new String[]{des},
            String.valueOf(hour),
            String.valueOf(value))
            .single())
            .map(o -> (List<String>) o);
    }

    public Mono<Map<String, ScoredValue<String>>> getStats(String context, String type) {
        String key = STATS + context + SEPARATOR + type;
        return comm.zrangeWithScores(key, 0, -1)
            .collectMap(Value::getValue)
            .map(map -> {
                double sum = map.get("sum").getScore();
                double count = map.get("count").getScore();
                String ave = "average";
                map.put(ave, ScoredValue.just(sum / count, ave));
                double sumsq = map.get("sumsq").getScore();
                String stddev = "stddev";
                map.put(stddev, ScoredValue.just(sumsq - Math.pow(sum, 2) / count, stddev));
                return map;
            });
    }

    /**
     * no check {@param ip}, may throw NumberFormatException
     */
    public static int ipToScore(String ip) {
        return Stream.of(ip.split("\\."))
            .mapToInt(Integer::parseInt)
            .reduce(0, (left, right) -> left * 256 + right);
    }

    public Mono<Long> importIpsToRedisByRow(int rowIndex, int cityId, int startIpNum) {
        String city = cityId + CITY_SEP + rowIndex;
        return comm.zadd(IP_CITY, startIpNum, city);
    }

    public Mono<Boolean> importCitiesToRedisByRow(City city) {
        try {
            String field = String.valueOf(city.getLocId());
            String json = mapper.writeValueAsString(city);
            return comm.hset(CITY_HASH, field, json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public Mono<City> findCityByIp(String ip) {
        int ipScore = ipToScore(ip);
        return comm.zrevrangebyscore(IP_CITY, Range.create(ipScore, 0), Limit.create(0, 1))
            .single()
            .flatMap(cityId -> {
                String id = cityId.split("_")[0];
                return comm.hget(CITY_HASH, id);
            })
            .map(json -> mapper.convertValue(json, City.class));
    }

    private final AtomicBoolean isUnderMaintenance = new AtomicBoolean(false);

    public void updateMaintenanceState() {
        comm.get(IS_UNDER_MAINTENANCE)
            .doOnNext(s -> isUnderMaintenance.set(Boolean.parseBoolean(s)))
            .repeat()
            .delayElements(Duration.ofSeconds(1))
            .subscribe();
    }

}
