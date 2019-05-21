package lettuce.chapter;

import io.lettuce.core.ZStoreArgs;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lettuce.key.Key01;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static lettuce.key.Key02.*;

/**
 * @author YaoXunYu
 * created on 04/10/2019
 */
public class Chapter02 extends BaseChapter {

    private final static int LIMIT = 100000000;

    public Chapter02(RedisReactiveCommands<String, String> comm) {
        super(comm);
    }

    public Mono<String> checkToken(String token) {
        return comm.hget(H_LOGIN, token);
    }

    public Mono<Long> updateToken(String token, long userId, long now) {
        return comm.hset(H_LOGIN, token, Key01.v_USER(userId))
            .then(comm.zadd(Z_RECENT, now, token));
    }

    public Mono<Long> updateToken(String token, long userId, long now, long itemId) {
        String viewedToken = VIEWED.apply(token);
        return updateToken(token, userId, now)
            .then(comm.zadd(viewedToken, now, v_ITEM(itemId)))
            .then(comm.zremrangebyrank(viewedToken, 0, -26));
    }

    public void cleanSessions() {
        cleanSession(tokenList -> tokenList.stream()
            .map(VIEWED)
            .toArray(String[]::new))
            .subscribe();
    }

    public Mono<Boolean> addToCart(String session, long itemId, int count) {
        if (count <= 0) {
            return comm.hdel(H_CART(session), v_ITEM(itemId))
                .thenReturn(true);
        } else {
            return comm.hset(H_CART(session), v_ITEM(itemId), String.valueOf(count));
        }
    }

    public void cleanFullSessions() {
        cleanSession(tokenList -> tokenList.stream()
            .flatMap(token -> Stream.of(VIEWED.apply(token), H_CART(token)))
            .toArray(String[]::new))
            .subscribe();
    }

    private Flux<Long> cleanSession(Function<List<String>, String[]> f) {
        return comm.zcard(Z_RECENT)
            .filter(l -> l > LIMIT)
            .map(size -> Integer.min(size.intValue() - LIMIT, 100) - 1)
            .flatMap(endIndex -> comm.zrange(Z_RECENT, 0, endIndex)
                .collectList())
            .flatMap(tokenList -> {
                String[] tokens = tokenList.toArray(new String[0]);
                String[] sessionKeys = f.apply(tokenList);
                return comm.del(sessionKeys)
                    .then(comm.hdel(H_LOGIN, tokens))
                    .then(comm.zrem(Z_RECENT, tokens));
            })
            .defaultIfEmpty(0L)
            .repeat()
            .delayElements(Duration.ofSeconds(1));
    }

    public Mono<Double> updateToken2(String token, long userId, long now, long itemId) {
        String viewedToken = VIEWED.apply(token);
        String item = v_ITEM(itemId);
        return updateToken(token, userId, now)
            .then(comm.zadd(viewedToken, now, item))
            .then(comm.zremrangebyrank(viewedToken, 0, -26))
            .then(comm.zincrby(Z_VIEWED, -1, item));
    }

    public void rescaleViewed() {
        comm.zremrangebyrank(Z_VIEWED, 0, -20001)
            .flatMap(l -> comm.zinterstore(Z_VIEWED, ZStoreArgs.Builder
                .weights(0.5), Z_VIEWED))
            .repeat()
            .delayElements(Duration.ofMillis(5))
            .subscribe();
    }
}
