package lettuce.chapter;

import io.lettuce.core.ZStoreArgs;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.stream.Stream;

import static lettuce.key.ArticleKey.USER_PREFIX;
import static lettuce.key.C02Key.*;

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
        return comm.hget(LOGIN, token);
    }

    public Mono<Long> updateToken(String token, long userId, long now) {
        return comm.hset(LOGIN, token, USER_PREFIX + userId)
            .then(comm.zadd(RECENT, now, token));
    }

    public Mono<Long> updateToken(String token, long userId, long now, long itemId) {
        String viewedToken = VIEWED + token;
        return updateToken(token, userId, now)
            .then(comm.zadd(viewedToken, now, ITEM + itemId))
            .then(comm.zremrangebyrank(viewedToken, 0, -26));
    }

    public void cleanSession() {
        comm.zcard(RECENT)
            .filter(l -> l > LIMIT)
            .map(size -> Integer.min(size.intValue() - LIMIT, 100) - 1)
            .flatMap(endIndex -> comm.zrange(RECENT, 0, endIndex)
                .collectList())
            .flatMap(tokenList -> {
                String[] tokens = tokenList.toArray(new String[0]);
                String[] sessionKeys = tokenList.stream()
                    .map(token -> VIEWED + token)
                    .toArray(String[]::new);
                return comm.del(sessionKeys)
                    .then(comm.hdel(LOGIN, tokens))
                    .then(comm.zrem(RECENT, tokens));
            })
            .defaultIfEmpty(0L)
            .repeat()
            .delayElements(Duration.ofSeconds(1))
            .subscribe();
    }

    public Mono<Boolean> addToCart(String session, long itemId, int count) {
        if (count <= 0) {
            return comm.hdel(CART + session, ITEM + itemId)
                .thenReturn(true);
        } else {
            return comm.hset(CART + session, ITEM + itemId, String.valueOf(count));
        }
    }

    public void cleanFullSessions() {
        comm.zcard(RECENT)
            .filter(l -> l > LIMIT)
            .map(size -> Integer.min(size.intValue() - LIMIT, 100) - 1)
            .flatMap(endIndex -> comm.zrange(RECENT, 0, endIndex)
                .collectList())
            .flatMap(tokenList -> {
                String[] tokens = tokenList.toArray(new String[0]);
                String[] sessionKeys = tokenList.stream()
                    .flatMap(token -> Stream.of(VIEWED + token, CART + token))
                    .toArray(String[]::new);
                return comm.del(sessionKeys)
                    .then(comm.hdel(LOGIN, tokens))
                    .then(comm.zrem(RECENT, tokens));
            })
            .defaultIfEmpty(0L)
            .repeat()
            .delayElements(Duration.ofSeconds(1))
            .subscribe();
    }

    public Mono<Double> updateToken2(String token, long userId, long now, long itemId) {
        String viewedToken = VIEWED + token;
        String item = ITEM + itemId;
        return updateToken(token, userId, now)
            .then(comm.zadd(viewedToken, now, item))
            .then(comm.zremrangebyrank(viewedToken, 0, -26))
            .then(comm.zincrby(VIEWED, -1, item));
    }

    public void rescaleViewed() {
        comm.zremrangebyrank(VIEWED, 0, -20001)
            .flatMap(l -> comm.zinterstore(VIEWED, ZStoreArgs.Builder
                .weights(0.5), VIEWED))
            .repeat()
            .delayElements(Duration.ofMillis(5))
            .subscribe();
    }
}
