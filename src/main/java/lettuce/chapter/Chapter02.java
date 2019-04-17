package lettuce.chapter;

import lettuce.key.ArticleKey;
import lettuce.key.C02Key;
import io.lettuce.core.ZStoreArgs;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.stream.Stream;

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
        return comm.hget(C02Key.LOGIN, token);
    }

    public Mono<Long> updateToken(String token, long userId, long now) {
        return comm.hset(C02Key.LOGIN, token, ArticleKey.USER_PREFIX + userId)
            .then(comm.zadd(C02Key.RECENT, now, token));
    }

    public Mono<Long> updateToken(String token, long userId, long now, long itemId) {
        String viewedToken = C02Key.VIEWED + token;
        return updateToken(token, userId, now)
            .then(comm.zadd(viewedToken, now, C02Key.ITEM + itemId))
            .then(comm.zremrangebyrank(viewedToken, 0, -26));
    }

    public void cleanSession() {
        comm.zcard(C02Key.RECENT)
            .filter(l -> l > LIMIT)
            .map(size -> Integer.min(size.intValue() - LIMIT, 100) - 1)
            .flatMap(endIndex -> comm.zrange(C02Key.RECENT, 0, endIndex)
                .collectList())
            .flatMap(tokenList -> {
                String[] tokens = tokenList.toArray(new String[0]);
                String[] sessionKeys = tokenList.stream()
                    .map(token -> C02Key.VIEWED + token)
                    .toArray(String[]::new);
                return comm.del(sessionKeys)
                    .then(comm.hdel(C02Key.LOGIN, tokens))
                    .then(comm.zrem(C02Key.RECENT, tokens));
            })
            .defaultIfEmpty(0L)
            .repeat()
            .delayElements(Duration.ofSeconds(1))
            .subscribe();
    }

    public Mono<Boolean> addToCart(String session, long itemId, int count) {
        if (count <= 0) {
            return comm.hdel(C02Key.CART + session, C02Key.ITEM + itemId)
                .thenReturn(true);
        } else {
            return comm.hset(C02Key.CART + session, C02Key.ITEM + itemId, String.valueOf(count));
        }
    }

    public void cleanFullSessions() {
        comm.zcard(C02Key.RECENT)
            .filter(l -> l > LIMIT)
            .map(size -> Integer.min(size.intValue() - LIMIT, 100) - 1)
            .flatMap(endIndex -> comm.zrange(C02Key.RECENT, 0, endIndex)
                .collectList())
            .flatMap(tokenList -> {
                String[] tokens = tokenList.toArray(new String[0]);
                String[] sessionKeys = tokenList.stream()
                    .flatMap(token -> Stream.of(C02Key.VIEWED + token, C02Key.CART + token))
                    .toArray(String[]::new);
                return comm.del(sessionKeys)
                    .then(comm.hdel(C02Key.LOGIN, tokens))
                    .then(comm.zrem(C02Key.RECENT, tokens));
            })
            .defaultIfEmpty(0L)
            .repeat()
            .delayElements(Duration.ofSeconds(1))
            .subscribe();
    }

    public Mono<Double> updateToken2(String token, long userId, long now, long itemId) {
        String viewedToken = C02Key.VIEWED + token;
        String item = C02Key.ITEM + itemId;
        return updateToken(token, userId, now)
            .then(comm.zadd(viewedToken, now, item))
            .then(comm.zremrangebyrank(viewedToken, 0, -26))
            .then(comm.zincrby(C02Key.VIEWED, -1, item));
    }

    public void rescaleViewed() {
        comm.zremrangebyrank(C02Key.VIEWED, 0, -20001)
            .flatMap(l -> comm.zinterstore(C02Key.VIEWED, ZStoreArgs.Builder
                .weights(0.5), C02Key.VIEWED))
            .repeat()
            .delayElements(Duration.ofMillis(5))
            .subscribe();
    }
}
