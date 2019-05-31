package lettuce.chapter;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lettuce.key.Key08;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static lettuce.key.Key08.*;

/**
 * @author YaoXunYu
 * created on 05/28/19
 */
public class Chapter08 extends BaseChapter {
    public Chapter08(RedisReactiveCommands<String, String> comm) {
        super(comm);
    }

    public Mono<String> createUser(String login, String name) {
        String lLogin = login.toLowerCase();
        String lockName = v_USER(lLogin);
        //acquire lock, if failed user-side retry
        return Chapter06.acquireLockWithTimeout(comm, lockName)
            .flatMap(lockId -> comm.hget(USER, lLogin)
                .map(s -> Tuples.of(false, s))
                //user id no exist, create user
                .switchIfEmpty(comm.incr(USER_ID)
                    .map(String::valueOf)
                    .flatMap(id -> Mono.zip(comm.hset(USER, lLogin, id),
                        comm.hmset(H_USER(id), generateUser(login, id, name)))
                        .thenReturn(Tuples.of(true, id))))
                //releaseLock
                .flatMap(tuple -> Chapter06.releaseLock(comm, lockName, lockId)
                    //if filtered, user exist. TODO throw 'user-exist' exception
                    .filter(b -> tuple.getT1())
                    .map(b -> tuple.getT2())));
    }

    private Map<String, String> generateUser(String login, String id, String name) {
        Map<String, String> map = new HashMap<>();
        map.put("login", login);
        map.put("id", id);
        map.put("name", name);
        map.put("followers", "0");
        map.put("following", "0");
        map.put("posts", "0");
        map.put("signup", String.valueOf(ZonedDateTime.now().toEpochSecond()));
        return map;
    }

    public Mono<String> createStatus(String userId, String message, Map<String, String> data) {
        String userKey = H_USER(userId);
        return comm.hget(userKey, "login")
            .flatMap(login -> comm.incr(STATUS_ID)
                .map(String::valueOf)
                .flatMap(id -> {
                    data.put("message", message);
                    data.put("posted", String.valueOf(ZonedDateTime.now().toEpochSecond()));
                    data.put("id", id);
                    data.put("uid", userId);
                    data.put("login", login);
                    return Mono.zip(comm.hmset(H_STATUS(id), data),
                        comm.hincrby(userKey, "posts", 1))
                        .thenReturn(id);
                }));
    }

    public Flux<Map<String, String>> getStatusMessages(String userId, String timeLine, long start, long end) {
        return comm.zrevrange(timeLine + SEPARATOR + userId, start, end)
            .map(Key08::H_STATUS)
            .flatMap(comm::hgetall);
    }

    private static final long HOME_TIMELINE_SIZE = 1000;

    public Mono<Void> followUser(String userId, String otherUserId) {
        String fKey1 = Z_FOLLOWING(userId);
        String fKey2 = Z_FOLLOWERS(otherUserId);
        long now = ZonedDateTime.now().toEpochSecond();
        String homeKey = Z_HOME(userId);
        //if followed, throw exception
        return comm.zscore(fKey1, otherUserId)
            //TODO throw specific exception
            .flatMap(d -> Mono.error(new RuntimeException("followed")))
            .switchIfEmpty(Mono.zip(comm.zadd(fKey1, now, otherUserId)
                    .flatMap(following -> comm.hincrby(H_USER(userId), FOLLOWING, following)),
                comm.zadd(fKey2, now, userId)
                    .flatMap(followers -> comm.hincrby(H_USER(otherUserId), FOLLOWERS, followers)))
                .then(comm.zrevrangeWithScores(Z_PROFILE(otherUserId), 0, HOME_TIMELINE_SIZE)
                    .collectList()
                    .filter(list -> list.size() > 0)
                    .flatMap(sv -> comm.zadd(homeKey, sv.toArray())))
                .then(comm.zremrangebyrank(homeKey, 0, -HOME_TIMELINE_SIZE - 1)))
            .then();
    }

    //TODO recharge home-line after remove unfollow-user
    public Mono<Long> unfollowUser(String userId, String otherUserId) {
        String fKey1 = Z_FOLLOWING(userId);
        String fKey2 = Z_FOLLOWERS(otherUserId);
        return comm.zscore(fKey1, otherUserId)
            .flatMap(d -> Mono.zip(comm.zrem(fKey1, otherUserId)
                    .flatMap(following -> comm.hincrby(H_USER(userId), FOLLOWING, -following)),
                comm.zrem(fKey2, userId)
                    .flatMap(followers -> comm.hincrby(H_USER(otherUserId), FOLLOWERS, -followers)))
                .then(comm.zrevrange(Z_PROFILE(otherUserId), 0, HOME_TIMELINE_SIZE - 1)
                    .collectList()
                    .filter(list -> list.size() > 0)
                    .flatMap(list -> comm.zrem(Z_HOME(userId), list.toArray(new String[0])))));
    }

    //TODO add following to specific-follow-type, follow user with specific-follow-type

    public Mono<String> postStatus(String userId, String message, Map<String, String> data) {
        return createStatus(userId, message, data)
            .flatMap(id -> comm.hget(H_STATUS(id), "posted")
                .map(Double::valueOf)
                .flatMap(posted -> comm.zadd(Z_PROFILE(userId), posted, id)
                    .flatMap(l -> syndicateStatus(userId, ScoredValue.just(posted, id)))
                    .thenReturn(id)));
    }

    private static final long POSTS_PER_PASS = 1000;

    public Mono<Long> syndicateStatus(String userId, ScoredValue<String> post) {
        return syndicateStatus(userId, post, 0);
    }

    @SuppressWarnings("unchecked")
    public Mono<Long> syndicateStatus(String userId, ScoredValue<String> post, long offset) {
        String followers = Z_FOLLOWERS(userId);
        //正向range,新增关注者不会影响已关注用户的时间线获取,取关则会让部分用户时间线重刷,也不会对结果产生影响
        return comm.zrangebyscore(followers, Range.unbounded(), Limit.create(offset, POSTS_PER_PASS))
            .map(Key08::Z_HOME)
            .flatMap(fKey -> comm.zadd(fKey, post)
                .then(comm.zremrangebyrank(fKey, 0, -HOME_TIMELINE_SIZE - 1)))
            .then(comm.zcard(followers)
                .filter(size -> offset + POSTS_PER_PASS < size)
                .doOnNext(size -> Chapter06.executeLater(syndicateStatus(userId, post, offset)
                    .then())));
    }

    public Mono<Boolean> deleteStatus(String userId, String statusId) {
        String key = H_STATUS(statusId);
        return Chapter06.acquireLockWithTimeout(comm, key, 1)
            .flatMap(lock -> comm.hget(key, "uid")
                .filter(s -> s.equals(userId))
                .flatMap(s -> Mono.zip(comm.del(key),
                    comm.zrem(Z_PROFILE(userId), statusId),
                    comm.zrem(Z_HOME(userId), statusId),
                    comm.hincrby(H_USER(userId), "posts", -1)))
                .then(Chapter06.releaseLock(comm, key, lock)));
    }
}
