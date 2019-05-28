package lettuce.chapter;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
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



    public static void main(String[] args) {
        RedisClient client = RedisClient.create(RedisURI.create("localhost", 6379));
        RedisReactiveCommands<String, String> c = client.connect().reactive();
        c.hget("k", "f")
            .doOnNext(s -> System.out.println("hh"))
            .block();
    }
}
