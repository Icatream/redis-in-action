package lettuce.chapter;

import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static lettuce.key.C00Key.*;

/**
 * @author YaoXunYu
 * created on 04/08/2019
 */
public class Chapter00 extends BaseChapter {

    private final String v1 = "item1";
    private final String v2 = "item2";
    private final String v3 = "item3";

    public Chapter00(RedisReactiveCommands<String, String> comm) {
        super(comm);
    }

    public void hello() {
        String value = "world";
        Mono<String> set = comm.set(STR, value);

        StepVerifier.create(set)
            .expectNext("OK")
            .verifyComplete();

        del(STR);
    }

    public void list() {

        //rightPush return the length of the list after the push operation
        StepVerifier.create(comm.rpush(LIST, v1)
            .then(comm.rpush(LIST, v2))
            .then(comm.rpush(LIST, v3)))
            .expectNext(3L)
            .verifyComplete();

        Flux<String> flux = comm.lrange(LIST, 0, -1);

        StepVerifier.create(flux)
            .expectNext(v1)
            .expectNext(v2)
            .expectNext(v3)
            .verifyComplete();

        Mono<String> index1 = comm.lindex(LIST, 1);
        StepVerifier.create(index1)
            .expectNext(v2)
            .verifyComplete();

        Mono<String> popValue = comm.lpop(LIST);
        StepVerifier.create(popValue)
            .expectNext(v1)
            .verifyComplete();

        StepVerifier.create(flux)
            .expectNext(v2)
            .expectNext(v3)
            .verifyComplete();

        del(LIST);
    }

    //set unordered
    //sadd return the number of elements that were added to the set, not including all the elements already present into the set
    public void set() {

        StepVerifier.create(comm.sadd(SET, v1, v2, v3))
            .expectNext(3L)
            .verifyComplete();

        StepVerifier.create(comm.smembers(SET))
            .expectNextCount(3)
            .verifyComplete();

        StepVerifier.create(comm.sismember(SET, v1))
            .expectNext(true)
            .verifyComplete();

        StepVerifier.create(comm.srem(SET, v1))
            .expectNext(1L)
            .verifyComplete();

        StepVerifier.create(comm.sismember(SET, v1))
            .expectNext(false)
            .verifyComplete();

        del(SET);
    }

    public void hash() {
        String subKey1 = "sub-key1";
        String subKey2 = "sub-key2";

        //put return success
        StepVerifier.create(comm.hset(HASH, subKey1, v1)
            .then(comm.hset(HASH, subKey2, v2)))
            .expectNext(true)
            .verifyComplete();

        StepVerifier.create(comm.hgetall(HASH))
            .expectNextMatches(map -> map.size() == 2)
            .verifyComplete();

        StepVerifier.create(comm.hget(HASH, subKey1))
            .expectNext(v1)
            .verifyComplete();

        StepVerifier.create(comm.hdel(HASH, subKey1))
            .expectNext(1L)
            .verifyComplete();

        StepVerifier.create(comm.hgetall(HASH))
            .expectNextMatches(map -> map.size() == 1)
            .verifyComplete();

        del(HASH);
    }

    public void zSet() {
        //分值
        StepVerifier.create(comm.zadd(Z_SET, 728d, v1))
            .expectNext(1L)
            .verifyComplete();

        StepVerifier.create(comm.zadd(Z_SET, 982d, v2))
            .expectNext(1L)
            .verifyComplete();

        StepVerifier.create(comm.zadd(Z_SET, 982d, v3))
            .expectNext(1L)
            .verifyComplete();

        StepVerifier.create(comm.zrange(Z_SET, 0, -1))
            .expectNextCount(3)
            .verifyComplete();

        StepVerifier.create(comm.zrem(Z_SET, v1))
            .expectNext(1L)
            .verifyComplete();

        del(Z_SET);
    }
}
