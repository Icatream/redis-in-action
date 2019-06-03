package lettuce.chapter;

import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static lettuce.key.Key00.*;

/**
 * @author YaoXunYu
 * created on 04/08/2019
 */
public class Chapter00 extends BaseChapter {

    private static final String v1 = "item1";
    private static final String v2 = "item2";
    private static final String v3 = "item3";

    public Chapter00(RedisReactiveCommands<String, String> comm) {
        super(comm);
    }

    public void hello() {
        String value = "world";
        Mono<String> set = comm.set(s_Key, value);

        StepVerifier.create(set)
          .expectNext("OK")
          .verifyComplete();

        del(s_Key);
    }

    public void list() {

        //rightPush return the length of the list after the push operation
        StepVerifier.create(comm.rpush(L_Key, v1)
          .then(comm.rpush(L_Key, v2))
          .then(comm.rpush(L_Key, v3)))
          .expectNext(3L)
          .verifyComplete();

        Flux<String> flux = comm.lrange(L_Key, 0, -1);

        StepVerifier.create(flux)
          .expectNext(v1)
          .expectNext(v2)
          .expectNext(v3)
          .verifyComplete();

        Mono<String> index1 = comm.lindex(L_Key, 1);
        StepVerifier.create(index1)
          .expectNext(v2)
          .verifyComplete();

        Mono<String> popValue = comm.lpop(L_Key);
        StepVerifier.create(popValue)
          .expectNext(v1)
          .verifyComplete();

        StepVerifier.create(flux)
          .expectNext(v2)
          .expectNext(v3)
          .verifyComplete();

        del(L_Key);
    }

    //set unordered
    //sadd return the number of elements that were added to the set, not including all the elements already present into the set
    public void set() {

        StepVerifier.create(comm.sadd(S_Key, v1, v2, v3))
          .expectNext(3L)
          .verifyComplete();

        StepVerifier.create(comm.smembers(S_Key))
          .expectNextCount(3)
          .verifyComplete();

        StepVerifier.create(comm.sismember(S_Key, v1))
          .expectNext(true)
          .verifyComplete();

        StepVerifier.create(comm.srem(S_Key, v1))
          .expectNext(1L)
          .verifyComplete();

        StepVerifier.create(comm.sismember(S_Key, v1))
          .expectNext(false)
          .verifyComplete();

        del(S_Key);
    }

    public void hash() {
        String subKey1 = "sub-key1";
        String subKey2 = "sub-key2";

        //put return success
        StepVerifier.create(comm.hset(H_Key, subKey1, v1)
          .then(comm.hset(H_Key, subKey2, v2)))
          .expectNext(true)
          .verifyComplete();

        StepVerifier.create(comm.hgetall(H_Key))
          .expectNextMatches(map -> map.size() == 2)
          .verifyComplete();

        StepVerifier.create(comm.hget(H_Key, subKey1))
          .expectNext(v1)
          .verifyComplete();

        StepVerifier.create(comm.hdel(H_Key, subKey1))
          .expectNext(1L)
          .verifyComplete();

        StepVerifier.create(comm.hgetall(H_Key))
          .expectNextMatches(map -> map.size() == 1)
          .verifyComplete();

        del(H_Key);
    }

    public void zSet() {
        //分值
        StepVerifier.create(comm.zadd(ZS_Key, 728d, v1))
          .expectNext(1L)
          .verifyComplete();

        StepVerifier.create(comm.zadd(ZS_Key, 982d, v2))
          .expectNext(1L)
          .verifyComplete();

        StepVerifier.create(comm.zadd(ZS_Key, 982d, v3))
          .expectNext(1L)
          .verifyComplete();

        StepVerifier.create(comm.zrange(ZS_Key, 0, -1))
          .expectNextCount(3)
          .verifyComplete();

        StepVerifier.create(comm.zrem(ZS_Key, v1))
          .expectNext(1L)
          .verifyComplete();

        del(ZS_Key);
    }
}
