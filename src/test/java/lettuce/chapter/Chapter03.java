package lettuce.chapter;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author YaoXunYu
 * created on 04/11/2019
 */
public class Chapter03 extends BaseChapter {

    public Chapter03(RedisReactiveCommands<String, String> comm) {
        super(comm);
    }

    public void incr() {
        String key = "key";
        StepVerifier.create(comm.get(key))
          .verifyComplete();

        StepVerifier.create(comm.incr(key))
          .expectNext(1L)
          .verifyComplete();

        StepVerifier.create(comm.incrby(key, 15L))
          .expectNext(16L)
          .verifyComplete();

        StepVerifier.create(comm.decrby(key, 5L))
          .expectNext(11L)
          .verifyComplete();

        StepVerifier.create(comm.get(key))
          .expectNext("11")
          .verifyComplete();

        StepVerifier.create(comm.set(key, "13"))
          .expectNext("OK")
          .verifyComplete();

        StepVerifier.create(comm.incr(key))
          .expectNext(14L)
          .verifyComplete();

        del(key);
    }

    public void subStr() {
        String key = "new-string-key";
        StepVerifier.create(comm.append(key, "hello "))
          .expectNext(6L)
          .verifyComplete();

        StepVerifier.create(comm.append(key, "world!"))
          .expectNext(12L)
          .verifyComplete();

        StepVerifier.create(comm.getrange(key, 3, 7))
          .expectNext("lo wo")
          .verifyComplete();

        StepVerifier.create(comm.setrange(key, 0, "H"))
          .expectNext(12L)
          .verifyComplete();

        StepVerifier.create(comm.get(key))
          .expectNext("Hello world!")
          .verifyComplete();

        StepVerifier.create(comm.setrange(key, 11, ", how are you?"))
          .expectNext(25L)
          .verifyComplete();

        StepVerifier.create(comm.get(key))
          .expectNext("Hello world, how are you?")
          .verifyComplete();

        del(key);
    }

    public void bit() {
        String key = "another-key";
        StepVerifier.create(comm.setbit(key, 2, 1))
          .expectNext(0L)
          .verifyComplete();

        StepVerifier.create(comm.setbit(key, 7, 1))
          .expectNext(0L)
          .verifyComplete();

        StepVerifier.create(comm.get(key))
          .expectNext("!")
          .verifyComplete();

        del(key);
    }

    public void push() {
        String k = "list-key";
        StepVerifier.create(comm.rpush(k, "last"))
          .expectNext(1L)
          .verifyComplete();

        StepVerifier.create(comm.lpush(k, "first"))
          .expectNext(2L)
          .verifyComplete();

        StepVerifier.create(comm.rpush(k, "new last"))
          .expectNext(3L)
          .verifyComplete();

        StepVerifier.create(comm.lrange(k, 0, -1))
          .expectNext("first")
          .expectNext("last")
          .expectNext("new last")
          .verifyComplete();

        StepVerifier.create(comm.lpop(k))
          .expectNext("first")
          .verifyComplete();

        StepVerifier.create(comm.lpop(k))
          .expectNext("last")
          .verifyComplete();

        StepVerifier.create(comm.lrange(k, 0, -1))
          .expectNext("new last")
          .verifyComplete();

        StepVerifier.create(comm.rpush(k, "a", "b", "c"))
          .expectNext(4L)
          .verifyComplete();

        StepVerifier.create(comm.lrange(k, 0, -1))
          .expectNext("new last")
          .expectNext("a")
          .expectNext("b")
          .expectNext("c")
          .verifyComplete();

        StepVerifier.create(comm.ltrim(k, 2, -1))
          .expectNext("OK")
          .verifyComplete();

        StepVerifier.create(comm.lrange(k, 0, -1))
          .expectNext("b")
          .expectNext("c")
          .verifyComplete();

        del(k);
    }

    public void move() {
        String k1 = "list1";
        String k2 = "list2";

        StepVerifier.create(comm.rpush(k1, "item1"))
          .expectNext(1L)
          .verifyComplete();
        StepVerifier.create(comm.rpush(k1, "item2"))
          .expectNext(2L)
          .verifyComplete();
        StepVerifier.create(comm.rpush(k2, "item3"))
          .expectNext(1L)
          .verifyComplete();

        //将list2中的右端元素弹出,list1左端推入,返回被移动的元素
        StepVerifier.create(comm.brpoplpush(1L, k2, k1))
          .expectNext("item3")
          .verifyComplete();

        //list2已无元素
        StepVerifier.create(comm.brpoplpush(1L, k2, k1))
          .verifyComplete();

        StepVerifier.create(comm.lrange(k1, 0, -1))
          .expectNext("item3")
          .expectNext("item1")
          .expectNext("item2")
          .verifyComplete();

        StepVerifier.create(comm.brpoplpush(1L, k1, k2))
          .expectNext("item2")
          .verifyComplete();

        StepVerifier.create(comm.blpop(1L, k1, k2))
          .expectNext(KeyValue.just(k1, "item3"))
          .verifyComplete();
        StepVerifier.create(comm.blpop(1L, k1, k2))
          .expectNext(KeyValue.just(k1, "item1"))
          .verifyComplete();
        StepVerifier.create(comm.blpop(1L, k1, k2))
          .expectNext(KeyValue.just(k2, "item2"))
          .verifyComplete();
        //list1已全部弹出
        StepVerifier.create(comm.blpop(1L, k1, k2))
          .verifyComplete();

        //del(k1);
    }

    public void set() {
        String k1 = "set-key1";
        String k2 = "set-key2";
        StepVerifier.create(comm.sadd(k1, "a", "b", "c"))
          .expectNext(3L)
          .verifyComplete();

        StepVerifier.create(comm.srem(k1, "c", "d"))
          .expectNext(1L)
          .verifyComplete();

        StepVerifier.create(comm.srem(k1, "c", "d"))
          .expectNext(0L)
          .verifyComplete();

        StepVerifier.create(comm.scard(k1))
          .expectNext(2L)
          .verifyComplete();

        StepVerifier.create(comm.smembers(k1))
          .expectNextCount(2L)
          .verifyComplete();

        StepVerifier.create(comm.smove(k1, k2, "a"))
          .expectNext(true)
          .verifyComplete();

        StepVerifier.create(comm.smove(k1, k2, "c"))
          .expectNext(false)
          .verifyComplete();

        StepVerifier.create(comm.smembers(k2))
          .expectNext("a")
          .verifyComplete();

        del(k1);
        del(k2);
    }

    public void set1() {
        String k1 = "set-key1";
        String k2 = "set-key2";
        StepVerifier.create(comm.sadd(k1, "a", "b", "c", "d"))
          .expectNext(4L)
          .verifyComplete();
        StepVerifier.create(comm.sadd(k2, "c", "d", "e", "f"))
          .expectNext(4L)
          .verifyComplete();

        //in k1, not in others, value=[a,b]
        StepVerifier.create(comm.sdiff(k1, k2))
          .expectNextCount(2L)
          .verifyComplete();

        //both in k1 and k2, value=[c,d]
        StepVerifier.create(comm.sinter(k1, k2))
          .expectNextCount(2L)
          .verifyComplete();

        //all distinct items in k1,k2
        StepVerifier.create(comm.sunion(k1, k2))
          .expectNextCount(6L)
          .verifyComplete();

        del(k1);
        del(k2);
    }

    public void hash() {
        String k = "hash-key";
        Map<String, String> map = new HashMap<>();
        map.put("k1", "v1");
        map.put("k2", "v2");
        map.put("k3", "v3");
        StepVerifier.create(comm.hmset(k, map))
          .expectNext("OK")
          .verifyComplete();

        StepVerifier.create(comm.hmget(k, "k2", "k3"))
          .expectNextCount(2L)
          .verifyComplete();

        StepVerifier.create(comm.hlen(k))
          .expectNext(3L)
          .verifyComplete();

        StepVerifier.create(comm.hdel(k, "k3", "k4"))
          .expectNext(1L)
          .verifyComplete();

        StepVerifier.create(comm.hexists(k, "num"))
          .expectNext(false)
          .verifyComplete();

        StepVerifier.create(comm.hincrby(k, "num", 2L))
          .expectNext(2L)
          .verifyComplete();

        StepVerifier.create(comm.hexists(k, "num"))
          .expectNext(true)
          .verifyComplete();

        del(k);
    }

    public void zset() {
        String k = "zset-key";

        StepVerifier.create(comm.zadd(k,
          ScoredValue.just(3, "a"),
          ScoredValue.just(2, "b"),
          ScoredValue.just(1, "c")))
          .expectNext(3L)
          .verifyComplete();

        StepVerifier.create(comm.zincrby(k, 3, "c"))
          .expectNext(4d)
          .verifyComplete();

        StepVerifier.create(comm.zscore(k, "b"))
          .expectNext(2d)
          .verifyComplete();

        StepVerifier.create(comm.zrank(k, "c"))
          .expectNext(2L)
          .verifyComplete();

        StepVerifier.create(comm.zcount(k, Range.create(0, 3)))
          .expectNext(2L)
          .verifyComplete();

        StepVerifier.create(comm.zrem(k, "b"))
          .expectNext(1L)
          .verifyComplete();

        StepVerifier.create(comm.zrangeWithScores(k, 0, -1))
          .expectNextCount(2L)
          .verifyComplete();

        del(k);
    }

    public void zInterAndUnion() {
        String k1 = "zset-1";
        String k2 = "zset-2";
        String x1 = "zset-i";
        String x2 = "zset-j";
        String x3 = "zset-k";

        StepVerifier.create(comm.zadd(k1,
          ScoredValue.just(1, "a"),
          ScoredValue.just(2, "b"),
          ScoredValue.just(3, "c")))
          .expectNext(3L)
          .verifyComplete();
        StepVerifier.create(comm.zadd(k2,
          ScoredValue.just(4, "b"),
          ScoredValue.just(1, "c"),
          ScoredValue.just(0, "d")))
          .expectNext(3L)
          .verifyComplete();

        //ZStoreArgs default SUM
        StepVerifier.create(comm.zinterstore(x1, k1, k2))
          .expectNext(2L)
          .verifyComplete();

        StepVerifier.create(comm.zrangeWithScores(x1, 0, -1))
          .expectNext(ScoredValue.just(4, "c"))
          .expectNext(ScoredValue.just(6, "b"))
          .verifyComplete();

        StepVerifier.create(comm.zunionstore(x2, ZStoreArgs.Builder.min(), k1, k2))
          .expectNext(4L)
          .verifyComplete();

        StepVerifier.create(comm.zrangeWithScores(x2, 0, -1))
          .expectNext(ScoredValue.just(0, "d"))
          .expectNext(ScoredValue.just(1, "a"))
          .expectNext(ScoredValue.just(1, "c"))
          .expectNext(ScoredValue.just(2, "b"))
          .verifyComplete();

        del(k1);
        del(k2);
        del(x1);
        del(x2);
    }

    public void sort() {
        String k = "sort-input";
        StepVerifier.create(comm.rpush(k, "23", "15", "110", "7"))
          .expectNext(4L)
          .verifyComplete();

        StepVerifier.create(comm.sort(k))
          .expectNext("7")
          .expectNext("15")
          .expectNext("23")
          .expectNext("110")
          .verifyComplete();

        StepVerifier.create(comm.sort(k, SortArgs.Builder.alpha()))
          .expectNext("110")
          .expectNext("15")
          .expectNext("23")
          .expectNext("7")
          .verifyComplete();

        StepVerifier.create(comm.hset("d-7", "field", "5")
          .then(comm.hset("d-15", "field", "1"))
          .then(comm.hset("d-23", "field", "9"))
          .then(comm.hset("d-110", "field", "3")))
          .expectNext(true)
          .verifyComplete();

        StepVerifier.create(comm.sort(k, SortArgs.Builder.by("d-*->field")))
          .expectNext("15")
          .expectNext("110")
          .expectNext("7")
          .expectNext("23")
          .verifyComplete();

        StepVerifier.create(comm.sort(k, SortArgs.Builder.by("d-*->field").get("d-*->field")))
          .expectNext("1")
          .expectNext("3")
          .expectNext("5")
          .expectNext("9")
          .verifyComplete();

        del(k);
        del("d-7");
        del("d-15");
        del("d-23");
        del("d-110");
    }

    public void transactionWithAsync(RedisClient client) {
        String k = "key";
        StatefulRedisConnection<String, String> conn = client.connect();
        RedisAsyncCommands<String, String> comm = conn.async();
        comm.multi();
        comm.incr(k);
        RedisFuture<String> f1 = comm.get(k);
        comm.decr(k);
        RedisFuture<String> f2 = comm.get(k);
        comm.del(k);
        comm.exec();
        try {
            System.out.println(f1.get());
            System.out.println(f2.get());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        conn.close();
    }

    /**
     * copy from <a>https://github.com/lettuce-io/lettuce-core/wiki/Transactions</a>
     * not thread safe, if you are using single connection
     *
     * @see Chapter03 test
     */
    public void transactionWithReactive(RedisClient client) {
        String k = "key";
        StatefulRedisConnection<String, String> conn = client.connect();
        RedisReactiveCommands<String, String> comm = conn.reactive();
        comm.multi()
          .subscribe(ok -> {
              comm.incr(k)
                .subscribe(System.out::println);
              comm.decr(k)
                .subscribe(System.out::println);
              comm.del(k)
                .subscribe(System.out::println);
              comm.exec().subscribe();
          });

        comm.get(k)
          .block();
    }

    /**
     * throw exceptions in sub-thread
     */
    public void test() {
        new Thread(() -> {
            System.out.println("thread1");
            try {
                System.out.println("t1 multi start");
                comm.multi()
                  .doOnNext(ok -> comm.lpush("k", "v1").subscribe())
                  .block();
                System.out.println("ti multi end");
                Thread.sleep(10000);
                System.out.println("t1 after sleep");
                comm.exec()
                  .doOnNext(res -> {
                      System.out.println("t1-exec");
                      System.out.println(res.wasDiscarded());
                  })
                  .block();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        new Thread(() -> {
            try {
                Thread.sleep(1000);
                System.out.println("thread2");
                comm.multi()
                  .doOnNext(ok -> comm.lpush("k", "v2").subscribe())
                  .block();
                comm.exec()
                  .doOnNext(res -> {
                      System.out.println("thread2-exec");
                      for (int i = 0; i < res.size(); i++) {
                          System.out.println("i: " + i + " , res: " + res.get(i));
                      }
                  })
                  .block();
            } catch (Exception e) {
                System.out.println("t2-multi-err");
                e.printStackTrace();
            }
        }).start();
    }

    public void expire() {
        String k = "key";
        String v = "value";
        StepVerifier.create(comm.set(k, v))
          .expectNext("OK")
          .verifyComplete();

        StepVerifier.create(comm.get(k))
          .expectNext(v)
          .verifyComplete();

        StepVerifier.create(comm.expire(k, 2))
          .expectNext(true)
          .verifyComplete();

        StepVerifier.create(comm.get(k)
          .delaySubscription(Duration.ofSeconds(2)))
          .verifyComplete();
    }
}
