package ria.lettuce.chapter;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.codec.CompressionCodec;
import io.lettuce.core.codec.StringCodec;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.test.StepVerifier;
import ria.lettuce.key.Key01;
import ria.lettuce.key.Key04;

/**
 * @author YaoXunYu
 * created on 06/19/19
 */
public class Chapter04Test {

    private static RedisReactiveCommands<String, String> comm;
    private static Chapter04 chapter04;

    @BeforeClass
    public static void setUp() {
        RedisClient client = RedisClient.create(RedisURI.create("localhost", 6379));
        comm = client.connect(CompressionCodec.valueCompressor(StringCodec.UTF8, CompressionCodec.CompressionType.GZIP)).reactive();
        chapter04 = new Chapter04(comm);
    }

    private static void del(String key) {
        comm.del(key)
          .block();
    }

    public void listItem(long itemId, long sellerId, long price) {
        String itemIdStr = String.valueOf(itemId);
        String inventory = Key04.S_INVENTORY(sellerId);

        StepVerifier.create(comm.sadd(inventory, itemIdStr))
          .expectNext(1L)
          .verifyComplete();

        String item = itemId + "." + sellerId;
        //long end = LocalDateTime.now().plusSeconds(5).atZone(ZoneOffset.systemDefault()).toEpochSecond();
        chapter04.listItemSHA1.flatMap(sha1 -> comm.evalsha(sha1,
          ScriptOutputType.BOOLEAN,
          new String[]{inventory, Key04.Z_MARKET, item},
          itemIdStr, String.valueOf(price))
          .single())
          .doOnNext(r -> {
              System.out.println(r.getClass());
              System.out.println(r);
          })
          .block();

        StepVerifier.create(comm.sismember(inventory, itemIdStr))
          .expectNext(false)
          .verifyComplete();

        StepVerifier.create(comm.zrange(Key04.Z_MARKET, 0, -1))
          .expectNext(item)
          .verifyComplete();

        del(Key04.Z_MARKET);
    }

    public void purchaseItem(long itemId, long sellerId, long buyerId, long lPrice) {
        int fundsOfBuyer = 120;

        if (lPrice > fundsOfBuyer) {
            throw new IllegalArgumentException();
        }

        String seller = Key01.v_USER(sellerId);
        String buyer = Key01.v_USER(buyerId);
        String bInventory = Key04.S_INVENTORY(buyerId);
        String item = itemId + "." + sellerId;

        StepVerifier.create(comm.zadd(Key04.Z_MARKET, lPrice, item))
          .expectNext(1L)
          .verifyComplete();

        //设置buyer拥有的金额
        StepVerifier.create(comm.hset(buyer, Key04.f_FUNDS, String.valueOf(fundsOfBuyer)))
          .expectNext(true)
          .verifyComplete();

        StepVerifier.create(chapter04.purchaseItemSHA1.flatMap(sha1 -> comm.evalsha(sha1,
          ScriptOutputType.BOOLEAN,
          new String[]{Key04.Z_MARKET, buyer, seller, bInventory, Key04.f_FUNDS},
          item,
          String.valueOf(lPrice),
          String.valueOf(itemId))
          .single()))
          .expectNextMatches(o -> {
              System.out.println(o);
              return true;
          })
          .verifyComplete();

        StepVerifier.create(comm.zrange(Key04.Z_MARKET, 0, -1))
          .verifyComplete();

        StepVerifier.create(comm.hget(buyer, Key04.f_FUNDS))
          .expectNext(String.valueOf(fundsOfBuyer - lPrice))
          .verifyComplete();

        StepVerifier.create(comm.smembers(bInventory))
          .expectNext(String.valueOf(itemId))
          .verifyComplete();

        StepVerifier.create(comm.del(buyer, seller, bInventory))
          .expectNext(3L)
          .verifyComplete();
    }
}
