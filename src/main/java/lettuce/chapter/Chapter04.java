package lettuce.chapter;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lettuce.key.Key01;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static lettuce.key.Key04.*;

/**
 * @author YaoXunYu
 * created on 04/12/2019
 */
public class Chapter04 extends BaseChapter {
    private final Mono<String> listItemSHA1;
    private final Mono<String> purchaseItemSHA1;

    public Chapter04(RedisReactiveCommands<String, String> comm) {
        super(comm);
        listItemSHA1 = uploadScript("lua/ListItem.lua");
        purchaseItemSHA1 = uploadScript("lua/PurchaseItem.lua");
    }

    //Warning: No error handler
    public Mono<Void> processLog(Path outerPath, Function<String, Mono<Void>> lineAnalyze) {
        return comm.get(s_FILE)
          .zipWith(comm.get(s_OFFSET)
            .map(Integer::valueOf))
          .flatMap(tuple -> {
              String currentFile = tuple.getT1();
              int offset = tuple.getT2();
              Path current = outerPath.resolve(currentFile);
              return analyzeFile(current, offset, lineAnalyze)
                .then(Mono.fromSupplier(
                  () -> {
                      try {
                          return Files.walk(outerPath, 1)
                            .filter(Files::isDirectory)
                            .filter(path -> !path.equals(outerPath))
                            .filter(path -> path.getFileName().toString().compareTo(currentFile) > 0);
                      } catch (IOException e) {
                          throw new RuntimeException(e);
                      }
                  })
                  .flatMapMany(Flux::fromStream)
                  .flatMap(path -> analyzeFile(path, 0, lineAnalyze))
                  .then());
          });
    }

    private Mono<Integer> analyzeFile(Path file, int offset, Function<String, Mono<Void>> lineAnalyze) {
        AtomicInteger i = new AtomicInteger(offset);
        return Mono.fromSupplier(
          () -> {
              try {
                  return Files.readAllLines(file);
              } catch (IOException e) {
                  throw new RuntimeException(e);
              }
          })
          .flatMapMany(Flux::fromIterable)
          .skip(offset)
          .window(1000)
          .flatMap(flux -> flux.doOnNext(line -> i.incrementAndGet())
            .flatMap(lineAnalyze)
            .then(updateProgress(file, i.get())))
          .then(Mono.just(i.get()));
    }

    private Mono<String> updateProgress(Path file, int offset) {
        Map<String, String> map = new HashMap<>();
        map.put(s_FILE, file.getFileName().toString());
        map.put(s_OFFSET, String.valueOf(offset));
        return comm.mset(map);
    }

    public void listItem(long itemId, long sellerId, long price) {
        String itemIdStr = String.valueOf(itemId);
        String inventory = S_INVENTORY(sellerId);

        StepVerifier.create(comm.sadd(inventory, itemIdStr))
          .expectNext(1L)
          .verifyComplete();

        String item = itemId + "." + sellerId;
        //long end = LocalDateTime.now().plusSeconds(5).atZone(ZoneOffset.systemDefault()).toEpochSecond();
        listItemSHA1.flatMap(sha1 -> comm.evalsha(sha1,
          ScriptOutputType.BOOLEAN,
          new String[]{inventory, Z_MARKET, item},
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

        StepVerifier.create(comm.zrange(Z_MARKET, 0, -1))
          .expectNext(item)
          .verifyComplete();

        del(Z_MARKET);
    }

    public void purchaseItem(long itemId, long sellerId, long buyerId, long lPrice) {
        int fundsOfBuyer = 120;

        if (lPrice > fundsOfBuyer) {
            throw new IllegalArgumentException();
        }

        String seller = Key01.v_USER(sellerId);
        String buyer = Key01.v_USER(buyerId);
        String bInventory = S_INVENTORY(buyerId);
        String item = itemId + "." + sellerId;

        StepVerifier.create(comm.zadd(Z_MARKET, lPrice, item))
          .expectNext(1L)
          .verifyComplete();

        //设置buyer拥有的金额
        StepVerifier.create(comm.hset(buyer, f_FUNDS, String.valueOf(fundsOfBuyer)))
          .expectNext(true)
          .verifyComplete();

        StepVerifier.create(purchaseItemSHA1.flatMap(sha1 -> comm.evalsha(sha1,
          ScriptOutputType.BOOLEAN,
          new String[]{Z_MARKET, buyer, seller, bInventory, f_FUNDS},
          item,
          String.valueOf(lPrice),
          String.valueOf(itemId))
          .single()))
          .expectNextMatches(o -> {
              System.out.println(o);
              return true;
          })
          .verifyComplete();

        StepVerifier.create(comm.zrange(Z_MARKET, 0, -1))
          .verifyComplete();

        StepVerifier.create(comm.hget(buyer, f_FUNDS))
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
