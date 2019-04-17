package lettuce.chapter;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static lettuce.key.ArticleKey.USER_PREFIX;
import static lettuce.key.C04Key.*;

/**
 * @author YaoXunYu
 * created on 04/12/2019
 */
public class Chapter04 extends BaseChapter {
    private final Mono<String> listItemSHA1;
    private final Mono<String> purchaseItemSHA1;

    public Chapter04(RedisReactiveCommands<String, String> comm) {
        super(comm);
        try {
            URL listItemLua = ClassLoader.getSystemResource("lua/ListItem.lua");
            String listItemScript = new String(Files.readAllBytes(Paths.get(listItemLua.toURI())));
            listItemSHA1 = comm.scriptLoad(listItemScript)
                .cache();
            URL purchaseItemLua = ClassLoader.getSystemResource("lua/PurchaseItem.lua");
            String purchaseItemScript = new String(Files.readAllBytes(Paths.get(purchaseItemLua.toURI())));
            purchaseItemSHA1 = comm.scriptLoad(purchaseItemScript)
                .cache();
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    //Warning: No error handler
    public Mono<Void> processLog(Path outerPath, Function<String, Mono<Void>> lineAnalyze) {
        return comm.get(CURRENT_FILE)
            .zipWith(comm.get(OFFSET)
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
        map.put(CURRENT_FILE, file.getFileName().toString());
        map.put(OFFSET, String.valueOf(offset));
        return comm.mset(map);
    }

    public void listItem(long itemId, long sellerId, long price) {
        String itemIdStr = String.valueOf(itemId);
        String inventory = INVENTORY + sellerId;

        StepVerifier.create(comm.sadd(inventory, itemIdStr))
            .expectNext(1L)
            .verifyComplete();

        String item = itemId + "." + sellerId;
        //long end = LocalDateTime.now().plusSeconds(5).atZone(ZoneOffset.systemDefault()).toEpochSecond();
        listItemSHA1.flatMap(sha -> comm.evalsha(sha,
            ScriptOutputType.BOOLEAN,
            new String[]{inventory, MARKET, item},
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

        StepVerifier.create(comm.zrange(MARKET, 0, -1))
            .expectNext(item)
            .verifyComplete();

        del(MARKET);
    }

    public void purchaseItem(long itemId, long sellerId, long buyerId, long lPrice) {
        int fundsOfBuyer = 120;

        if (lPrice > fundsOfBuyer) {
            throw new IllegalArgumentException();
        }

        String seller = USER_PREFIX + sellerId;
        String buyer = USER_PREFIX + buyerId;
        String bInventory = INVENTORY + buyerId;
        String item = itemId + "." + sellerId;

        StepVerifier.create(comm.zadd(MARKET, lPrice, item))
            .expectNext(1L)
            .verifyComplete();

        //设置buyer拥有的金额
        StepVerifier.create(comm.hset(buyer, FUNDS, String.valueOf(fundsOfBuyer)))
            .expectNext(true)
            .verifyComplete();

        StepVerifier.create(purchaseItemSHA1.flatMap(sha -> comm.evalsha(sha,
            ScriptOutputType.BOOLEAN,
            new String[]{MARKET, buyer, seller, bInventory, FUNDS},
            item,
            String.valueOf(lPrice),
            String.valueOf(itemId))
            .single()))
            .expectNextMatches(o -> {
                System.out.println(o);
                return true;
            })
            .verifyComplete();

        StepVerifier.create(comm.zrange(MARKET, 0, -1))
            .verifyComplete();

        StepVerifier.create(comm.hget(buyer, FUNDS))
            .expectNext(String.valueOf(fundsOfBuyer - lPrice))
            .verifyComplete();

        StepVerifier.create(comm.smembers(bInventory))
            .expectNext(String.valueOf(itemId))
            .verifyComplete();

        StepVerifier.create(comm.del(buyer, USER_PREFIX + sellerId, bInventory))
            .expectNext(3L)
            .verifyComplete();
    }
}
