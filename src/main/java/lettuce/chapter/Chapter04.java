package lettuce.chapter;

import lettuce.key.ArticleKey;
import lettuce.key.C04Key;
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
        return comm.get(C04Key.CURRENT_FILE)
            .zipWith(comm.get(C04Key.OFFSET)
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
        map.put(C04Key.CURRENT_FILE, file.getFileName().toString());
        map.put(C04Key.OFFSET, String.valueOf(offset));
        return comm.mset(map);
    }

    public void listItem(long itemId, long sellerId, long price) {
        String itemIdStr = String.valueOf(itemId);
        String inventory = C04Key.INVENTORY + sellerId;

        StepVerifier.create(comm.sadd(inventory, itemIdStr))
            .expectNext(1L)
            .verifyComplete();

        String item = itemId + "." + sellerId;
        //long end = LocalDateTime.now().plusSeconds(5).atZone(ZoneOffset.systemDefault()).toEpochSecond();
        listItemSHA1.flatMap(sha -> comm.evalsha(sha,
            ScriptOutputType.BOOLEAN,
            new String[]{inventory, C04Key.MARKET, item},
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

        StepVerifier.create(comm.zrange(C04Key.MARKET, 0, -1))
            .expectNext(item)
            .verifyComplete();

        del(C04Key.MARKET);
    }

    public void purchaseItem(long itemId, long sellerId, long buyerId, long lPrice) {
        int fundsOfBuyer = 120;

        if (lPrice > fundsOfBuyer) {
            throw new IllegalArgumentException();
        }

        String seller = ArticleKey.USER_PREFIX + sellerId;
        String buyer = ArticleKey.USER_PREFIX + buyerId;
        String bInventory = C04Key.INVENTORY + buyerId;
        String item = itemId + "." + sellerId;

        StepVerifier.create(comm.zadd(C04Key.MARKET, lPrice, item))
            .expectNext(1L)
            .verifyComplete();

        //设置buyer拥有的金额
        StepVerifier.create(comm.hset(buyer, C04Key.FUNDS, String.valueOf(fundsOfBuyer)))
            .expectNext(true)
            .verifyComplete();

        StepVerifier.create(purchaseItemSHA1.flatMap(sha -> comm.evalsha(sha,
            ScriptOutputType.BOOLEAN,
            new String[]{C04Key.MARKET, buyer, seller, bInventory, C04Key.FUNDS},
            item,
            String.valueOf(lPrice),
            String.valueOf(itemId))
            .single()))
            .expectNextMatches(o -> {
                System.out.println(o);
                return true;
            })
            .verifyComplete();

        StepVerifier.create(comm.zrange(C04Key.MARKET, 0, -1))
            .verifyComplete();

        StepVerifier.create(comm.hget(buyer, C04Key.FUNDS))
            .expectNext(String.valueOf(fundsOfBuyer - lPrice))
            .verifyComplete();

        StepVerifier.create(comm.smembers(bInventory))
            .expectNext(String.valueOf(itemId))
            .verifyComplete();

        StepVerifier.create(comm.del(buyer, ArticleKey.USER_PREFIX + sellerId, bInventory))
            .expectNext(3L)
            .verifyComplete();
    }
}
