package ria.lettuce.chapter;

import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import ria.lettuce.key.Key04;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * @author YaoXunYu
 * created on 04/12/2019
 */
public class Chapter04 extends BaseChapter {
    final Mono<String> listItemSHA1;
    final Mono<String> purchaseItemSHA1;

    public Chapter04(RedisReactiveCommands<String, String> comm) {
        super(comm);
        listItemSHA1 = uploadScript("lua/ListItem.lua");
        purchaseItemSHA1 = uploadScript("lua/PurchaseItem.lua");
    }

    //Warning: No error handler
    public Mono<Void> processLog(Path outerPath, Function<String, Mono<Void>> lineAnalyze) {
        return comm.get(Key04.s_FILE)
          .zipWith(comm.get(Key04.s_OFFSET)
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
        map.put(Key04.s_FILE, file.getFileName().toString());
        map.put(Key04.s_OFFSET, String.valueOf(offset));
        return comm.mset(map);
    }
}
