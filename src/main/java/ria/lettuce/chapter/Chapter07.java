package ria.lettuce.chapter;

import com.google.common.collect.Sets;
import io.lettuce.core.Range;
import io.lettuce.core.SortArgs;
import io.lettuce.core.ZStoreArgs;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import ria.lettuce.enums.Ecpm;
import ria.lettuce.key.Key07;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author YaoXunYu
 * created on 05/20/19
 */
public class Chapter07 extends BaseChapter {

    private final HashSet<String> STOP_WORDS = Sets.newHashSet("able", "about",
      "across", "after", "all", "almost", "also", "am", "among", "an", "and", "any",
      "are", "as", "at", "be", "because", "been", "but", "by", "can", "cannot",
      "could", "dear", "did", "do", "does", "either", "else", "ever", "every",
      "for", "from", "get", "got", "had", "has", "have", "he", "her", "hers",
      "him", "his", "how", "however", "if", "in", "into", "is", "it", "its",
      "just", "least", "let", "like", "likely", "may", "me", "might", "most",
      "must", "my", "neither", "no", "nor", "not", "of", "off", "often", "on",
      "only", "or", "other", "our", "own", "rather", "said", "say", "says", "she",
      "should", "since", "so", "some", "than", "that", "the", "their", "them",
      "then", "there", "these", "they", "this", "tis", "to", "too", "twas", "us",
      "wants", "was", "we", "were", "what", "when", "where", "which", "while",
      "who", "whom", "why", "will", "with", "would", "yet", "you", "your");

    private final Pattern WORDS_RE = Pattern.compile("[a-z']{2,}");
    private final Pattern QUERY_RE = Pattern.compile("[+-]?[a-z']{2,}");

    public Chapter07(RedisReactiveCommands<String, String> comm) {
        super(comm);
    }

    /**
     * TODO strip("'")
     */
    public Stream<String> tokenize(String content) {
        Iterator<String> iterator = new Iterator<String>() {
            private Matcher matcher = WORDS_RE.matcher(content.toLowerCase());

            @Override
            public boolean hasNext() {
                return matcher.find();
            }

            @Override
            public String next() {
                return matcher.group();
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
          Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE),
          false)
          .filter(s -> !STOP_WORDS.contains(s));
    }

    public Flux<Long> indexDocument(Integer docId, String content) {
        return Flux.fromStream(tokenize(content))
          .flatMap(word -> comm.sadd(Key07.S_IDX(word), docId.toString()));
    }

    public Mono<String> intersect(String[] keys) {
        return collectAndExpire(id -> comm.sinterstore(id, keys));
    }

    public Mono<String> intersect(String[] keys, long timeout) {
        return collectAndExpire(id -> comm.sinterstore(id, keys), timeout);
    }

    public Mono<String> union(String[] keys) {
        return collectAndExpire(id -> comm.sunionstore(id, keys));
    }

    public Mono<String> union(String[] keys, long timeout) {
        return collectAndExpire(id -> comm.sunionstore(id, keys), timeout);

    }

    public Mono<String> difference(String[] keys) {
        return collectAndExpire(id -> comm.sdiffstore(id, keys));
    }

    public Mono<String> difference(String[] keys, long timeout) {
        return collectAndExpire(id -> comm.sdiffstore(id, keys), timeout);
    }

    private Mono<String> collectAndExpire(Function<String, Mono<Long>> collectOps) {
        String id = Key07.S_IDX(UUID.randomUUID().toString());
        return collectOps.apply(id)
          .then(comm.expire(id, 30))
          .thenReturn(id);
    }

    private Mono<String> collectAndExpire(Function<String, Mono<Long>> collectOps, long timeout) {
        String id = Key07.S_IDX(UUID.randomUUID().toString());
        return collectOps.apply(id)
          .then(comm.expire(id, timeout))
          .thenReturn(id);
    }

    /**
     * TODO strip("'")
     *
     * @return t1: add, t2 unwanted
     */
    private Tuple2<List<Set<String>>, Set<String>> parse(String query) {
        List<Set<String>> all = new ArrayList<>();
        Set<String> unwanted = new HashSet<>();
        Set<String> current = new HashSet<>();
        Matcher matcher = QUERY_RE.matcher(query.toLowerCase());
        while (matcher.find()) {
            String word = matcher.group();
            String prefix = word.substring(0, 1);
            boolean add = "+".equals(prefix);
            boolean subtract = "-".equals(prefix);
            if (add || subtract) {
                word = word.substring(1);
            }
            if (word.length() < 2 || STOP_WORDS.contains(word)) {
                continue;
            }
            if (subtract) {
                unwanted.add(word);
                continue;
            }
            if (!current.isEmpty() && !add) {
                all.add(current);
                current = new HashSet<>();
            }
            current.add(word);

        }
        if (!current.isEmpty()) {
            all.add(current);
        }
        return Tuples.of(all, unwanted);
    }

    public Mono<String> parseAndSearch(String query, long timeout) {
        return Mono.just(parse(query))
          .filter(tuple -> !tuple.getT1().isEmpty())
          .flatMap(tuple -> {
              Mono<String> mono = Flux.fromIterable(tuple.getT1())
                .map(set -> set.stream()
                  .map(Key07::S_IDX)
                  .toArray(String[]::new))
                .flatMap(arr -> union(arr, timeout))
                .collectList()
                .flatMap(list -> intersect(list.toArray(new String[0]), timeout));
              if (tuple.getT2().isEmpty()) {
                  return mono;
              } else {
                  return mono.flatMap(id -> difference(
                    Stream.concat(Stream.of(id),
                      tuple.getT2()
                        .stream()
                        .map(Key07::S_IDX))
                      .toArray(String[]::new),
                    timeout));
              }
          });
    }

    public Mono<SortResult> searchAndSort(String query, SortArgs sortArgs, long timeout) {
        return parseAndSearch(query, timeout)
          .flatMap(id -> getSortResult(id, sortArgs));
    }

    public Mono<SortResult> searchAndSort(String query, SortArgs sortArgs, long timeout, String idx) {
        return comm.expire(idx, timeout)
          .filter(b -> b)
          .map(b -> idx)
          .switchIfEmpty(parseAndSearch(query, timeout))
          .flatMap(id -> getSortResult(id, sortArgs));
    }

    private Mono<SortResult> getSortResult(String id, SortArgs sortArgs) {
        return Mono.zip(comm.scard(id),
          comm.sort(id, sortArgs).collectList())
          .map(tuple -> {
              SortResult r = new SortResult();
              r.id = id;
              r.size = tuple.getT1();
              r.values = tuple.getT2();
              return r;
          });
    }

    private static class SortResult {
        String id;
        Long size;
        List<String> values;

        public String getId() {
            return id;
        }

        public Long getSize() {
            return size;
        }

        public List<String> getValues() {
            return values;
        }
    }

    /**
     * default ZStoreArgs is sum
     */
    public Mono<String> zintersect(String[] keys) {
        return collectAndExpire(id -> comm.zinterstore(id, keys));
    }

    public Mono<String> zintersect(String[] keys, ZStoreArgs args) {
        return collectAndExpire(id -> comm.zinterstore(id, args, keys), 30);
    }

    public Mono<String> zintersect(String[] keys, ZStoreArgs args, long timeout) {
        return collectAndExpire(id -> comm.zinterstore(id, args, keys), timeout);
    }

    /**
     * default ZStoreArgs is sum
     */
    public Mono<String> zunion(String[] keys) {
        return collectAndExpire(id -> comm.zunionstore(id, keys));
    }

    public Mono<String> zunion(String[] keys, ZStoreArgs args) {
        return collectAndExpire(id -> comm.zunionstore(id, args, keys), 30);
    }

    public Mono<String> zunion(String[] keys, ZStoreArgs args, long timeout) {
        return collectAndExpire(id -> comm.zunionstore(id, args, keys), timeout);
    }

    public Mono<SortResult> searchAndZSort(String query, double update, double vote, long start, long end, boolean desc, long timeout) {
        return parseAndSearch(query, timeout)
          .flatMap(id -> {
              String[] keys = new String[]{id, Key07.Z_SORT_UPDATE, Key07.Z_SORT_VOTES};
              ZStoreArgs args = ZStoreArgs.Builder.weights(0, update, vote);
              return zintersect(keys, args, timeout);
          })
          .flatMap(id -> getZSortResult(id, desc, start, end));
    }

    public Mono<SortResult> searchAndZSort(String query, double update, double vote, long start, long end, boolean desc, long timeout, String idx) {
        return comm.expire(idx, timeout)
          .filter(b -> b)
          .flatMap(b -> getZSortResult(idx, desc, start, end))
          .switchIfEmpty(searchAndZSort(query, update, vote, start, end, desc, timeout));
    }

    private Mono<SortResult> getZSortResult(String id, boolean desc, long start, long end) {
        return comm.zcard(id)
          .flatMap(size -> {
              Flux<String> f;
              if (desc) {
                  f = comm.zrevrange(id, start, end);
              } else {
                  f = comm.zrange(id, start, end);
              }
              return f.collectList()
                .map(list -> {
                    SortResult r = new SortResult();
                    r.id = id;
                    r.size = size;
                    r.values = list;
                    return r;
                });
          });
    }

    public long stringToScore(String string, boolean ignoreCase) {
        if (ignoreCase) {
            string = string.toLowerCase();
        }
        List<Integer> pieces = new ArrayList<>();
        for (int i = 0; i < Math.min(string.length(), 6); i++) {
            pieces.add((int) string.charAt(i));
        }
        while (pieces.size() < 6) {
            pieces.add(-1);
        }

        long score = 0;
        for (int piece : pieces) {
            score = score * 257 + piece + 1;
        }

        return score * 2 + (string.length() > 6 ? 1 : 0);
    }

    /*
     * 7.3
     * CPM: cost per mille
     * CPC: cost per click
     * eCPC: estimated CPM
     */

    // clicks/views 点击通过率
    public double cpcToEcpm(long views, long clicks, double cpc) {
        return 1000 * cpc * clicks / views;
    }

    // actions/views 动作执行概率
    public double cpaToEcpm(long views, long actions, double cpa) {
        return 1000 * cpa * actions / views;
    }

    private final ConcurrentMap<Ecpm, Double> AVERAGE_PER_1K = new ConcurrentHashMap<>();

    public Mono<Void> indexAd(String id, Stream<String> locations, String content, Ecpm type, double value) {
        String[] words = tokenize(content).toArray(String[]::new);
        return Flux.fromStream(locations)
          .map(Key07::S_IDX_REQ)
          //index ad locations
          .flatMap(k -> comm.sadd(k, id))
          .concatWith(Flux.fromArray(words)
            .map(Key07::ZS_IDX)
            //index add words
            .flatMap(k -> comm.zadd(k, 0, id)))
          .concatWith(comm.hset(Key07.H_TYPE, id, type.name())
            .thenReturn(1L))
          //record ad value
          .concatWith(comm.zadd(Key07.Z_IDX_AD_VALUE,
            type.val(1000, AVERAGE_PER_1K.getOrDefault(type, 1d), value),
            id))
          //add base value
          .concatWith(comm.zadd(Key07.Z_AD_BASE_VALUE, value, id))
          .concatWith(comm.sadd(Key07.S_TERMS(id), words))
          .then();
    }

    /**
     * @return t1: adId, t2:targetId
     */
    public Mono<Tuple2<String, Long>> targetAds(String content, Stream<String> locations) {
        Set<String> words = tokenize(content).collect(Collectors.toSet());
        return matchLocation(locations)
          .flatMap(tuple -> finishScoring(tuple.getT1(), tuple.getT2(), words))
          .flatMap(targetedAds -> comm.zrevrange(targetedAds, 0, 0)
            .singleOrEmpty()
            .zipWith(comm.incr(Key07.ADS_SERVED))
            .flatMap(tuple -> recordTargetingResult(tuple.getT1(), tuple.getT2(), words)
              .then(Mono.just(tuple))));
    }

    /**
     * TODO bug? idx:req:{location}? idx:ad:value:{location}?
     *
     * @return t1: ads_id, t2: ad_values_id
     */
    public Mono<Tuple2<String, String>> matchLocation(Stream<String> locations) {
        return union(locations.map(Key07::S_IDX_REQ).toArray(String[]::new), 300)
          .flatMap(id -> zintersect(new String[]{id, Key07.Z_IDX_AD_VALUE}, ZStoreArgs.Builder.weights(0, 1), 300)
            .map(baseEcpm -> Tuples.of(id, baseEcpm)));
    }

    public Mono<String> finishScoring(String matched, String baseEcpm, Set<String> words) {
        return Flux.fromIterable(words)
          .flatMap(word -> zintersect(new String[]{matched, word}, ZStoreArgs.Builder.weights(0, 1)))
          .collectList()
          .filter(list -> list.size() > 0)
          .flatMap(bonusEcpm -> {
              String[] arr = bonusEcpm.toArray(new String[0]);
              return Mono.zip(zunion(arr, ZStoreArgs.Builder.min()),
                zunion(arr, ZStoreArgs.Builder.max()))
                .flatMap(tuple -> zunion(new String[]{baseEcpm, tuple.getT1(), tuple.getT2()},
                  ZStoreArgs.Builder.weights(1, 0.5, 0.5)));
          })
          .defaultIfEmpty(baseEcpm);
    }

    public Flux<Long> recordTargetingResult(String adId, Long targetId, Set<String> words) {
        String adViews = Key07.Z_VIEWS(adId);
        return comm.hget(Key07.H_TYPE, adId)
          .map(Key07::TYPE_VIEWS)
          .flatMap(comm::incr)
          .then(comm.smembers(Key07.S_TERMS(adId))
            .filter(words::contains)
            .flatMap(word -> comm.zincrby(adViews, 1, word)
              .thenReturn(word))
            .collectList()
            .filter(list -> list.size() > 0)
            .flatMap(matched -> {
                String matchedKey = Key07.S_MATCHED(targetId);
                return comm.sadd(matchedKey, matched.toArray(new String[0]))
                  .then(comm.expire(matchedKey, 900));
            }))
          .thenMany(comm.zincrby(adViews, 1, "")
            .filter(l -> (l % 100) == 0)
            .flatMapMany(l -> updateCpms(adId)));
    }

    /**
     * 广告每次被点击,都会增加该广告的匹配单词计数,如果一个广告匹配单词较多,会有较多单词因此受益.
     * incr value 改为 1/count(words)
     */
    public Flux<Long> recordClick(String adId, Long targetId, boolean act) {
        String matchKey = Key07.S_MATCHED(targetId);
        return comm.hget(Key07.H_TYPE, adId)
          .flatMap(type -> {
              Mono<Boolean> m = Mono.just(true);
              if (Ecpm.CPA.nameEqual(type)) {
                  m = comm.expire(matchKey, 900);
                  if (act) {
                      return m.then(comm.incr(Key07.TYPE_ACTIONS(type)))
                        .thenReturn(Key07.Z_ACTIONS(adId));
                  }
              }
              return m.then(comm.incr(Key07.TYPE_CLICKS(type)))
                .thenReturn(Key07.Z_CLICKS(adId));
          })
          .flatMapMany(clickKey -> comm.smembers(matchKey)
            .concatWithValues("")
            .flatMap(word -> comm.zincrby(clickKey, 1, word)))
          .thenMany(updateCpms(adId));
    }

    public Flux<Long> updateCpms(String adId) {
        return comm.hget(Key07.H_TYPE, adId)
          .map(Ecpm::valueOf)
          .flatMapMany(type -> {
              String typeKey;
              String clickKey;
              String tName = type.name();
              if (Ecpm.CPA.equals(type)) {
                  typeKey = Key07.TYPE_ACTIONS(tName);
                  clickKey = Key07.Z_ACTIONS(adId);
              } else {
                  typeKey = Key07.TYPE_CLICKS(tName);
                  clickKey = Key07.Z_CLICKS(adId);
              }
              return Mono.zip(comm.get(Key07.TYPE_VIEWS(tName))
                  .map(Integer::valueOf)
                  .defaultIfEmpty(1),
                comm.get(typeKey)
                  .map(Integer::valueOf)
                  .defaultIfEmpty(1))
                .doOnNext(tuple -> AVERAGE_PER_1K.put(type, 1000 * (double) tuple.getT2() / tuple.getT1()))
                .filter(tuple -> !Ecpm.CPM.equals(type))
                .flatMap(t -> comm.zscore(Key07.Z_AD_BASE_VALUE, adId))
                .flatMapMany(baseValue -> {
                    String viewKey = Key07.Z_VIEWS(adId);
                    return Mono.zip(comm.zscore(viewKey, "")
                        .defaultIfEmpty(1d),
                      comm.zscore(clickKey, "")
                        .defaultIfEmpty(0d))
                      .flatMap(tuple -> {
                          if (tuple.getT2() < 1) {
                              return comm.zscore(Key07.Z_IDX_AD_VALUE, adId);
                          } else {
                              double adEcpm = type.val(tuple.getT1(), tuple.getT2(), baseValue);
                              return comm.zadd(Key07.Z_IDX_AD_VALUE, adEcpm, adId)
                                .thenReturn(adEcpm);
                          }
                      })
                      .flatMapMany(adEcpm -> comm.smembers(Key07.S_TERMS(adId))
                        //zscore改成zrange,本地处理再写入,降低通信次数
                        .flatMap(word -> comm.zscore(clickKey, word)
                          .defaultIfEmpty(0d)
                          .filter(clicks -> clicks >= 1)
                          .flatMap(clicks -> comm.zscore(viewKey, word)
                            .defaultIfEmpty(1d)
                            .map(views -> type.val(views, clicks, baseValue))
                            .map(wordEcpm -> wordEcpm - adEcpm)
                            .flatMap(bonus -> comm.zadd(Key07.ZS_IDX(word), bonus, adId)))));
                });
          });
    }

    /*
     * 7.4
     */

    public Mono<Long> addJob(String jobId, String[] requiredSkills) {
        return comm.sadd(Key07.S_JOB(jobId), requiredSkills);
    }

    public Flux<String> isQualified(String jobId, String[] candidateSkills) {
        String temp = UUID.randomUUID().toString();
        return comm.sadd(temp, candidateSkills)
          .then(comm.expire(temp, 5))
          .thenMany(comm.sdiff(Key07.S_JOB(jobId), temp));
    }

    /**
     * @param skills size > 0
     */
    public Flux<Long> indexJob(String jobId, Set<String> skills) {
        return Flux.fromIterable(skills)
          .map(Key07::S_IDX_SKILL)
          .flatMap(k -> comm.sadd(k, jobId))
          .concatWith(comm.zadd(Key07.Z_IDX_JOB_REQ, skills.size(), jobId));
    }

    /**
     * 技能熟练度: 修改求职者技能分值
     * 技能经验: 将求职者技能开始时间作为分值??
     */
    public Flux<String> findJob(Stream<String> candidateSkills) {
        return zunion(candidateSkills.map(Key07::Z_SKILL).toArray(String[]::new))
          .flatMap(jobScores -> zintersect(new String[]{jobScores, Key07.Z_JOB_REQ}, ZStoreArgs.Builder.weights(-1, 1)))
          .flatMapMany(k -> comm.zrangebyscore(k, Range.create(0, 0)));
    }
}
