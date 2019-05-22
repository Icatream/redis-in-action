package lettuce.chapter;

import com.google.common.collect.Sets;
import io.lettuce.core.SortArgs;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lettuce.key.Key07;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static lettuce.key.Key07.S_IDX;

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
            .flatMap(word -> comm.sadd(S_IDX(word), docId.toString()));
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
        String id = S_IDX(UUID.randomUUID().toString());
        return collectOps.apply(id)
            .then(comm.expire(id, 30))
            .thenReturn(id);
    }

    private Mono<String> collectAndExpire(Function<String, Mono<Long>> collectOps, long timeout) {
        String id = S_IDX(UUID.randomUUID().toString());
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

    public Flux<String> searchAndSort(String query, SortArgs sortArgs, long timeout) {
        return parseAndSearch(query, timeout)
            .flatMapMany(id -> comm.sort(id, sortArgs));
    }

    public Mono<SortResult> searchAndSort(String query, SortArgs sortArgs, long timeout, String idx) {
        return comm.expire(idx, timeout)
            .filter(b -> b)
            .map(b -> idx)
            .switchIfEmpty(parseAndSearch(query, timeout))
            .flatMap(id -> Mono.zip(comm.scard(id),
                comm.sort(id, sortArgs).collectList())
                .map(tuple -> {
                    SortResult r = new SortResult();
                    r.id = id;
                    r.size = tuple.getT1();
                    r.values = tuple.getT2();
                    return r;
                }));
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

}
