package lettuce.chapter;

import com.google.common.collect.Sets;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static lettuce.key.Key07.*;

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

    private static final Pattern WORDS_RE = Pattern.compile("[a-z']{2,}");

    public Chapter07(RedisReactiveCommands<String, String> comm) {
        super(comm);
    }

    public Stream<String> tokenize(String content) {
        Iterator<String> iterator = new Iterator<String>() {
            private Matcher matcher = WORDS_RE.matcher(content.toLowerCase());

            @Override
            public boolean hasNext() {
                return matcher.find();
            }

            @Override
            public String next() {
                return matcher.group().trim();
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
            Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE),
            false)
            .filter(s -> s.length() > 2)
            .filter(s -> !STOP_WORDS.contains(s));
    }

    public Flux<Long> indexDocument(Integer docId, String content) {
        return Flux.fromStream(tokenize(content))
            .flatMap(word -> comm.sadd(S_IDX(word), docId.toString()));
    }

    public Mono intersect(String[] keys, long timeout) {
        String id = UUID.randomUUID().toString();
        return comm.sinterstore(id, keys)
            .then(comm.expire(id, timeout))
            .thenReturn(id);
    }
}
