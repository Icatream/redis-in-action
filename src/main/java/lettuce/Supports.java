package lettuce;

import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author YaoXunYu
 * created on 05/17/19
 */
public class Supports {

    /**
     * ignore sub-string after the last '\n'
     */
    public static Stream<String> createStringStream(String string) {
        Iterator<String> iterator = new Iterator<String>() {
            private final String s = string;
            private int i;

            @Override
            public boolean hasNext() {
                return s.indexOf("\n", i) >= 0;
            }

            @Override
            public String next() {
                int n = s.indexOf("\n", i) + 1;
                String sub = s.substring(i, n);
                i = n;
                return sub;
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
          Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE),
          false);
    }

    /**
     * change {@code String} into {@code Stream<String>}, split by '\n',
     * store the half-line at the {@code T2} in {@code Tuple2}
     */
    public static final BiFunction<Tuple2<Stream<String>, String>, String, Tuple2<Stream<String>, String>> accumulator = (tuple, str) -> {
        String s = tuple.getT2() + str;
        int index = s.lastIndexOf("\n") + 1;
        if (index == 0) {
            return Tuples.of(Stream.empty(), s);
        } else if (index == s.length()) {
            return Tuples.of(createStringStream(s), "");
        } else {
            return Tuples.of(createStringStream(s.substring(0, index)), s.substring(index));
        }
    };
}
