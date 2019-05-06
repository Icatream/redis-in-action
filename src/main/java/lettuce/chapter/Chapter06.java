package lettuce.chapter;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import static lettuce.key.ArticleKey.USER_PREFIX;
import static lettuce.key.C02Key.RECENT;
import static lettuce.key.C06Key.MEMBER;

/**
 * @author YaoXunYu
 * created on 05/05/19
 */
public class Chapter06 extends BaseChapter {

    private final Mono<String> addUpdateContactSHA1;
    private final Mono<String> autoCompleteOnPrefixSHA1;

    public Chapter06(RedisReactiveCommands<String, String> comm) {
        super(comm);
        addUpdateContactSHA1 = uploadScript("lua/AddUpdateContact.lua");
        autoCompleteOnPrefixSHA1 = uploadScript("lua/AutoCompleteOnPrefix.lua");
    }

    public Mono<Boolean> addUpdateContact(int userId, String contact) {
        return addUpdateContactSHA1.flatMap(sha1 -> comm.evalsha(sha1,
            ScriptOutputType.BOOLEAN,
            new String[]{getUserContactKey(userId)},
            contact)
            .single()
            .map(b -> (Boolean) b));
    }

    public Mono<Long> removeContact(int userId, String contact) {
        return comm.lrem(getUserContactKey(userId), 1, contact);
    }

    public Flux<String> fetchAutoCompleteList(int user, String prefix) {
        return comm.lrange(getUserContactKey(user), 0, -1)
            .filter(contact -> contact.startsWith(prefix));
    }

    private String getUserContactKey(int userId) {
        return RECENT + USER_PREFIX + userId;
    }

    private static final String VALID_CHARACTERS = "`abcdefghijklmnopqrstuvwxyz{";

    private String[] findPrefixRange(String prefix) {
        int posn = VALID_CHARACTERS.indexOf(prefix.charAt(prefix.length() - 1));
        char suffix = VALID_CHARACTERS.charAt(posn > 0 ? posn - 1 : 0);
        String start = prefix.substring(0, prefix.length() - 1) + suffix + "{";
        String end = prefix + "{";
        return new String[]{start, end};
    }

    @SuppressWarnings("unchecked")
    public Mono<List<String>> autoCompleteOnPrefix(int guild, String prefix, int limit) {
        String k = MEMBER + guild;
        String[] prefixRange = findPrefixRange(prefix);
        return autoCompleteOnPrefixSHA1.flatMap(sha1 ->
            comm.evalsha(sha1,
                ScriptOutputType.MULTI,
                new String[]{k, String.valueOf(limit)},
                prefixRange)
                .single()
                .map(o -> (List<String>) o));
    }

}
