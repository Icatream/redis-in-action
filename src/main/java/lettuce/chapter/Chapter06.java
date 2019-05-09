package lettuce.chapter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static lettuce.key.ArticleKey.USER_PREFIX;
import static lettuce.key.C02Key.RECENT;
import static lettuce.key.C06Key.*;

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
    public Mono<List<String>> autoCompleteOnPrefix(int guildId, String prefix, int limit) {
        String k = MEMBER + guildId;
        String[] prefixRange = findPrefixRange(prefix);
        return autoCompleteOnPrefixSHA1.flatMap(sha1 ->
            comm.evalsha(sha1,
                ScriptOutputType.MULTI,
                new String[]{k, String.valueOf(limit)},
                prefixRange)
                .single()
                .map(o -> (List<String>) o));
    }

    public Mono<Long> joinGuild(int guildId, String user) {
        return comm.zadd(MEMBER + guildId, 0, user);
    }

    public Mono<Long> leaveGuild(int guildId, String user) {
        return comm.zrem(MEMBER + guildId, user);
    }

    public void processSoldEmailQueue() {
        Flux.interval(Duration.ofSeconds(5))
            .flatMap(i -> comm.lpop(EMAIL_QUEUE))
            .flatMap(this::fakeProcessEmail)
            .subscribe();
    }

    private Mono<String> fakeProcessEmail(String email) {
        System.out.println(email);
        return Mono.just("ok");
    }

    //there're nothing in callbackMap...
    private Map<String, Function<List<String>, Mono<Object>>> callbackMap = new HashMap<>();
    private ObjectMapper mapper = new ObjectMapper();

    public void workerWatchQueue(String queue) {
        Flux.interval(Duration.ofSeconds(5))
            .flatMap(i -> comm.lpop(queue))
            .transform(callbackFlux)
            .subscribe();
    }

    public void workerWatchQueues(List<String> queue) {
        Flux.interval(Duration.ofSeconds(5))
            .flatMap(l -> {
                AtomicInteger index = new AtomicInteger();
                return Mono.fromSupplier(index::get)
                    .flatMap(i -> comm.lpop(queue.get(i)))
                    .repeatWhenEmpty(queue.size() - 1,
                        f -> f.doOnNext(i -> index.incrementAndGet()))
                    .onErrorResume(throwable -> Mono.empty());
            })
            .transform(callbackFlux)
            .subscribe();
    }

    private Function<Flux<String>, Flux<Object>> callbackFlux = f -> f
        .map(json -> mapper.convertValue(json, Callback.class))
        .onErrorContinue(error -> true,
            (throwable, o) -> {
                System.out.println(o);
                throwable.printStackTrace();
            })
        .flatMap(callback -> {
            Function<List<String>, Mono<Object>> callBack = callbackMap.get(callback.name);
            if (callBack != null) {
                return callBack.apply(callback.args);
            }
            System.out.println("Unknown callback: " + callback.name);
            return Mono.empty();
        });

    public static class Callback {
        private String id;
        private String queue;
        private String name;
        private List<String> args;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getQueue() {
            return queue;
        }

        public void setQueue(String queue) {
            this.queue = queue;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<String> getArgs() {
            return args;
        }

        public void setArgs(List<String> args) {
            this.args = args;
        }
    }

    public Mono<String> executeLater(Callback callback, long delay) {
        UUID uuid = UUID.randomUUID();
        callback.setId(uuid.toString());
        try {
            String json = mapper.writeValueAsString(callback);
            Mono<Long> mono;
            if (delay > 0) {
                mono = comm.zadd(DELAYED,
                    ZAddArgs.Builder.nx(),
                    LocalDateTime.now().plusSeconds(delay).atZone(ZoneOffset.systemDefault()).toEpochSecond(),
                    json);
            } else {
                mono = comm.rpush(QUEUE + callback.getQueue(), json);
            }
            return mono.thenReturn(uuid.toString());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public Mono<Boolean> acquireLockWithTimeout(String lockName, String lockId, long expireSeconds) {
        return comm.setnx(lockName, lockId)
            .filter(b -> b)
            .flatMap(b -> comm.expire(lockName, expireSeconds));
    }

    public Mono<Boolean> releaseLock(String lockName, String id) {
        return comm.get(lockName)
            .flatMap(lockId -> {
                if (lockId.equals(id)) {
                    return comm.del(lockName)
                        .thenReturn(true);
                }
                return Mono.just(false);
            });
    }

    public void pollQueue() {
        Flux.interval(Duration.ofSeconds(5))
            .flatMap(l -> comm.zrangeWithScores(DELAYED, 0, 0)
                .single())
            .flatMap(scoreValue -> {
                long now = LocalDateTime.now().atZone(ZoneOffset.systemDefault()).toEpochSecond();
                if (now < scoreValue.getScore()) {
                    return Mono.empty();
                }
                String json = scoreValue.getValue();
                Callback c = mapper.convertValue(json, Callback.class);
                String lockName = LOCK + c.id;
                String lockId = UUID.randomUUID().toString();
                return acquireLockWithTimeout(lockName, lockId, 60)
                    .flatMap(b -> comm.zrem(DELAYED, json)
                        .filter(l -> l == 1)
                        .flatMap(l -> comm.rpush(QUEUE + c.getQueue(), json))
                        .flatMap(l -> releaseLock(lockName, lockId))
                        .doOnNext(release -> {
                            if (!release) {
                                System.out.println("Release lock fail: LockName<" + lockName + "> LockId<" + lockId + ">");
                            }
                        }));
            })
            .subscribe();
    }
}
