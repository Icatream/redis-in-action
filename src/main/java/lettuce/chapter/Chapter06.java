package lettuce.chapter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lettuce.pojo.City;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.*;

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
    private final Mono<String> sendMessageSHA1;

    public Chapter06(RedisReactiveCommands<String, String> comm) {
        super(comm);
        addUpdateContactSHA1 = uploadScript("lua/AddUpdateContact.lua");
        autoCompleteOnPrefixSHA1 = uploadScript("lua/AutoCompleteOnPrefix.lua");
        sendMessageSHA1 = uploadScript("lua/SendMessage.lua");
    }

    public Mono<Boolean> addUpdateContact(int userId, String contact) {
        return addUpdateContactSHA1.flatMap(sha1 -> comm.evalsha(sha1,
            ScriptOutputType.BOOLEAN,
            new String[]{getUserContactKey(userId)},
            contact)
            .single()
            .cast(Boolean.class));
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

    /**
     * @return chatId
     * SEEN + recipients: <value>chatId</value>, <score>chat-message-id</score>
     */
    public Mono<Long> createChat(List<String> recipients) {
        return comm.incr(CHAT_ID)
            .flatMap(chatId -> createChat(recipients, chatId));
    }

    public Mono<Long> createChat(List<String> recipients, Long chatId) {
        return comm.zadd(CHAT + chatId,
            recipients.stream()
                .map(r -> ScoredValue.just(0, r))
                .toArray((IntFunction<ScoredValue<String>[]>) ScoredValue[]::new))
            .thenMany(Flux.fromIterable(recipients)
                .map(r -> SEEN + r)
                .flatMap(k -> comm.zadd(k, 0, chatId)))
            .then(Mono.just(chatId));
    }

    //将消息推入message zset, 存在顺序问题, 乱序会导致读消息时丢失. 改用lua
    public Mono<Long> sendMessage(Long chatId, Message message) {
        try {
            String msg = mapper.writeValueAsString(message);
            return sendMessageSHA1.flatMap(sha1 -> comm.evalsha(sha1,
                ScriptOutputType.INTEGER,
                new String[]{IDS + chatId, MSG + chatId},
                msg)
                .single()
                .cast(Long.class));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static class Message {
        private String sender;
        private String message;
        private long time;

        public Message() {
        }

        public Message(String sender, String message, long time) {
            this.sender = sender;
            this.message = message;
            this.time = time;
        }

        public String getSender() {
            return sender;
        }

        public void setSender(String sender) {
            this.sender = sender;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public long getTime() {
            return time;
        }

        public void setTime(long time) {
            this.time = time;
        }
    }

    /**
     * 当提取msg的客户端挂掉, msg在未处理前丢失
     * TODO 改用回调等形式, 在对msg操作完成后update redis
     *
     * @return chatId: List< Msg & mId >
     */
    public Flux<Tuple2<String, List<ScoredValue<String>>>> fetchPendingMessage(Integer recipient) {
        return comm.zrangeWithScores(SEEN + recipient, 0, -1)
            .flatMap(scoredValue -> {
                String chatId = scoredValue.getValue();
                double mid = scoredValue.getScore();
                //redis comm已排序
                return comm.zrangebyscoreWithScores(MSG + chatId, Range.<Double>unbounded().gt(mid))
                    .collectList()
                    //.collectSortedList(Comparator.comparingDouble(ScoredValue::getScore))
                    .flatMap(list -> {
                        //TODO 检查最大 or 最小
                        double lastMId = list.get(list.size() - 1).getScore();
                        //修改 chat:... 中 recipient 的 mid 为最大 mid
                        return comm.zadd(CHAT + chatId, lastMId, recipient)
                            //清理 chat:... 中 mid<lastMId
                            .then(comm.zremrangebyscore(MSG + chatId, Range.<Double>unbounded().lt(lastMId)))
                            .thenReturn(Tuples.of(chatId, list));
                    });
            });
    }

    public Mono<Long> joinChat(Integer chatId, Integer userId) {
        return comm.get(IDS + chatId)
            .flatMap(messageId -> {
                Integer mid = Integer.valueOf(messageId);
                return comm.zadd(CHAT + chatId, mid, userId)
                    .then(comm.zadd(SEEN + userId, mid, chatId));
            });
    }

    public Mono<Long> leaveChat(Integer chatId, Integer userId) {
        String c = chatId.toString();
        String u = userId.toString();
        String chat = CHAT + c;
        return comm.zrem(chat, u)
            .then(comm.zrem(SEEN + u, c))
            .then(comm.zcard(chat)
                .defaultIfEmpty(0L)
                .flatMap(size -> {
                    String msg = MSG + c;
                    if (size == 0) {
                        return comm.del(msg, IDS + c);
                    } else {
                        return comm.zrangeWithScores(chat, 0, 0)
                            .single()
                            .flatMap(sv -> comm.zremrangebyscore(msg, Range.<Double>unbounded().lt(sv.getScore())));
                    }
                }));
    }

    //day:country:counter
    private ConcurrentMap<LocalDate, ConcurrentMap<String, Integer>> aggregates = new ConcurrentHashMap<>();

    public Mono<String> dailyCountryAggregate(String ip, LocalDate day, Function<String, Mono<City>> findCityByIp) {
        return findCityByIp.apply(ip)
            .map(City::getCountry)
            .doOnNext(country -> {
                aggregates.putIfAbsent(day, new ConcurrentHashMap<>());
                aggregates.get(day)
                    .merge(country, 1, (ov, v) -> ov + v);
            });
    }

    /**
     * 多个日志处理客户端时,会相互覆写redis中country:counter.
     * 改用lua script,增加zadd-combine(array)操作
     */
    public Mono<Long> dailyCountryStorage(LocalDate day) {
        return Mono.justOrEmpty(aggregates.get(day))
            .map(map -> map.entrySet()
                .stream()
                .map(entry -> ScoredValue.just(entry.getValue(), entry.getKey()))
                .toArray((IntFunction<ScoredValue<String>[]>) ScoredValue[]::new))
            .flatMap(array -> comm.zadd(DAILY_COUNTRY + day, array))
            .doOnSuccess(l -> aggregates.remove(day));
    }

    /**
     * 6.6.2
     * 与6.6.1完全不同,之前是本地聚合计算,结果存入redis.
     * 这里要将整个日志文件写入redis???
     * 1份日志文件会被所有的recipient读取处理???
     */
    private Cache cache = new Cache();

    public void copyLogsToRedis(Path path, Long channel, int processorCount, long limit) {
        final String source = "Source";
        List<String> members = IntStream.rangeClosed(1, processorCount)
            .mapToObj(i -> "LogProcessor" + i)
            .collect(Collectors.toList());
        //add sender
        members.add(source);
        createChat(members, channel)
            .flatMapMany(cid -> Flux.using(() -> Files.walk(path, 1), Flux::fromStream, BaseStream::close)
                .filter(p -> Files.isRegularFile(p))
                .filter(p -> !p.equals(path))
                .flatMap(log -> Mono.fromCallable(() -> Files.size(log)).cache()
                    .filter(size -> cache.bytesInRedis.get() + size < limit)
                    //clean redis, until can put log file
                    .repeatWhenEmpty(f -> f
                        .delayElements(Duration.ofSeconds(1))
                        .flatMap(l -> cleanLogInRedis(cid, processorCount)
                            .thenReturn(l)))
                    //put log file by lines
                    .flatMap(size -> {
                        String logPath = log.getFileName().toString();
                        return Flux.using(() -> Files.lines(log),
                            Flux::fromStream,
                            BaseStream::close)
                            .map(line -> line + "\n")
                            .flatMap(line -> comm.append(cid + logPath, line))
                            //send log ready msg
                            .then(sendMessage(cid, new Message(source, logPath, ZonedDateTime.now().toEpochSecond()))
                                .doOnNext(l -> cache.offer(Tuples.of(log, size))));
                    }))
                .then(sendMessage(cid, new Message(source, PROCESS_FINISH_SUFFIX, ZonedDateTime.now().toEpochSecond())))
                //clean logs
                .thenMany(Mono.fromSupplier(() -> cache.bytesInRedis.get())
                    .repeat()
                    .delayElements(Duration.ofSeconds(1))
                    .takeWhile(size -> size > 0)
                    .flatMap(size -> cleanLogInRedis(cid, processorCount))))
            .subscribe();

    }

    public Mono<Long> cleanLogInRedis(Long chatId, int processorCount) {
        return Mono.justOrEmpty(cache.waiting.peek())
            .flatMap(tuple -> {
                String log = chatId + SEPARATOR + tuple.getT1().getFileName();
                String logProcessCount = log + PROCESS_FINISH_SUFFIX;
                return comm.get(logProcessCount)
                    .map(Integer::valueOf)
                    .filter(i -> i == processorCount)
                    .flatMap(i -> comm.del(log, logProcessCount))
                    .doOnNext(l -> cache.remove(tuple));
            });
    }

    private static class Cache {
        private AtomicLong bytesInRedis = new AtomicLong();
        private ConcurrentLinkedQueue<Tuple2<Path, Long>> waiting = new ConcurrentLinkedQueue<>();

        private void offer(Tuple2<Path, Long> tuple) {
            waiting.offer(tuple);
            bytesInRedis.accumulateAndGet(tuple.getT2(), (prev, n) -> prev + n);
        }

        private void remove(Tuple2<Path, Long> tuple) {
            waiting.remove(tuple);
            bytesInRedis.accumulateAndGet(tuple.getT2(), (prev, n) -> prev - n);
        }
    }

    public void processLogsFromRedis(Integer recipient, Function<String, Mono<Void>> callback) {
        /*fetchPendingMessage(recipient)
            .flatMap(tuple -> {
                Flux.fromIterable(tuple.getT2())
                    .map(ScoredValue::getValue)
                    .map(s -> mapper.convertValue(s, Message.class))
                    .filter(msg -> PROCESS_FINISH_SUFFIX.equals(msg.message))
                    .flatMap(msg -> {
                        String logPath = msg.getMessage();

                    })
            })*/
    }

    public BiFunction<Tuple2<Stream<String>, String>, String, Tuple2<Stream<String>, String>> accumulator = (tuple, str) -> {
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
    private BiFunction<String, RedisReactiveCommands<String, String>, Flux<String>> readLines = (key, comm) -> {
        long blockSize = 2 ^ 17;
        AtomicLong pos = new AtomicLong();
        return Mono.fromSupplier(() -> pos.getAndAccumulate(blockSize, (pv, v) -> pv + v))
            .flatMap(p -> comm.getrange(key, p, p + blockSize - 1))
            .repeat()
            .takeWhile(s -> !"".equals(s))
            .scan(Tuples.of(Stream.empty(), ""), accumulator)
            .flatMap(tuple -> Flux.fromStream(tuple.getT1()));
    };

    private Stream<String> createStringStream(String string) {
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

    /*private BiFunction<String, RedisReactiveCommands<String, String>, Void> readBlocksGZ = (key, comm) -> {

    }*/

}
