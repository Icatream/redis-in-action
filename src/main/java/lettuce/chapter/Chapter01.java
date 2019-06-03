package lettuce.chapter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import io.lettuce.core.ZStoreArgs;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import lettuce.pojo.Article;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static lettuce.key.Key01.*;

/**
 * @author YaoXunYu
 * created on 04/08/2019
 */
public class Chapter01 extends BaseChapter {

    private final static int VOTE_SCORE = 432;
    private final static int ARTICLE_PER_PAGE = 25;
    private final ObjectMapper mapper = new ObjectMapper();

    public Chapter01(RedisReactiveCommands<String, String> comm) {
        super(comm);
    }

    /**
     * 投票,评分, 暂无事物
     */
    public Mono<Long> articleVote(long articleId, long userId) {
        long cutOff = LocalDateTime.now()
          .minusWeeks(1).atZone(ZoneOffset.systemDefault()).toEpochSecond();
        String a = v_ARTICLE(articleId);
        return comm.zscore(ZS_TIME, a)
          .filter(d -> d >= cutOff)
          .then(comm.sadd(S_VOTED(articleId), v_USER(userId))
            .filter(i -> i == 1)
            .then(comm.zincrby(ZS_SCORE, VOTE_SCORE, a)
              .then(comm.hincrby(a, f_VOTES, 1))));
    }

    /**
     * redis incr获取文章ID,
     * 添加投票, 和投票超时设置,
     * 添加 文章 hash
     * 添加分值,
     * 添加时间
     */
    public Mono<Long> postArticle(Article article) {
        return comm.incr(ARTICLE)
          .flatMap(id -> {
              article.setId(id);
              long now = Instant.now().getEpochSecond();
              article.setTime(now);
              String voted = S_VOTED(id);
              String a = v_ARTICLE(id);
              return comm.sadd(voted, v_USER(article.getPosterId()))
                .then(comm.expire(voted, Duration.ofDays(7).getSeconds()))
                .then(putArticle(article))
                .then(comm.zadd(ZS_SCORE, now + VOTE_SCORE, a))
                .then(comm.zadd(ZS_TIME, now, a))
                .thenReturn(id);
          });
    }

    public Flux<Article> getArticles(int page) {
        return getArticles(page, ZS_SCORE);
    }

    public Flux<Article> getArticles(int page, String order) {
        long start = (page - 1) * ARTICLE_PER_PAGE;
        long end = start + ARTICLE_PER_PAGE - 1;
        return comm.zrevrange(order, start, end)
          .flatMap(comm::hgetall)
          .map(map -> mapper.convertValue(map, Article.class));
    }

    public Flux<Long> changeGroup(long articleId, List<Long> addGroupIds, List<Long> removeGroupIds) {
        String a = v_ARTICLE(articleId);
        return Flux.fromIterable(addGroupIds)
          .flatMap(groupId -> comm.sadd(S_GROUP(groupId), a))
          .concatWith(Flux.fromIterable(removeGroupIds)
            .flatMap(groupId -> comm.srem(S_GROUP(groupId), a)));
    }

    public Flux<Article> getGroupArticles(long groupId, int page) {
        return getGroupArticles(groupId, page, ZS_SCORE);
    }

    public Flux<Article> getGroupArticles(long groupId, int page, String order) {
        String group = S_GROUP(groupId);
        String key = order + group;
        return comm.exists(key)
          .filter(i -> i == 1)
          .switchIfEmpty(comm.zinterstore(key, ZStoreArgs.Builder.max(), group, order))
          .thenMany(getArticles(page, key));
    }

    private Mono<String> putArticle(Article article) {
        Map<String, String> map = mapper.convertValue(article, new TypeReference<Map<String, String>>() {
        });
        return comm.hmset(v_ARTICLE(article.getId()), map);
    }

    private static class EntryToArticle implements Collector<Map.Entry<String, String>, Article, Article> {
        @Override
        public Supplier<Article> supplier() {
            return Suppliers.memoize(Article::new);
        }

        @Override
        public BiConsumer<Article, Map.Entry<String, String>> accumulator() {
            return (article, entry) -> {
                switch (entry.getKey()) {
                    case "id":
                        article.setId(Long.parseLong(entry.getValue()));
                        break;
                    case "title":
                        article.setTitle(entry.getValue());
                        break;
                    case "link":
                        article.setLink(entry.getValue());
                        break;
                    case "posterId":
                        article.setPosterId(Long.parseLong(entry.getValue()));
                        break;
                    case "time":
                        article.setTime(Long.parseLong(entry.getValue()));
                        break;
                    case "votes":
                        article.setVotes(Integer.parseInt(entry.getValue()));
                        break;
                    default:
                }
            };
        }

        @Override
        public BinaryOperator<Article> combiner() {
            return (a1, a2) -> a1;
        }

        @Override
        public Function<Article, Article> finisher() {
            return Function.identity();
        }

        @Override
        public Set<Characteristics> characteristics() {
            return EnumSet.of(Characteristics.UNORDERED,
              Characteristics.IDENTITY_FINISH,
              Characteristics.CONCURRENT);
        }
    }
}
