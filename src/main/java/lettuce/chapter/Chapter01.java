package lettuce.chapter;

import lettuce.pojo.Article;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import lettuce.key.ArticleKey;
import io.lettuce.core.ZStoreArgs;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
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
        String a = ArticleKey.ARTICLE_PREFIX + articleId;
        return comm.zscore(ArticleKey.TIME_Z_SET, a)
            .filter(d -> d >= cutOff)
            .then(comm.sadd(ArticleKey.VOTED_PREFIX + articleId, ArticleKey.USER_PREFIX + userId)
                .filter(i -> i == 1)
                .then(comm.zincrby(ArticleKey.SCORE_Z_SET, VOTE_SCORE, a)
                    .then(comm.hincrby(a, ArticleKey.PROP_VOTES, 1))));
    }

    /**
     * redis incr获取文章ID,
     * 添加投票, 和投票超时设置,
     * 添加 文章 hash
     * 添加分值,
     * 添加时间
     */
    public Mono<Long> postArticle(Article article) {
        return comm.incr(ArticleKey.ARTICLE_PREFIX)
            .flatMap(id -> {
                article.setId(id);
                long now = LocalDateTime.now().atZone(ZoneOffset.systemDefault()).toEpochSecond();
                article.setTime(now);
                String voted = ArticleKey.VOTED_PREFIX + id;
                String a = ArticleKey.ARTICLE_PREFIX + id;
                return comm.sadd(voted, ArticleKey.USER_PREFIX + article.getPosterId())
                    .then(comm.expire(voted, Duration.ofDays(7).getSeconds()))
                    .then(putArticle(article))
                    .then(comm.zadd(ArticleKey.SCORE_Z_SET, now + VOTE_SCORE, a))
                    .then(comm.zadd(ArticleKey.TIME_Z_SET, now, a))
                    .thenReturn(id);
            });
    }

    public Flux<Article> getArticles(int page) {
        return getArticles(page, ArticleKey.SCORE_Z_SET);
    }

    public Flux<Article> getArticles(int page, String order) {
        long start = (page - 1) * ARTICLE_PER_PAGE;
        long end = start + ARTICLE_PER_PAGE - 1;
        return comm.zrevrange(order, start, end)
            .flatMap(comm::hgetall)
            .map(map -> mapper.convertValue(map, Article.class));
    }

    public Flux<Long> changeGroup(long articleId, List<Long> addGroupIds, List<Long> removeGroupIds) {
        String a = ArticleKey.ARTICLE_PREFIX + articleId;
        return Flux.fromIterable(addGroupIds)
            .flatMap(groupId -> comm.sadd(ArticleKey.GROUP_PREFIX + groupId, a))
            .concatWith(Flux.fromIterable(removeGroupIds)
                .flatMap(groupId -> comm.srem(ArticleKey.GROUP_PREFIX + groupId, a)));
    }

    public Flux<Article> getGroupArticles(long groupId, int page) {
        return getGroupArticles(groupId, page, ArticleKey.SCORE_Z_SET);
    }

    public Flux<Article> getGroupArticles(long groupId, int page, String order) {
        String group = ArticleKey.GROUP_PREFIX + groupId;
        String key = order + group;
        return comm.exists(key)
            .filter(i -> i == 1)
            .switchIfEmpty(comm.zinterstore(key, ZStoreArgs.Builder.max(), group, order))
            .thenMany(getArticles(page, key));
    }

    private Mono<String> putArticle(Article article) {
        Map<String, String> map = mapper.convertValue(article, new TypeReference<Map<String, String>>() {
        });
        return comm.hmset(ArticleKey.ARTICLE_PREFIX + article.getId(), map);
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
