package lettuce.key;

/**
 * @author YaoXunYu
 * created on 04/10/2019
 */
public interface ArticleKey extends BaseKey {

    String ARTICLE = "article";
    String ARTICLE_PREFIX = ARTICLE + SEPARATOR;
    String TIME_Z_SET = "time" + SEPARATOR;
    String SCORE_Z_SET = "score" + SEPARATOR;
    String VOTED_PREFIX = "voted" + SEPARATOR;
    String USER_PREFIX = "user" + SEPARATOR;
    String GROUP_PREFIX = "group" + SEPARATOR;
    String PROP_VOTES = "votes";
}
