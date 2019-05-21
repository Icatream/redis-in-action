package lettuce.key;

/**
 * @author YaoXunYu
 * created on 04/10/2019
 */
public interface Key01 extends BaseKey {

    String ARTICLE = "article" + SEPARATOR;
    String ZS_TIME = "time" + SEPARATOR;
    String ZS_SCORE = "score" + SEPARATOR;
    String VOTED = "voted" + SEPARATOR;
    String USER = "user" + SEPARATOR;
    String GROUP = "group" + SEPARATOR;
    String f_VOTES = "votes";

    static String v_ARTICLE(long articleId) {
        return ARTICLE + articleId;
    }

    static String S_VOTED(long articleId) {
        return VOTED + articleId;
    }

    static String v_USER(long userId) {
        return USER + userId;
    }

    static String S_GROUP(long groupId) {
        return GROUP + groupId;
    }
}
