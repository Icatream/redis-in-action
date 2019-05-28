package lettuce.key;

/**
 * @author YaoXunYu
 * created on 05/28/19
 */
public interface Key08 extends BaseKey {
    String USER = "user" + SEPARATOR;
    String USER_ID = USER + "id" + SEPARATOR;

    static String v_USER(String k) {
        return USER + k;
    }

    static String H_USER(String id) {
        return USER + id;
    }
}
