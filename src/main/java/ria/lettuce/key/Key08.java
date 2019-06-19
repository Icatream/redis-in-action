package ria.lettuce.key;

/**
 * @author YaoXunYu
 * created on 05/28/19
 */
public interface Key08 extends BaseKey {
    String ID = "id" + SEPARATOR;
    String USER = "user" + SEPARATOR;
    String USER_ID = USER + ID;
    String STATUS = "status" + SEPARATOR;
    String STATUS_ID = STATUS + ID;
    String FOLLOWING = "following" + SEPARATOR;
    String FOLLOWERS = "followers" + SEPARATOR;
    String PROFILE = "profile" + SEPARATOR;
    String HOME = "home" + SEPARATOR;

    static String v_USER(String k) {
        return USER + k;
    }

    static String H_USER(String k) {
        return v_USER(k);
    }

    static String H_STATUS(String k) {
        return STATUS + k;
    }

    static String Z_FOLLOWING(String k) {
        return FOLLOWING + k;
    }

    static String Z_FOLLOWERS(String k) {
        return FOLLOWERS + k;
    }

    static String Z_PROFILE(String k) {
        return PROFILE + k;
    }

    static String Z_HOME(String k) {
        return HOME + k;
    }
}
