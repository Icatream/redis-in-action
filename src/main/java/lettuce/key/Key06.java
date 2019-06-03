package lettuce.key;

import java.time.LocalDate;

/**
 * @author YaoXunYu
 * created on 05/05/19
 */
public interface Key06 extends BaseKey {
    String MEMBER = "member" + SEPARATOR;
    String DELAYED = "delayed" + SEPARATOR;
    String QUEUE = "queue" + SEPARATOR;
    String L_EMAIL_QUEUE = QUEUE + "email";
    String LOCK = "lock" + SEPARATOR;
    String CHAT = "chat" + SEPARATOR;
    String i_CHAT_ID = IDS + CHAT;
    String SEEN = "seen" + SEPARATOR;
    String MSG = "msg" + SEPARATOR;
    String DAILY = "daily" + SEPARATOR;
    String DAILY_COUNTRY = DAILY + "country" + SEPARATOR;
    String PROCESS_FINISH_SUFFIX = SEPARATOR + "done";

    static String Z_MEMBER(long guildId) {
        return MEMBER + guildId;
    }

    static String L_QUEUE(String q) {
        return QUEUE + q;
    }

    static String s_LOCK(String k) {
        return LOCK + k;
    }

    static String Z_CHAT(String chatId) {
        return CHAT + chatId;
    }

    static String Z_SEEN(String k) {
        return SEEN + k;
    }

    static String Z_MSG(String k) {
        return MSG + k;
    }

    static String Z_DAILY_COUNTRY(LocalDate day) {
        return DAILY_COUNTRY + day;
    }
}
