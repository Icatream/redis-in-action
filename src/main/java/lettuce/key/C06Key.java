package lettuce.key;

/**
 * @author YaoXunYu
 * created on 05/05/19
 */
public interface C06Key extends BaseKey {
    String MEMBER = "member" + SEPARATOR;
    String QUEUE = "queue" + SEPARATOR;
    String DELAYED = "delayed" + SEPARATOR;
    String EMAIL_QUEUE = QUEUE + "email";
    String LOCK = "lock" + SEPARATOR;
    String CHAT = "chat" + SEPARATOR;
    String CHAT_ID = IDS + CHAT;
    String SEEN = "seen";
    String MSG = "msg" + SEPARATOR;
}
