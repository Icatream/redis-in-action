package lettuce.key;

/**
 * @author YaoXunYu
 * created on 05/05/19
 */
public interface C06Key extends BaseKey {
    String MEMBER = "member" + SEPARATOR;
    String QUEUE = "queue" + SEPARATOR;
    String EMAIL_QUEUE = QUEUE + "email";
}
