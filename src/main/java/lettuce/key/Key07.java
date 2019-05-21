package lettuce.key;

/**
 * @author YaoXunYu
 * created on 05/20/19
 */
public interface Key07 extends BaseKey {
    String IDX = "idx" + SEPARATOR;

    static String S_IDX(String k) {
        return IDX + k;
    }
}
