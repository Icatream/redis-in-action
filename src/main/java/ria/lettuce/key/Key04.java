package ria.lettuce.key;

/**
 * @author YaoXunYu
 * created on 04/15/2019
 */
public interface Key04 extends BaseKey {
    String PROGRESS = "progress" + SEPARATOR;
    String s_FILE = PROGRESS + "FILE";
    String s_OFFSET = PROGRESS + "POSITION";
    String INVENTORY = "inventory" + SEPARATOR;
    String Z_MARKET = "market" + SEPARATOR;
    String f_FUNDS = "funds";

    static String S_INVENTORY(long userId) {
        return INVENTORY + userId;
    }
}
