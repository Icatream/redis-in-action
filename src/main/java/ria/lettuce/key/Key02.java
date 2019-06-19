package ria.lettuce.key;

import ria.lettuce.enums.Severity;

import java.util.function.Function;

/**
 * @author YaoXunYu
 * created on 04/10/2019
 */
public interface Key02 extends BaseKey {
    String H_LOGIN = "login" + SEPARATOR;
    String Z_RECENT = "recent" + SEPARATOR;
    String Z_VIEWED = "viewed" + SEPARATOR;
    String ITEM = "item" + SEPARATOR;
    String CART = "cart" + SEPARATOR;

    static String RECENT(String k) {
        return Z_RECENT + k;
    }

    static String RECENT_SEVERITY(String k, Severity severity) {
        return RECENT(k + SEPARATOR + severity.value);
    }

    Function<String, String> VIEWED = k -> Z_VIEWED + k;

    static String v_ITEM(long itemId) {
        return ITEM + itemId;
    }

    static String H_CART(String session) {
        return CART + session;
    }
}
