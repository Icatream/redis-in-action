package lettuce.key;

/**
 * @author YaoXunYu
 * created on 05/20/19
 */
public interface Key07 extends BaseKey {
    String IDX = "idx" + SEPARATOR;
    String REQ = "req" + SEPARATOR;
    String IDX_REQ = IDX + REQ;
    String Z_SORT_UPDATE = "sort" + SEPARATOR + "update";
    String Z_SORT_VOTES = "sort" + SEPARATOR + "votes";
    String H_TYPE = "type" + SEPARATOR;
    String AD = "ad" + SEPARATOR;
    String Z_AD_VALUE = AD + "value" + SEPARATOR;
    String Z_IDX_AD_VALUE = IDX + Z_AD_VALUE;
    String Z_AD_BASE_VALUE = AD + "base_value" + SEPARATOR;
    String TERMS = "terms" + SEPARATOR;
    String MATCHED = TERMS + "matched" + SEPARATOR;
    String ADS_SERVED = "ads" + SEPARATOR + "served" + SEPARATOR;
    String VIEWS = "views" + SEPARATOR;

    static String S_IDX(String k) {
        return IDX + k;
    }

    static String S_IDX_REQ(String k) {
        return IDX_REQ + k;
    }

    static String ZS_IDX(String k) {
        return S_IDX(k);
    }

    static String S_TERMS(String k) {
        return TERMS + k;
    }

    static String S_REQ(String k) {
        return REQ + k;
    }

    static String S_MATCHED(String k) {
        return MATCHED + k;
    }

    static String Z_VIEWS(String k) {
        return VIEWS + k;
    }

    static String TYPE_VIEWS(String k) {
        return H_TYPE + k + SEPARATOR + VIEWS;
    }
}
