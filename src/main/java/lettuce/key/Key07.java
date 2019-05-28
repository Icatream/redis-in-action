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
    String CLICKS = "clicks" + SEPARATOR;
    String ACTIONS = "actions" + SEPARATOR;
    String JOB = "job" + SEPARATOR;
    String Z_JOB_REQ = JOB + REQ;
    String SKILL = "skill" + SEPARATOR;
    String IDX_SKILL = IDX + SKILL;
    String Z_IDX_JOB_REQ = IDX + Z_JOB_REQ;

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

    static String S_MATCHED(Long id) {
        return MATCHED + id;
    }

    static String Z_VIEWS(String k) {
        return VIEWS + k;
    }

    static String TYPE_VIEWS(String k) {
        return H_TYPE + k + SEPARATOR + VIEWS;
    }

    static String Z_CLICKS(String k) {
        return CLICKS + k;
    }

    static String Z_ACTIONS(String k) {
        return ACTIONS + k;
    }

    static String TYPE_CLICKS(String k) {
        return H_TYPE + k + SEPARATOR + CLICKS;
    }

    static String TYPE_ACTIONS(String k) {
        return H_TYPE + k + SEPARATOR + ACTIONS;
    }

    static String Z_SKILL(String k) {
        return SKILL + k;
    }

    static String S_JOB(String k) {
        return JOB + k;
    }

    static String S_IDX_SKILL(String k) {
        return IDX_SKILL + k;
    }
}
