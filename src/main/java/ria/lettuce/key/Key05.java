package ria.lettuce.key;

import ria.lettuce.enums.Severity;

/**
 * @author YaoXunYu
 * created on 04/18/2019
 */
public interface Key05 extends BaseKey {
    String COMMON = "common" + SEPARATOR;
    String COUNT = "count" + SEPARATOR;
    String KNOWN = "known" + SEPARATOR;
    String STATS = "stats" + SEPARATOR;
    String Z_IP_CITY = "ip2cityid" + SEPARATOR;
    String H_CITY = "cityid2city" + SEPARATOR;
    String s_IS_UNDER_MAINTENANCE = "is-under-maintenance";

    static String COMMON_SEVERITY(String k, Severity severity) {
        return COMMON + k + SEPARATOR + severity.value;
    }

    static String CITY(int cityId, int rowIndex) {
        return cityId + OTHER_SEPARATOR + rowIndex;
    }
}
