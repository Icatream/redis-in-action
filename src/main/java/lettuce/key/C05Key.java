package lettuce.key;

/**
 * @author YaoXunYu
 * created on 04/18/2019
 */
public interface C05Key extends BaseKey {
    String COMMON = "common" + SEPARATOR;
    String COUNT = "count" + SEPARATOR;
    String KNOWN = "known" + SEPARATOR;
    String STATS = "stats" + SEPARATOR;
    String CITY_SEP = "_";
    String IP_CITY = "ip2cityid" + SEPARATOR;
    String CITY_HASH = "cityid2city" + SEPARATOR;
    String IS_UNDER_MAINTENANCE = "is-under-maintenance";
}
