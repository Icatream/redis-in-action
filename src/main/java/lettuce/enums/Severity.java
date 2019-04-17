package lettuce.enums;

/**
 * @author YaoXunYu
 * created on 04/17/2019
 */
public enum Severity {
    DEBUG("debug"),
    INFO("info"),
    WARNING("warning"),
    ERROR("error"),
    CRITICAL("critical");
    public final String value;

    Severity(String value) {
        this.value = value;
    }
}
