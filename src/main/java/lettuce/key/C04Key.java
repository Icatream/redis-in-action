package lettuce.key;

/**
 * @author YaoXunYu
 * created on 04/15/2019
 */
public interface C04Key extends BaseKey {
    String PROGRESS = "progress" + SEPARATOR;
    String CURRENT_FILE = PROGRESS + "FILE";
    String OFFSET = PROGRESS + "POSITION";
    String INVENTORY = "inventory" + SEPARATOR;
    String MARKET = "market" + SEPARATOR;
    String FUNDS = "funds";
}
