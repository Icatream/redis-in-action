package lettuce.enums;

/**
 * @author YaoXunYu
 * created on 05/23/19
 */
public enum Ecpm {
    CPM,    //cost per mille views
    CPC,    //cost per click
    CPA;    //cost per action

    public double val(double views, double avg, double value) {
        switch (this) {
            case CPC:
            case CPA:
                return 1000. * value * avg / views;
            case CPM:
            default:
                return value;
        }
    }

    public boolean nameEqual(String name) {
        return this.name().equals(name);
    }
}
