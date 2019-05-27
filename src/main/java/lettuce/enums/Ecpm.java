package lettuce.enums;

/**
 * @author YaoXunYu
 * created on 05/23/19
 */
public enum Ecpm {
    CPM, CPC, CPA;

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
}
