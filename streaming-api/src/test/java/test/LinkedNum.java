package test;

/**
 * @author YaoXunYu
 * created on 08/15/19
 */
public class LinkedNum {
    private int i;
    private LinkedNum next;

    public LinkedNum(int i) {
        if (i != 0) {
            int j = i / 10;
            if (j != 0) {
                this.i = i - 10 * j;
                this.next = new LinkedNum(j);
                return;
            }
        }
        this.i = i;
        this.next = null;
    }

    public LinkedNum(int i, LinkedNum next) {
        this.i = i;
        this.next = next;
    }

    public int getValue() {
        if (next != null) {
            return i + 10 * next.getValue();
        } else {
            return i;
        }
    }

    public static LinkedNum add(LinkedNum n1, LinkedNum n2) {
        int x = n1.i + n2.i;
        int y = x - 10;
        if (n1.next != null) {
            if (n2.next != null) {
                if (y >= 0) {
                    return new LinkedNum(y, add(new LinkedNum(1), add(n1.next, n2.next)));
                } else {
                    return new LinkedNum(x, add(n1.next, n2.next));
                }
            } else {
                if (y >= 0) {
                    return new LinkedNum(y, add(new LinkedNum(1), n1.next));
                } else {
                    return new LinkedNum(x, n1.next);
                }
            }
        } else {
            if (n2.next != null) {
                if (y >= 0) {
                    return new LinkedNum(y, add(new LinkedNum(1), n2.next));
                } else {
                    return new LinkedNum(x, n2.next);
                }
            } else {
                return new LinkedNum(x);
            }
        }
    }

    public static void main(String[] args) {
        LinkedNum x = new LinkedNum(1234);
        LinkedNum y = new LinkedNum(276);
        LinkedNum z = LinkedNum.add(x, y);
        System.out.println(x.getValue());
        System.out.println(y.getValue());
        System.out.println(z.getValue());

    }
}
