package test;

/**
 * @author YaoXunYu
 * created on 08/16/19
 * Tail call optimizable
 */
public class LinkedN {

    private int i;
    private LinkedN next;

    private LinkedN(int i) {
        this.i = i;
    }

    public int getValue() {
        if (next != null) {
            return i + 10 * next.getValue();
        } else {
            return i;
        }
    }

    public static LinkedN create(int i) {
        int j = i / 10;
        if (j != 0) {
            LinkedN link = new LinkedN(i - 10 * j);
            create0(link, j);
            return link;
        } else {
            return new LinkedN(i);
        }
    }

    private static void create0(LinkedN n, int i) {
        int j = i / 10;
        if (j != 0) {
            n.next = new LinkedN(i - 10 * j);
            create0(n.next, j);
        } else {
            n.next = new LinkedN(i);
        }
    }

    public static LinkedN add(LinkedN n1, LinkedN n2) {
        if (n1 == null || n2 == null) {
            throw new IllegalArgumentException("Empty argument");
        }
        LinkedN link = create(n1.i + n2.i);
        add0(link, n1.next, n2.next);
        return link;
    }

    private static void add0(LinkedN n0, LinkedN n1, LinkedN n2) {
        LinkedN next = n0.next;
        if (next != null) {
            int x = next.i + n1.i + n2.i;
            int y = x - 10;
            if (y >= 0) {
                next.i = y;
                next.next = new LinkedN(1);
            } else {
                next.i = x;
            }
        } else {
            int x = n1.i + n2.i;
            int y = x - 10;
            if (y >= 0) {
                next = new LinkedN(y);
                next.next = new LinkedN(1);
            } else {
                next = new LinkedN(x);
            }
            n0.next = next;
        }
        if (n1.next != null) {
            if (n2.next != null) {
                add0(next, n1.next, n2.next);
            } else {
                add1(next, n1.next);
            }
        } else {
            if (n2.next != null) {
                add1(next, n2.next);
            }
        }
    }

    private static void add1(LinkedN n0, LinkedN n1) {
        LinkedN next = n0.next;
        if (next != null) {
            int x = next.i + n1.i;
            int y = x - 10;
            if (y >= 0) {
                next.i = y;
                next.next = new LinkedN(1);
            } else {
                next.i = x;
            }
            add1(n0.next, n1.next);
        } else {
            //Unsafe
            n0.next = n1;
        }
    }

    public static void main(String[] args) {
        LinkedN x = LinkedN.create(129);
        LinkedN y = LinkedN.create(452);
        LinkedN z = LinkedN.add(x, y);
        System.out.println(x.getValue());
        System.out.println(y.getValue());
        System.out.println(z.getValue());
    }
}
