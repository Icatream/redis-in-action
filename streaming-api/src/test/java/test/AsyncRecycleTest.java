package test;

import io.netty.util.Recycler;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author YaoXunYu
 * created on 08/14/19
 */
public class AsyncRecycleTest {

    public static void main(String[] args) throws InterruptedException {
        Recyclable original = Recyclable.newInstance();
        System.out.println("In main: " + Thread.currentThread());

        AtomicReference<Recyclable> r2 = new AtomicReference<>();
        Thread thread = new Thread(
          () -> {
              System.out.println("Another: " + Thread.currentThread());
              original.recycle();
              r2.set(Recyclable.newInstance());
          });
        thread.start();
        thread.join();

        Recyclable r1 = Recyclable.newInstance();
        System.out.println(original == r1);
        System.out.println(original == r2.get());
    }

    private static class Recyclable {
        private static final Recycler<Recyclable> RECYCLER = new Recycler<Recyclable>() {
            @Override
            protected Recyclable newObject(Handle<Recyclable> handle) {
                return new Recyclable(handle);
            }
        };

        public static Recyclable newInstance() {
            return RECYCLER.get();
        }

        private final Recycler.Handle<Recyclable> handle;

        private Recyclable(Recycler.Handle<Recyclable> handle) {
            this.handle = handle;
        }

        public void recycle() {
            handle.recycle(this);
        }
    }
}
