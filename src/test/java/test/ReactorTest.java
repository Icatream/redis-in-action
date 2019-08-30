package test;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author YaoXunYu
 * created on 08/30/19
 */
public class ReactorTest {

    public static void main(String[] args) throws InterruptedException {
        elasticTest();
        Thread.currentThread().join();
        /*Connection connection = TcpClient.create()
          .addressSupplier(() -> new InetSocketAddress("127.0.0.1", 8000))
          .handle((in, out) -> {
              System.out.println(in.receiveObject());
              return out;
          })
          .connectNow();*/
    }

    private static Disposable elasticTest() {
        return Mono.zip(Mono.fromCallable(makeCall(1)).subscribeOn(Schedulers.elastic()),
          Mono.fromCallable(makeCall(2)).subscribeOn(Schedulers.elastic()))
          .map(tuple -> tuple.getT1() + tuple.getT2())
          .map(Object::toString)
          .publishOn(Schedulers.single())
          .doOnNext(s -> printThreadAndTime(" res-> " + s))
          .subscribe();
    }

    private static Disposable asyncTest() {
        return Mono.zip(Mono.fromCompletionStage(CompletableFuture.supplyAsync(makeSupplier(1))),
          Mono.fromCompletionStage(CompletableFuture.supplyAsync(makeSupplier(2))))
          .map(tuple -> tuple.getT1() + tuple.getT2())
          .map(Object::toString)
          .doOnNext(s -> printThreadAndTime(" result-> " + s))
          .subscribe();
    }

    private static <V> Callable<V> makeCall(V obj) {
        return () -> {
            printThreadAndTime(" -> producing -> " + obj);
            TimeUnit.SECONDS.sleep(2);
            printThreadAndTime(" -> finished -> " + obj);
            return obj;
        };
    }

    private static <V> Supplier<V> makeSupplier(V obj) {
        return () -> {
            printThreadAndTime(" -> producing -> " + obj);
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            printThreadAndTime(" -> finished -> " + obj);
            return obj;
        };
    }

    private static void printThreadAndTime(String s) {
        System.out.println(Thread.currentThread().getName() + " -> " + System.currentTimeMillis() + s);
    }
}
