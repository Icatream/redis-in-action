package ria.lettuce.streaming;

import io.lettuce.core.RedisClient;
import io.lettuce.core.pubsub.api.reactive.ChannelMessage;
import io.lettuce.core.pubsub.api.reactive.RedisPubSubReactiveCommands;
import io.netty.buffer.ByteBuf;
import io.rsocket.exceptions.InvalidException;
import org.jctools.maps.NonBlockingHashSet;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * @author YaoXunYu
 * created on 08/22/19
 */
public class DefaultStreamingService implements StreamingService {

    private final RedisPubSubReactiveCommands<String, String> pubsub;
    private final AtomicBoolean closed = new AtomicBoolean();

    private final NonBlockingHashSet<String> channels = new NonBlockingHashSet<>();

    private final Flux<ChannelMessage<String, String>> subpub;

    public DefaultStreamingService(RedisClient client) {
        this.pubsub = client.connectPubSub().reactive();
        subpub = pubsub.observeChannels(FluxSink.OverflowStrategy.LATEST)
          .publish()
          .autoConnect();
    }

    public Mono<Void> subscribe(String... channels) {
        return pubsub.subscribe(channels)
          .doOnSuccess(v -> this.channels.addAll(Arrays.asList(channels)));
    }

    public Mono<Void> unsubscribe(String[] channels) {
        return pubsub.unsubscribe(channels);
    }

    @Override
    public Mono<Subscription> requestReply(ApiReq message, ByteBuf metadata) {
        if (!authenticate(message)) {
            throw new InvalidException("Illegal identification");
        }
        if (ApiReq.Filter.SAMPLE.equals(message.getFilter())) {
            return Mono.from(subpub
              .transform(fakeRandom(message))
              .take(1))
              .map(messageMapper);
        }
        throw new InvalidException("Sample Only for Request-Reply");
    }

    private Function<ChannelMessage<String, String>, Subscription> messageMapper = cm -> Subscription.newBuilder()
      .setChannel(cm.getChannel())
      .setMessage(cm.getMessage())
      .build();

    private Function<Flux<ChannelMessage<String, String>>, Flux<ChannelMessage<String, String>>> fakeRandom(ApiReq message) {
        long skip = message.getId() & ((1 << 3) - 1);
        System.out.println("Id<" + message.getId() + "> skip: " + skip);
        return flux -> flux.skip(skip);
    }

    @Override
    public Flux<Subscription> requestStream(ApiReq message, ByteBuf metadata) {
        if (!authenticate(message)) {
            throw new InvalidException("Illegal identification");
        }
        switch (message.getFilter()) {
            case TRACK:
                //return subpub.map(messageMapper);
            case FOLLOW:
                //return subpub.map(messageMapper);
            case LOCATION:
                return subpub.map(messageMapper);
            default:
                throw new InvalidException("Sample Only for Request-Reply");
        }
    }

    private Function<Flux<ChannelMessage<String, String>>, Flux<ChannelMessage<String, String>>> fakeFilter = flux -> flux;

    private boolean authenticate(ApiReq m) {
        System.out.println("Incoming identification<" + m.getIdentification() + ">");
        return true;
    }

    public Disposable close() {
        return close0()
          .subscribe();
    }

    public Mono<Void> close0() {
        return pubsub.unsubscribe(channels.toArray(new String[0]))
          .doOnTerminate(() -> pubsub.getStatefulConnection()
            .closeAsync()
            .whenComplete((v, throwable) -> {
                if (throwable != null) {
                    throw new Error(throwable);
                }
                closed.set(true);
            }));
    }

    @Override
    protected void finalize() {
        if (!closed.get()) {
            close0().block();
        }
    }
}
