package ria.lettuce.streaming.restful;

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.util.DefaultPayload;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author YaoXunYu
 * created on 06/04/19
 */
public class RSocketImpl extends AbstractRSocket {

    private final RestfulMetadataRouter router;

    public RSocketImpl() {
        router = new DefaultRouterImpl();
    }

    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        return Mono.from(router.route(payload))
          .then();
    }

    @Override
    public Mono<Payload> requestResponse(Payload payload) {
        return Mono.fromDirect(router.route(payload))
          .map(this::fakePayloadWriter);
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        return Flux.from(router.route(payload))
          .map(this::fakePayloadWriter);
    }

    @Override
    public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
        return Flux.from(payloads)
          .flatMap(router::route)
          .map(this::fakePayloadWriter);
    }

    @Override
    public Mono<Void> metadataPush(Payload payload) {
        return Mono.empty();
    }

    private Payload fakePayloadWriter(Object obj) {
        if (obj instanceof CharSequence) {
            return DefaultPayload.create((CharSequence) obj);
        } else {
            return DefaultPayload.create(obj.toString());
        }
    }
}
