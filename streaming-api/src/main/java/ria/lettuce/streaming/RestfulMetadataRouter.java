package ria.lettuce.streaming;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import io.netty.buffer.ByteBuf;
import io.rsocket.Payload;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * @author YaoXunYu
 * created on 06/04/19
 */
public class RestfulMetadataRouter {
    private final Table<String, HttpMethod, Function<Map<String, String>, Publisher<?>>> table = HashBasedTable.create();
    private final static Logger logger = LoggerFactory.getLogger(RestfulMetadataRouter.class);
    private static final byte equal = 61;
    private static final byte and = 38;

    public Publisher<?> route(Payload payload) {
        Function<Map<String, String>, Publisher<?>> route = null;
        try {
            ByteBuf metadata = payload.metadata();
            byte b = metadata.readByte();
            HttpMethod method = HttpMethod.parse(b);
            String path = metadata.toString(StandardCharsets.UTF_8);
            route = table.get(path, method);
        } catch (Exception e) {
            logger.error("parse metadata fail", e);
        }
        return Optional.ofNullable(route)
          .map(r -> {
              ByteBuf data = payload.data();
              return r.apply(getParameters(data));
          })
          .orElse(Mono.error(new RuntimeException("Route not found, check metadata")));
    }

    private Map<String, String> getParameters(ByteBuf data) {
        Map<String, String> m = new HashMap<>();
        while (data.isReadable()) {
            int kIn = data.bytesBefore(equal);
            if (kIn == -1) {
                break;
            }
            String k = data.readBytes(kIn).toString(StandardCharsets.UTF_8);
            data.readerIndex(data.readerIndex() + 1);
            int vIn = data.bytesBefore(and);
            String v;
            if (vIn == -1) {
                v = data.readBytes(data.readableBytes()).toString(StandardCharsets.UTF_8);
            } else {
                v = data.readBytes(data.bytesBefore(and)).toString(StandardCharsets.UTF_8);
                data.readerIndex(data.readerIndex() + 1);
            }
            m.put(k, v);
        }
        return m;
    }

    public void addRoute(String path, HttpMethod method, Function<Map<String, String>, Publisher<?>> func) {
        table.put(path, method, func);
    }

    public enum HttpMethod {
        //DEFAULT((byte) 0),
        GET((byte) 1),
        POST((byte) 2),
        PUT((byte) 3),
        PATCH((byte) 4),
        DELETE((byte) 5),
        HEAD((byte) 6),
        OPTIONS((byte) 7),
        TRACE((byte) 8);

        byte flag;

        HttpMethod(byte flag) {
            this.flag = flag;
        }

        static HttpMethod parse(byte b) {
            for (HttpMethod httpMethod : HttpMethod.values()) {
                if (b == httpMethod.flag) {
                    return httpMethod;
                }
            }
            throw new IllegalArgumentException("HttpMethod not found");
        }
    }
}
