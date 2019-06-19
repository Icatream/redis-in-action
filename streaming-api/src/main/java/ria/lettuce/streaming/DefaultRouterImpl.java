package ria.lettuce.streaming;

import reactor.core.publisher.Mono;

/**
 * @author YaoXunYu
 * created on 06/10/19
 */
public class DefaultRouterImpl extends RestfulMetadataRouter {

    public DefaultRouterImpl() {
        super();
        addRoute("/ping", RestfulMetadataRouter.HttpMethod.GET,
          map -> {
              map.forEach((k, v) -> {
                  System.out.println("k: " + k);
                  System.out.println("v:" + v);
              });
              return Mono.just("pong");
          });
    }
}
