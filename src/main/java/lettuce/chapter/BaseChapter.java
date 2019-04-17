package lettuce.chapter;

import io.lettuce.core.api.reactive.RedisReactiveCommands;
import reactor.test.StepVerifier;

/**
 * @author YaoXunYu
 * created on 04/17/2019
 */
public abstract class BaseChapter {

    protected final RedisReactiveCommands<String, String> comm;

    protected BaseChapter(RedisReactiveCommands<String, String> comm) {
        this.comm = comm;
    }

    protected void del(String key) {
        StepVerifier.create(comm.del(key))
            .expectNext(1L)
            .verifyComplete();
    }

    public void scriptFlush() {
        StepVerifier.create(comm.scriptFlush())
            .expectNext("OK")
            .verifyComplete();
    }
}
