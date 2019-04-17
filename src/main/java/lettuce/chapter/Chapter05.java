package lettuce.chapter;

import io.lettuce.core.api.reactive.RedisReactiveCommands;

/**
 * @author YaoXunYu
 * created on 04/17/2019
 */
public class Chapter05 extends BaseChapter {
    public Chapter05(RedisReactiveCommands<String, String> comm) {
        super(comm);
    }

}
