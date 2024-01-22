package file.engine.event.handler.impl.configs;

import file.engine.configs.ConfigEntity;
import file.engine.event.handler.Event;
import lombok.Getter;

@Getter
public class SetConfigsEvent extends Event {

    /**
     * -- GETTER --
     *  只允许AllConfigs进行调用，因为不能保证配置的正确性
     *
     */
    private final ConfigEntity configs;

    public SetConfigsEvent(ConfigEntity configEntity) {
        super();
        this.setBlock();
        this.configs = configEntity;
    }
}
