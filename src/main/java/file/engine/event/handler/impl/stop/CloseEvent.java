package file.engine.event.handler.impl.stop;

import file.engine.event.handler.Event;

public class CloseEvent extends Event {
    public CloseEvent() {
        super();
        this.setBlock();
    }
}
