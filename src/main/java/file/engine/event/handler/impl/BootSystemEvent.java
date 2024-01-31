package file.engine.event.handler.impl;

import file.engine.event.handler.Event;

public class BootSystemEvent extends Event {
    public final int port;

    public BootSystemEvent(int port) {
        super();
        this.setBlock();
        this.port = port;
    }
}
