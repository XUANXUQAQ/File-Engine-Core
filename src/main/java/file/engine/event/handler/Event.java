package file.engine.event.handler;

import lombok.Setter;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class Event {
    private final AtomicBoolean isFinished = new AtomicBoolean(false);
    private final AtomicInteger executeTimes = new AtomicInteger(0);
    private final AtomicBoolean isBlock = new AtomicBoolean(false);
    @Setter
    private int maxRetryTimes = 5;
    @Setter
    private volatile Object returnValue;
    @Setter
    private Consumer<Event> callback;
    @Setter
    private Consumer<Event> errorHandler;
    @Setter
    private volatile Throwable exception;

    protected void incrementExecuteTimes() {
        executeTimes.incrementAndGet();
    }

    protected boolean allRetryFailed() {
        return executeTimes.get() > maxRetryTimes;
    }

    public boolean isFinished() {
        return isFinished.get();
    }

    public void setBlock() {
        isBlock.set(true);
    }

    public boolean isBlock() {
        return isBlock.get();
    }

    protected void execErrorHandler() {
        if (this.errorHandler != null) {
            this.errorHandler.accept(this);
        }
    }

    protected void setFinishedAndExecCallback() {
        if (this.callback != null) {
            this.callback.accept(this);
        }
        isFinished.set(true);
    }

    protected void setFinished() {
        isFinished.set(true);
    }

    @SuppressWarnings("unchecked")
    public <T> Optional<T> getReturnValue() {
        return Optional.ofNullable((T) returnValue);
    }

    public Optional<Throwable> getException() {
        return Optional.ofNullable(exception);
    }

    @Override
    public String toString() {
        return "{event >>> " + this.getClass() + "}";
    }
}
