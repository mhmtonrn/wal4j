package io.mhmtonrn.event;

import org.springframework.context.ApplicationEvent;

public class CDCEvent extends ApplicationEvent {
    private final String message;

    public CDCEvent(Object source, String message) {
        super(source);
        this.message = message;
    }
    public String getMessage() {
        return message;
    }
}