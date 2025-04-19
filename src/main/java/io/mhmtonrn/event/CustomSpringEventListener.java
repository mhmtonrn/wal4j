package io.mhmtonrn.event;

import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
public class CustomSpringEventListener implements ApplicationListener<CDCEvent> {
    @Override
    public void onApplicationEvent(CDCEvent event) {
        System.out.println("Received spring custom event - " + event.getMessage());
    }
}