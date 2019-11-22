package com.list.fh.ckanmetacreator;

import com.list.fh.ckanmetacreator.service.CkanMetaCreateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

@Component
public class ParamListener implements ApplicationListener<ContextRefreshedEvent> {

    @Value("${source:default_value}")
    private String source;

    @Value("${target:default_value}")
    private String target;

    @Autowired
    private CkanMetaCreateService ckanMetaCreateService;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        try {
            ckanMetaCreateService.createCkanMeta(source, target);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
