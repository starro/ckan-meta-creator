package com.list.fh.ckanmetacreator.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@SpringBootTest
@RunWith(SpringRunner.class)
class CkanMetaCreateServiceTest {
    @Autowired
    CkanMetaCreateService ckanMetaCreateService;

    @Test
    public void test1() throws Exception {
        ckanMetaCreateService.createCkanMeta("D:\\dcat_master.csv","D:\\tmp\\test.ckan");
    }
}