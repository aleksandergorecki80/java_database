package com.ag.peopledb.anotation;

import com.ag.peopledb.model.CrudOperation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface SQL {
    String value();
    CrudOperation operationType();
}
