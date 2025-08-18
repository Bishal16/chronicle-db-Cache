package com.telcobright.core.cache.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to specify column mapping for entity fields
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    /**
     * The name of the database column
     */
    String name();
    
    /**
     * Whether this column is the primary key
     */
    boolean primaryKey() default false;
}