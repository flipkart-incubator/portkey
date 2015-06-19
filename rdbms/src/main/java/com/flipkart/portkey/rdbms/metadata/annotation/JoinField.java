package com.flipkart.portkey.rdbms.metadata.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.flipkart.portkey.common.serializer.JsonSerializer;
import com.flipkart.portkey.common.serializer.Serializer;

@Retention (RetentionPolicy.RUNTIME)
@Target (ElementType.FIELD)
public @interface JoinField
{
	public String tableName();

	public String columnName();

	public Class<? extends Serializer> serializer() default JsonSerializer.class;
}
