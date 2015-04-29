/**
 * 
 */
package com.flipkart.portkey.rdbms.metadata.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.flipkart.portkey.common.serializer.JsonSerializer;
import com.flipkart.portkey.common.serializer.Serializer;

/**
 * @author santosh.p
 */
@Retention (RetentionPolicy.RUNTIME)
@Target (ElementType.FIELD)
public @interface RdbmsField
{
	public String columnName();

	public boolean isPrimaryKey() default false;

	public boolean isUnique() default false;

	public String defaultValue() default "";

	public Class<? extends Serializer> serializer() default JsonSerializer.class;
}
