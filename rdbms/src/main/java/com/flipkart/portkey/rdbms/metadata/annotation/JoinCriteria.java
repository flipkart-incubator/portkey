package com.flipkart.portkey.rdbms.metadata.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author santosh.p
 */

@Retention (RetentionPolicy.RUNTIME)
@Target (ElementType.TYPE)
public @interface JoinCriteria
{
	public String srcTable();

	public String destTable();

	public String criteriaValue();

	public String srcColumn();

	public String destColumn();

	public String joinType();
}
