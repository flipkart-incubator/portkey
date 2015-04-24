package com.flipkart.portkey.common.persistence.query;

import com.flipkart.portkey.common.entity.Entity;

public interface PortKeyQuery
{

	Class<? extends Entity> getClazz();
}
