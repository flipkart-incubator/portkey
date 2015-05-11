# PortKey

PortKey is a Java model abstraction of persistence that works across multiple data stores and supports sharding. Entities can be persisted to more than one data store based on a set of rules. 

## Entities

Entities are objects to be persisted in data stores. Entities are pojo classes which implement `com.flipkart.portkey.common.entity.Entity` interface. Entity interface is a marker interface and used to identify PortKey entities. Entities also contain annotations which are used to specify data store configurations and mappings of pojo fields to database columns.

Example `Entity` class:

```java
package com.flipkart.portkey.example.dao;

import com.flipkart.portkey.common.entity.Entity;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsDataStore;
import com.flipkart.portkey.rdbms.metadata.annotation.RdbmsField;
import com.flipkart.portkey.redis.metadata.annotation.RedisDataStore;
import com.flipkart.portkey.redis.metadata.annotation.RedisField;

@RdbmsDataStore (tableName = "Person", databaseName = "test_db", shardKeyField = "uid")
@RedisDataStore (primaryKeyPattern = "[CLASS]:{uid}", shardKeyField = "uid")
public class Person implements Entity
{
	@RdbmsField (columnName = "uid", isPrimaryKey = true)
	@RedisField ()
	private String uid;

	@RdbmsField (columnName = "first_name")
	@RedisField ()
	private String firstName;

	@RdbmsField (columnName = "last_name")
	@RedisField ()
	private String lastName;

	public void setUid(String uid){
		this.uid=uid;
	}
	public String setUid(){
		return uid;
	}

	public void setFirstName(String firstName){
		this.firstName=firstName;
	}
	public String getFirstName(){
		return firstName;
	}

	public void setLastName(String lastName){
		this.lastName=lastName;
	}
	public String getLastName(){
		return lastName;
	}
}
```

Above class represents a typical entity used by PortKey for persistence. Annotations `@RdbmsDataStore` and `@RedisDataStore` are used to specify data store specific information for the entity. Also the fields which are to be persisted in data store are annotated with `@RdbmsField` and `@RedisField` annotations.


##Annotations:
Annotations are used to store data store specific information. Currently supported data stores are Rdbms (MySQL) and Redis.

###Annotations for Rdbms:
* `@RdbmsDataStore` (class annotation) : This annotation has three elements:
  - `tableName` - Name of the table being represented by entity.
  - `databaseName` - Name of the database in which table is stored.
  - `shardKeyField` -  Name of the field on which sharding will be done by PortKey.

* `@RdbmsField` (field annotation) : This annotation is defined for each field to be persisted in Table. It has three elements: 
  - `columnName` - Name of the column in rdbms table to which the field is mapped.
  - `isPrimaryKey` (optional) - `true` if mapped column in table is primary key. If not specified by user, default value for this element is `false`.
  - `serializer` (optional) - class extending com.flipkart.portkey.common.serializer.Serializer which will be used to serialize and deserialize values while mapping the entity to table and vice versa. We believe that this element is required very rarely as most of the time, the default way in which PortKey handles these conversions works well.

Annotations for Redis:
* `@RedisDataStore` (class annotation) : This annotation has four elements: 
  - `database` - Redis database to be used for persistence
  - `primaryKeyPattern` - The pattern to be used as key while storing entity in redis
  - `secondaryKeyPatterns` - Being a key - value store, Redis doesn’t support searches over non-key attibutes. So secondaryKeyPatterns is set of attributes over which indices are created in redis to support searches over non-key attributes.
  - `shardKeyField` - The field on which sharding will be done by PortKey.

* `@RedisField` (field annotation) : This is a marker annotation and has no elements. Present of this annotation specifies that the field should be included while storing the entity into redis.


## Configuration files:
Database configurations and persistence preferences for PortKey are specified in xml files. Following are the configuration files used by the example code.
- __portkey-application-context.xml__: This file is loaded by PortKey at start and so should should load/import all the other configuration files. If user adds any xml configuration file, he should add an <import> tag referencing to that xml file.
- __persistence-preference-configs.xml__ :  Persistence preference for each `Entity` is specified in this file. Persistence preference specifies the order in which data stores to be tried to read/write data from/into data stores.
- __persistence-layer-config.xml__ : This file contains `PersistenceLayer` bean and the beans required to initialize it.
- __rdbms-data-store-config.xml__ and __redis-data-store-config.xml__: These xml files contain beans named `rdbmsDataStore` and `redisDataStore` respectively which specify data store specific settings like the list of data store instances, connection pool configurations for each data store.

For understanding of the configurations required by PortKey, we encourage user to go through the xml files used by [example] (https://github.com/flipkart-incubator/portkey/tree/master/example) code.



## Data store interactions using PortKey:
PortKey uses object of class `com.flipkart.portkey.persistence.PersistenceLayer` to interact with data stores. Object of `PersistenceLayer` is created in spring config. If you go through the [example] (https://github.com/flipkart-incubator/portkey/tree/master/example) provided in source code, you will find the following code:
```java
ApplicationContext context =
		        new FileSystemXmlApplicationContext("src/main/resources/external/portkey-application-context.xml");
		pl = context.getBean(PersistenceLayer.class, "persistenceLayer");
```
The code retrieves the object of `PersistenceLayer` from spring config. The `PersistenceLayer` provides various APIs to interact with data store. User can go through the code provided in [example] (https://github.com/flipkart-incubator/portkey/tree/master/example) to get more insight about the API.
