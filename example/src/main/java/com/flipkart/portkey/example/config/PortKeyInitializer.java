package com.flipkart.portkey.example.config;

import java.beans.PropertyVetoException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.flipkart.portkey.common.datastore.DataStoreConfig;
import com.flipkart.portkey.common.entity.persistence.EntityPersistencePreference;
import com.flipkart.portkey.common.entity.persistence.ReadConfig;
import com.flipkart.portkey.common.entity.persistence.WriteConfig;
import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.common.enumeration.FailureAction;
import com.flipkart.portkey.common.persistence.PersistenceManager;
import com.flipkart.portkey.common.sharding.ShardIdentifier;
import com.flipkart.portkey.persistence.PersistenceLayer;
import com.flipkart.portkey.rdbms.datastore.RdbmsDataStoreConfig;
import com.flipkart.portkey.rdbms.metadata.RdbmsMetaDataCache;
import com.flipkart.portkey.rdbms.persistence.RdbmsPersistenceManager;
import com.flipkart.portkey.rdbms.persistence.config.RdbmsPersistenceManagerConfig;
import com.flipkart.portkey.rdbms.sharding.RdbmsShardIdentifierForSingleShard;
import com.mchange.v2.c3p0.ComboPooledDataSource;

public class PortKeyInitializer
{
	private static Logger logger = Logger.getLogger(PortKeyInitializer.class);

	private static Properties loadProperties(String pathToPropertiesFile)
	{
		Properties prop = null;
		InputStream input = null;
		try
		{
			input = new FileInputStream(pathToPropertiesFile);
			prop = new Properties();
			prop.load(input);
			return prop;
		}
		catch (FileNotFoundException e)
		{
			logger.error("Failed to load properties file " + pathToPropertiesFile, e);
		}
		catch (IOException e)
		{
			logger.error("Failed to load properties file " + pathToPropertiesFile, e);
		}
		return null;
	}

	public static PersistenceLayer initialize()
	{

		// Config for MySQL
		ComboPooledDataSource shard1MasterDataSource = new ComboPooledDataSource();
		Properties prop = loadProperties("target/classes/project.properties");
		try
		{
			shard1MasterDataSource.setDriverClass(prop.getProperty("RDBMS.SHARD1.MASTER.DRIVER_CLASS"));
		}
		catch (PropertyVetoException e)
		{
			logger.error("Failed to set driver class in data source", e);
		}
		shard1MasterDataSource.setJdbcUrl(prop.getProperty("RDBMS.SHARD1.MASTER.JDBC_URL"));
		shard1MasterDataSource.setUser(prop.getProperty("RDBMS.SHARD1.MASTER.USER"));
		shard1MasterDataSource.setPassword("RDBMS.SHARD1.MASTER.PASSWORD");
		// shard1MasterDataSource.setAcquireIncrement(Integer.parseInt(prop.getProperty("C3P0_ACQUIRE_INCREMENT")));
		// shard1MasterDataSource
		// .setAcquireRetryAttempts(Integer.parseInt(prop.getProperty("C3P0_ACQUIRE_RETRY_ATTEMPTS")));
		// shard1MasterDataSource.setAcquireRetryDelay(Integer.parseInt(prop.getProperty("C3P0_ACQUIRE_RETRY_DELAY")));
		// shard1MasterDataSource.setMaxIdleTime(Integer.parseInt(prop.getProperty("C3P0_MAX_IDLE_TIME")));
		// shard1MasterDataSource.setMaxPoolSize(Integer.parseInt(prop.getProperty("C3P0_MAX_POOL_SIZE")));
		// shard1MasterDataSource.setMaxStatements(Integer.parseInt(prop.getProperty("C3P0_MAX_STATEMENTS")));
		// shard1MasterDataSource.setMinPoolSize(Integer.parseInt(prop.getProperty("C3P0_MIN_POOL_SIZE")));
		// shard1MasterDataSource.setTestConnectionOnCheckin(Boolean.parseBoolean(prop
		// .getProperty("C3P0_TEST_CONNECTION_ON_CHECKIN")));
		// shard1MasterDataSource.setIdleConnectionTestPeriod(Integer.parseInt(prop
		// .getProperty("C3P0_IDLE_CONNECTION_TEST_PERIOD")));
		// shard1MasterDataSource.setPreferredTestQuery(prop.getProperty("C3P0_PREFERRED_TEST_QUERY"));

		RdbmsPersistenceManagerConfig shard1Config = new RdbmsPersistenceManagerConfig();
		shard1Config.setMaster(shard1MasterDataSource);

		RdbmsPersistenceManager shard1RdbmsPersistenceManager = new RdbmsPersistenceManager(shard1Config);

		Map<String, PersistenceManager> shardIdToPersistenceManagerMap = new HashMap<String, PersistenceManager>();
		shardIdToPersistenceManagerMap.put("01", shard1RdbmsPersistenceManager);

		ShardIdentifier rdbmsShardIdentifier = new RdbmsShardIdentifierForSingleShard();

		RdbmsMetaDataCache rdbmsMetaDataCache = RdbmsMetaDataCache.getInstance();

		RdbmsDataStoreConfig rdbmsDataStore = new RdbmsDataStoreConfig();
		rdbmsDataStore.setMetaDataCache(rdbmsMetaDataCache);
		rdbmsDataStore.setShardIdentifier(rdbmsShardIdentifier);
		rdbmsDataStore.setShardIdToPersistenceManagerMap(shardIdToPersistenceManagerMap);

		List<DataStoreType> defaultReadOrder = new ArrayList<DataStoreType>();
		defaultReadOrder.add(DataStoreType.RDBMS);

		ReadConfig defaultReadConfig = new ReadConfig();
		defaultReadConfig.setReadOrder(defaultReadOrder);

		List<DataStoreType> defaultWriteOrder = new ArrayList<DataStoreType>();
		defaultWriteOrder.add(DataStoreType.RDBMS);

		WriteConfig defaultWriteConfig = new WriteConfig();
		defaultWriteConfig.setFailureAction(FailureAction.CONTINUE);
		defaultWriteConfig.setWriteOrder(defaultWriteOrder);

		EntityPersistencePreference defaultPersistencePreference = new EntityPersistencePreference();
		defaultPersistencePreference.setReadConfig(defaultReadConfig);
		defaultPersistencePreference.setWriteConfig(defaultWriteConfig);

		Map<DataStoreType, DataStoreConfig> dataStoreConfigMap = new HashMap<DataStoreType, DataStoreConfig>();
		dataStoreConfigMap.put(DataStoreType.RDBMS, rdbmsDataStore);

		PersistenceLayer pl = new PersistenceLayer();
		pl.setDataStoreConfigMap(dataStoreConfigMap);
		pl.setDefaultPersistencePreference(defaultPersistencePreference);

		return pl;
	}
}
