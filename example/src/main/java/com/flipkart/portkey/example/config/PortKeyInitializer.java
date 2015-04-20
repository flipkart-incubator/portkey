package com.flipkart.portkey.example.config;

import java.beans.PropertyVetoException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.flipkart.portkey.common.entity.persistence.EntityPersistencePreference;
import com.flipkart.portkey.common.entity.persistence.ReadConfig;
import com.flipkart.portkey.common.entity.persistence.WriteConfig;
import com.flipkart.portkey.common.enumeration.DataStoreType;
import com.flipkart.portkey.persistence.PersistenceLayer;
import com.flipkart.portkey.rdbms.persistence.RdbmsShardingManager;
import com.flipkart.portkey.rdbms.persistence.RdbmsSingleShardedDatabaseConfig;
import com.flipkart.portkey.rdbms.persistence.config.RdbmsConnectionConfig;
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
		ComboPooledDataSource master = new ComboPooledDataSource();
		Properties prop = loadProperties("target/classes/project.properties");
		try
		{
			master.setDriverClass(prop.getProperty("RDBMS.SHARD1.MASTER.DRIVER_CLASS"));
		}
		catch (PropertyVetoException e)
		{
			logger.error("Failed to set driver class in data source", e);
		}
		master.setJdbcUrl(prop.getProperty("RDBMS.SHARD1.MASTER.JDBC_URL"));
		master.setUser(prop.getProperty("RDBMS.SHARD1.MASTER.USER"));
		master.setPassword(prop.getProperty("RDBMS.SHARD1.MASTER.PASSWORD"));
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

		RdbmsConnectionConfig connectionConfig = new RdbmsConnectionConfig();
		connectionConfig.setMaster(master);

		RdbmsSingleShardedDatabaseConfig dbConfig = new RdbmsSingleShardedDatabaseConfig(connectionConfig);
		RdbmsShardingManager shardingManager = new RdbmsShardingManager();
		shardingManager.addDatabaseConfig("portkey_example", dbConfig);

		PersistenceLayer pl = new PersistenceLayer();
		pl.addShardingManager(DataStoreType.RDBMS, shardingManager);

		ReadConfig readConfig = new ReadConfig();
		List<DataStoreType> readOrder = new ArrayList<DataStoreType>();
		readOrder.add(DataStoreType.RDBMS);
		readConfig.setReadOrder(readOrder);

		List<DataStoreType> writeOrder = new ArrayList<DataStoreType>();
		writeOrder.add(DataStoreType.RDBMS);
		WriteConfig writeConfig = new WriteConfig();
		writeConfig.setWriteOrder(writeOrder);

		EntityPersistencePreference defaultPersistencePreference = new EntityPersistencePreference();
		defaultPersistencePreference.setReadConfig(readConfig);
		defaultPersistencePreference.setWriteConfig(writeConfig);

		pl.setDefaultPersistencePreference(defaultPersistencePreference);
		pl.initialize(false);
		return pl;
	}
}
