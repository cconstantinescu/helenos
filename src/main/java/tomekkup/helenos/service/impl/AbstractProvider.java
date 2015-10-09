package tomekkup.helenos.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.dozer.Mapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.Assert;

import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.SerializerTypeInferer;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.factory.HFactory;
import tomekkup.helenos.cassandra.model.AllConsistencyLevelPolicy;
import tomekkup.helenos.types.Column;
import tomekkup.helenos.types.Column.ColumnKeyType;
import tomekkup.helenos.types.qx.query.Query;

/**
 * ********************************************************
 * Copyright: 2012 Tomek Kuprowski
 *
 * License: GPLv2: http://www.gnu.org/licences/gpl.html
 *
 * @author Tomek Kuprowski (tomekkuprowski at gmail dot com)
 * *******************************************************
 */
public abstract class AbstractProvider {

	@Autowired
	protected Mapper mapper;

	@Autowired
	protected ObjectMapper objectMapper;

	protected ConsistencyLevelPolicy consistencyLevelPolicy;

	protected Cluster cluster;

	private com.datastax.driver.core.Cluster clusterDatastax;

	protected <V> Serializer<V> getSerializer(Class<V> clazz) {
		if (clazz == null)
			return SerializerTypeInferer.getSerializer(String.class);
		Serializer<V> serializer = SerializerTypeInferer.getSerializer(clazz);
		if (serializer.getClass().equals(ObjectSerializer.class)) {
			throw new IllegalStateException("can not obtain correct serializer for class: " + clazz);
		}
		return serializer;
	}

	protected Keyspace getKeyspace(String keyspaceName, String consistencyLevel) {
		Assert.notNull(cluster, "connection not ready yet");
		return HFactory.createKeyspace(keyspaceName, cluster, this.resolveCLP(consistencyLevel));
	}

	protected Keyspace getKeyspace(Query query) {
		Assert.notNull(cluster, "connection not ready yet");

		return HFactory.createKeyspace(query.getKeyspace(), cluster, this.resolveCLP(query.getConsistencyLevel()));
	}

	private ConsistencyLevelPolicy resolveCLP(String consistencyLevelStr) {
		if ("ONE".equals(consistencyLevelStr)) {
			return AllConsistencyLevelPolicy.getInstance(HConsistencyLevel.ONE);
        } else
        if ("TWO".equals(consistencyLevelStr)) {
            return AllConsistencyLevelPolicy.getInstance(HConsistencyLevel.TWO);
        } else
        if ("THREE".equals(consistencyLevelStr)) {
            return AllConsistencyLevelPolicy.getInstance(HConsistencyLevel.THREE);
        } else
        if ("QUORUM".equals(consistencyLevelStr)) {
            return AllConsistencyLevelPolicy.getInstance(HConsistencyLevel.QUORUM);
        } else
        if ("ALL".equals(consistencyLevelStr)) {
            return AllConsistencyLevelPolicy.getInstance(HConsistencyLevel.ALL);
        } else
        if ("ANY".equals(consistencyLevelStr)) {
            return AllConsistencyLevelPolicy.getInstance(HConsistencyLevel.ANY);
        } else
        if ("LOCAL_QUORUM".equals(consistencyLevelStr)) {
            return AllConsistencyLevelPolicy.getInstance(HConsistencyLevel.LOCAL_QUORUM);
        } else
        if ("EACH_QUORUM".equals(consistencyLevelStr)) {
            return AllConsistencyLevelPolicy.getInstance(HConsistencyLevel.EACH_QUORUM);
		}
		throw new IllegalStateException("unknown consistency level");
	}

	@Required
	public void setMapper(Mapper mapper) {
		this.mapper = mapper;
	}

	@Required
    public void setConsistencyLevelPolicy(ConsistencyLevelPolicy consistencyLevelPolicy) {
        this.consistencyLevelPolicy = consistencyLevelPolicy;
    }
	public final void setNewCluster(Cluster cluster) {
		this.cluster = cluster;
		Set<CassandraHost> hosts = this.cluster.getConnectionManager().getHosts();
		if (hosts.size() > 0) {
			String[] hostsStr = new String[hosts.size()];
			int i = 0;
			for (CassandraHost host : hosts) {
				hostsStr[i++] = host.getHost();
			}
			Map<String, String> credentials = cluster.getCredentials();
			String password = credentials.get("password");
			String username = credentials.get("username");
			Builder builder = com.datastax.driver.core.Cluster.builder().addContactPoints(hostsStr);
			if (StringUtils.isNotBlank(username)) {
				builder.withCredentials(username, password);
			}
			clusterDatastax = builder.build();
		}
	}

	@Required
	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public Session getNewCQLSession() {
		Assert.notNull(clusterDatastax, "connection not ready yet");
		Session session = clusterDatastax.connect();
		return session;
	}
	
	protected List<Column<String, Object>> getColumnsByType(String keyspace, String columnFamily,
			ColumnKeyType requiredType, Session session) {
		// fetches the column metadata for the columnfamily selected, to find
		// out the keys
		ResultSet resultColumnsDefinition = session
				.execute("SELECT * from system.schema_columns where keyspace_name = '" + keyspace
						+ "' and columnfamily_name='" + columnFamily + "'");
		List<Column<String, Object>> columns = new ArrayList<Column<String, Object>>();
		// creates a list of partition keys first, then clustering keys
		for (Row row : resultColumnsDefinition) {
			Column<String, Object> column = new Column<String, Object>();
			ColumnKeyType type = ColumnKeyType.fromName(row.getString("type"));
			if (requiredType == null || type == requiredType) {
				column.setType(type);
				column.setName(row.getString("column_name"));
				column.setComponentIndex(row.getInt("component_index"));
				columns.add(column);
			}
		}
		if (requiredType == ColumnKeyType.PARTITION_KEY && columns.isEmpty()) {
			throw new IllegalArgumentException("The column family " + columnFamily + " doesn't have a partition key.");
		}
		return columns;
	}
}
