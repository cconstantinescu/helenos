package tomekkup.helenos.service.query.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import tomekkup.helenos.service.ClusterConfigAware;
import tomekkup.helenos.service.impl.AbstractQueryProvider;
import tomekkup.helenos.service.query.StandardQueryProvider;
import tomekkup.helenos.types.Column;
import tomekkup.helenos.types.Column.ColumnKeyType;
import tomekkup.helenos.types.Slice;
import tomekkup.helenos.types.qx.query.AbstractColumnQuery;
import tomekkup.helenos.types.qx.query.AbstractQuery;
import tomekkup.helenos.types.qx.query.QxCqlQuery;
import tomekkup.helenos.types.qx.query.QxPredicateQuery;
import tomekkup.helenos.types.qx.query.QxRangeQuery;
import tomekkup.helenos.types.qx.query.AbstractColumnQuery.ColumnsModeType;

@Component("datastaxQueryProvider")
public class DatastaxQueryProviderImpl extends AbstractQueryProvider
		implements StandardQueryProvider, ClusterConfigAware {

	public DatastaxQueryProviderImpl() {
		super();
		super.logger = Logger.getLogger(this.getClass());
	}

	@Override
	public <K, N, V> List<Slice<K, N, V>> cql(QxCqlQuery<K, N, V> query) {

		logQueryObject(query);

		Session session = getNewCQLSession();
		try {
			return executeQuery((AbstractQuery<K, String, V>) query, session, query.getQuery());
		} finally {
			session.close();
		}
	}

	private <V, K, N> List<Slice<K, N, V>> executeQuery(AbstractQuery<K, String, V> queryParam, Session session,
			String query, Object... bind) {
		List<String> allKeys = new ArrayList<String>();
		String keyspace = queryParam.getKeyspace();
		String consistencyLevel = queryParam.getConsistencyLevel();
		String columnFamily = queryParam.getColumnFamily();
		List<Column<String, Object>> allColumnTypes = getColumnsByType(keyspace, columnFamily, null, session);
		Collections.sort(allColumnTypes, new Comparator<Column<String, Object>>() {
			@Override
			public int compare(Column<String, Object> o1, Column<String, Object> o2) {
				if (o1.getType() == o2.getType()) {
					return 0;
				}
				if (o1.getType() == ColumnKeyType.PARTITION_KEY) {
					return -1;
				}
				if (o1.getType() == ColumnKeyType.CLUSTERING_KEY && o2.getType() == ColumnKeyType.REGULAR) {
					return -1;
				}
				return 1;
			}
		});
		for (Column<String, Object> column : allColumnTypes) {
			if (column.getType() == ColumnKeyType.CLUSTERING_KEY || column.getType() == ColumnKeyType.PARTITION_KEY) {
				allKeys.add(column.getName());
			}
		}
		session.execute("USE " + keyspace);

		PreparedStatement preparedStmt = session.prepare(query);
		preparedStmt.setConsistencyLevel(ConsistencyLevel.valueOf(consistencyLevel));
		ResultSet result = session.execute(preparedStmt.bind(bind));

		Slice<K, N, V> ret = new Slice<K, N, V>();
		for (Row row : result) {
			ColumnDefinitions columnDefinitions = row.getColumnDefinitions();
			List<Column<String, Object>> allColumns = new ArrayList<Column<String, Object>>();
			for (ColumnDefinitions.Definition columnDefinition : columnDefinitions) {
				if (!columnDefinition.getTable().equals(columnFamily)) {
					// this is the case when the user selects one columnfamily
					// to query, but changes it before executing.
					throw new IllegalArgumentException(
							"The column family for the query is not the same selected before.");
				}
				Column<String, Object> column = new Column<String, Object>();
				String name = columnDefinition.getName();
				column.setName(name);
				column.setValue(row.getObject(column.getName()));
				allColumns.add(column);
			}
			// The result is grouped by the keys (first the partition keys, then
			// the clustering keys) in a form of a tree.
			String rootkeyColumn = allKeys.get(0);
			String key = rootkeyColumn + "=" + row.getObject(rootkeyColumn);
			Slice root = ret.getByKey(key);
			if (root == null) {
				root = new Slice();
				root.setKey(key);
				ret.addSlice(root);
			}
			Slice child = root;
			for (int i = 1; i < allKeys.size(); i++) {
				String keyColumn = allKeys.get(i);
				key = keyColumn + "=" + row.getObject(keyColumn);
				child = root.getByKey(key);
				if (child == null) {
					child = new Slice();
					child.setKey(key);
					root.addSlice(child);
				}
				root = child;
			}
			child.setColumns(allColumns);

		}
		return ret.getSlices();

	}

	@Override
	public <K, N, V> List<Slice<K, N, V>> predicate(QxPredicateQuery<K, N, V> query) {
		return predicateCQL((QxPredicateQuery<K, String, V>) query);
	}

	public <K, N, V> List<Slice<K, N, V>> predicateCQL(QxPredicateQuery<K, String, V> query) {
		logQueryObject(query);
		Session session = getNewCQLSession();
		try {
			List<Column<String, Object>> keyColumns = getColumnsByType(query.getKeyspace(), query.getColumnFamily(),
					ColumnKeyType.PARTITION_KEY, session);
			StringBuilder sb = new StringBuilder();

			sb.append("SELECT ");
			handleColumns(sb, query, session);
			sb.append(" FROM ").append(query.getKeyspace()).append(".").append(query.getColumnFamily());
			sb.append(" WHERE ");
			for (Column<String, Object> keyColumn : keyColumns) {
				sb.append("\"").append(keyColumn.getName()).append("\" = ? ");
			}
			return executeQuery(query, session, sb.toString(), query.getKey());
		} finally {
			session.close();
		}
	}

	@Override
	public <K, N, V> List<Slice<K, N, V>> keyRange(QxRangeQuery<K, N, V> query) {
		return keyRangeCQL((QxRangeQuery<K, String, V>) query);
	}

	public <K, N, V> List<Slice<K, N, V>> keyRangeCQL(QxRangeQuery<K, String, V> query) {
		logQueryObject(query);
		Session session = getNewCQLSession();
		try {
			StringBuilder sb = new StringBuilder();

			sb.append("SELECT ");
			handleColumns(sb, query, session);
			sb.append(" FROM ").append(query.getKeyspace()).append(".").append(query.getColumnFamily());
			List<Object> binds = new ArrayList<Object>();
			if (query.getKeyFrom() != null) {
				addClause(binds, sb);
				sb.append("\"").append(query.getSelectedKeyColumn()).append("\" >= ? ");
				binds.add(query.getKeyFrom());
			}
			if (query.getKeyTo() != null ) {
				addClause(binds, sb);
				sb.append("\"").append(query.getSelectedKeyColumn()).append("\" <= ? ");
				binds.add(query.getKeyTo());
			}
			sb.append(" LIMIT ").append(query.getRowCount());
			sb.append(" ALLOW FILTERING ");
			return executeQuery(query, session, sb.toString(), binds.toArray());
		} finally {
			session.close();
		}
	}

	private void addClause(List<Object> binds, StringBuilder sb) {
		if (binds.size() > 0) {
			sb.append(" AND ");
		} else {
			sb.append(" WHERE ");
		}
	}

	private <K, V> void handleColumns(StringBuilder sb, AbstractColumnQuery<K, String, V> query, Session session) {
		query.setNameClass(String.class);

		if (query.getColumnsMode() == ColumnsModeType.ALL) {
			sb.append(" * ");
			return;
		}
		List<String> columns;
		List<Column<String, Object>> allColumnTypes = getColumnsByType(query.getKeyspace(), query.getColumnFamily(),
				null, session);
		if (query.getColumnsMode() == ColumnsModeType.RANGE) {
			columns = new ArrayList<String>();
			for (Column<String, Object> keyColumn : allColumnTypes) {
				// always add the clustering and partition keys.
				if (keyColumn.getType() == ColumnKeyType.CLUSTERING_KEY
						|| keyColumn.getType() == ColumnKeyType.PARTITION_KEY) {
					columns.add(keyColumn.getName());
				} else {
					// for regular columns, respect the limit and the parameters
					// to filter
					if (columns.size() == query.getLimit()) {
						continue;
					}
					if ((StringUtils.isBlank(query.getNameStart())
							|| keyColumn.getName().compareTo(query.getNameStart()) >= 0)
							&& (StringUtils.isEmpty(query.getNameEnd())
									|| keyColumn.getName().compareTo(query.getNameEnd()) <= 0)) {
						columns.add(keyColumn.getName());
					}
				}
			}
			if (columns.isEmpty()) {
				throw new IllegalArgumentException("No column was found from with given criteria");
			}
			if (query.isReversed()) {
				Collections.reverse(columns);
			}
		} else {
			// get the columns provided by the user and adds the clustering and
			// partition keys columns, if not there yet
			Set<String> uniqueColumns = new HashSet<String>();
			for (String column : query.getColumnNames()) {
				uniqueColumns.add(column.trim());
			}
			for (Column<String, Object> column : allColumnTypes) {
				if (column.getType() == ColumnKeyType.CLUSTERING_KEY
						|| column.getType() == ColumnKeyType.PARTITION_KEY) {
					uniqueColumns.add(column.getName());
				}
			}
			columns = new ArrayList<String>(uniqueColumns);
		}
		for (int i = 0; i < columns.size(); i++) {
			String columnName = columns.get(i);
			if (i != 0) {
				sb.append(", ");
			}
			sb.append("\"").append(columnName.trim()).append("\"");
		}
	}

}