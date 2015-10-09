package tomekkup.helenos.types.qx.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ******************************************************** Copyright: 2012
 * Tomek Kuprowski
 *
 * License: GPLv2: http://www.gnu.org/licences/gpl.html
 *
 * @author Tomek Kuprowski (tomekkuprowski at gmail dot com) * * *
 * ****************************************************
 */
public class QxCombinedQuery<K, N, V> extends AbstractQuery<K, N, V> {

	public static final int DEFAULT_ROW_COUNT = 100;
	private List<QxCombinedQueryParam> paramList;
	private int rowCount = DEFAULT_ROW_COUNT;

	public QxCombinedQuery() {
		super();
	}

	public QxCombinedQuery(Class<K> keyClass, Class<N> nameClass, String keyspace, String columnFamily,
			List<String> columnNames, String keyFrom, String keyTo, int rowCount) {
		super(keyClass, nameClass, keyspace, columnFamily);
		this.rowCount = rowCount;
		this.setParamList(new ArrayList<QxCombinedQueryParam>());
	}

	public QxCombinedQuery(Class<K> keyClass, Class<N> nameClass, String keyspace, String columnFamily,
			String nameStart, String nameEnd, int colLimit, boolean reversed, String keyFrom, String keyTo,
			int rowCount) {
		super(keyClass, nameClass, keyspace, columnFamily);
		this.rowCount = rowCount;
		this.setParamList(new ArrayList<QxCombinedQueryParam>());
	}

	public int getRowCount() {
		return rowCount;
	}

	public void setRowCount(int rowCount) {
		this.rowCount = rowCount;
	}

	public List<QxCombinedQueryParam> getParamList() {
		return paramList;
	}

	public void setParamList(List<QxCombinedQueryParam> paramList) {
		this.paramList = paramList;
	}

}
