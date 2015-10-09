package tomekkup.helenos.types;

/**
 * ******************************************************** Copyright: 2012
 * Tomek Kuprowski
 *
 * License: GPLv2: http://www.gnu.org/licences/gpl.html
 *
 * @author Tomek Kuprowski (tomekkuprowski at gmail dot com) *
 * ******************************************************
 */
public class Column<N, V> implements Comparable<Column<N, V>> {

	private N name;
	private V value;
	private ColumnKeyType type;
	private int componentIndex;
	private long clock;
	private int ttl;

	public Column() {
	}

	public N getName() {
		return name;
	}

	public void setName(N name) {
		this.name = name;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}

	public long getClock() {
		return clock;
	}

	public void setClock(long clock) {
		this.clock = clock;
	}

	public int getTtl() {
		return ttl;
	}

	public void setTtl(int ttl) {
		this.ttl = ttl;
	}

	public ColumnKeyType getType() {
		return type;
	}

	public void setType(ColumnKeyType type) {
		this.type = type;
	}

	public int getComponentIndex() {
		return componentIndex;
	}

	public void setComponentIndex(int componentIndex) {
		this.componentIndex = componentIndex;
	}

	public enum ColumnKeyType {
		PARTITION_KEY("partition_key"), CLUSTERING_KEY("clustering_key"), REGULAR("regular");
		private String name;

		ColumnKeyType(String name) {
			this.name = name;
		}

		public static ColumnKeyType fromName(String name) {
			for (ColumnKeyType type : values()) {
				if (type.name.equals(name)) {
					return type;
				}
			}
			return null;
		}
	}

	@Override
	public int compareTo(Column<N, V> o) {
		if (this.getType() == o.getType()) {
			return Integer.compare(this.getComponentIndex(), o.getComponentIndex());
		}
		if (this.getType() == ColumnKeyType.PARTITION_KEY) {
			return -1;
		}
		if (this.getType() == ColumnKeyType.CLUSTERING_KEY && o.getType() == ColumnKeyType.REGULAR) {
			return -1;
		}
		return 1;
	}
}
