package tomekkup.helenos.types;

import java.util.ArrayList;
import java.util.List;

/**
 * ********************************************************
 * Copyright: 2012 Tomek Kuprowski
 *
 * License: GPLv2: http://www.gnu.org/licences/gpl.html
 *
 * @author Tomek Kuprowski (tomekkuprowski at gmail dot com)
 * *******************************************************
 */
public class Slice<K,N,V> {

    private K key;
    private List<Column<N,V>> columns = new ArrayList<Column<N,V>>();
    private List<Slice<K,N,V>> slices = new ArrayList<Slice<K,N,V>>();

    public Slice() {
        super();
    }
    
    public Slice(K key, List<Column<N,V>> columns) {
        setKey(key);
        setColumns(columns);
    }
    
    public Slice<K,N,V> getByKey(Object key){
    	for (Slice<K,N,V> slice : slices){
    		if( slice.key.equals(key)){
    			return slice;
    		}
    	}
    	return null;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public List<Column<N,V>> getColumns() {
        return columns;
    }

    public void setColumns(List<Column<N,V>> columns) {
        this.columns = columns;
    }

	public List<Slice<K,N,V>> getSlices() {
		return slices;
	}

	public void addSlice(Slice<K,N,V> slice) {
		this.slices.add(slice);
	}
}
