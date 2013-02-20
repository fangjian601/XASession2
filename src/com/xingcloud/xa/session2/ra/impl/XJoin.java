package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Join;
import com.xingcloud.xa.session2.ra.Relation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.RowIterator;

import java.util.*;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XJoin extends AbstractOperation implements Join{

	RelationProvider left;
	RelationProvider right;

	public Relation evaluate() {
        if(result == null){
            Map<String, Integer> leftColumnIndex = left.getColumnIndex();
            Map<String, Integer> rightColumnIndex = right.getColumnIndex();
            Set<String> intersection = new HashSet<String>(leftColumnIndex.keySet());
            intersection.retainAll(rightColumnIndex.keySet());
            Map<String, Integer> columnIndex = new TreeMap<String, Integer>(leftColumnIndex);
            String[] rightColumns = new String[rightColumnIndex.size()];
            for(String column: rightColumnIndex.keySet()){
                rightColumns[rightColumnIndex.get(column)] = column;
            }
            int index = columnIndex.size();
            for(String column: rightColumns){
                if(!intersection.contains(column)){
                    columnIndex.put(column, index);
                    index++;
                }
            }
            if(intersection.size() > 0){
                List<Object[]> rows = new ArrayList<Object[]>();
                RowIterator leftIterator = left.iterator();
                while(leftIterator.hasNext()){
                    XRelation.XRow leftRow = (XRelation.XRow)leftIterator.nextRow();
                    RowIterator rightIterator = right.iterator();
                    while(rightIterator.hasNext()){
                        XRelation.XRow rightRow = (XRelation.XRow)rightIterator.nextRow();
                        boolean isRowEqual = true;
                        for(String column: intersection){
                            if(!leftRow.get(column).equals(rightRow.get(column))){
                                isRowEqual = false;
                                break;
                            }
                        }
                        if(isRowEqual){
                            Object[] rowData = new Object[columnIndex.size()];
                            for(String column: columnIndex.keySet()){
                                if(leftColumnIndex.containsKey(column)){
                                    rowData[columnIndex.get(column)] = leftRow.get(column);
                                } else {
                                    rowData[columnIndex.get(column)] = rightRow.get(column);
                                }
                            }
                            rows.add(rowData);
                        }
                    }
                }
                result = new XRelation(columnIndex, rows);
            } else {
                result = new XRelation(columnIndex, new ArrayList<Object[]>());
            }
        }
        return result;
	}

	public Join setInput(RelationProvider left, RelationProvider right) {
		resetInput();
        this.left = left;
		this.right = right;
		addInput(left);
		addInput(right);
		return this;
	}

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}

}
