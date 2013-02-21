package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.*;
import com.xingcloud.xa.session2.util.StringUtil;

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
                Map<String, List<XRelation.XRow>> rightMap = new HashMap<String, List<XRelation.XRow>>();
                RowIterator leftIterator = left.iterator();
                RowIterator rightIterator = right.iterator();
                while(rightIterator.hasNext()){
                    XRelation.XRow rightRow = (XRelation.XRow)rightIterator.nextRow();
                    StringBuilder sb = new StringBuilder();
                    for(String column: intersection){
                        sb.append(StringUtil.getMD5(rightRow.get(column).toString()));
                    }
                    if(rightMap.containsKey(sb.toString())){
                        List<XRelation.XRow> list = rightMap.get(sb.toString());
                        list.add(rightRow);
                    } else {
                        List<XRelation.XRow> list = new ArrayList<XRelation.XRow>();
                        list.add(rightRow);
                        rightMap.put(sb.toString(), list);
                    }
                }
                while(leftIterator.hasNext()){
                    XRelation.XRow leftRow = (XRelation.XRow)leftIterator.nextRow();
                    StringBuilder sb = new StringBuilder();
                    for(String column: intersection){
                        sb.append(StringUtil.getMD5(leftRow.get(column).toString()));
                    }
                    if(rightMap.containsKey(sb.toString())){
                        List<XRelation.XRow> rightList = rightMap.get(sb.toString());
                        for(XRelation.XRow rightRow: rightList){
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
