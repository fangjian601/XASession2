package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Aggregation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.RowIterator;
import com.xingcloud.xa.session2.ra.Sum;

/**
 * Author: mulisen
 * Date:   2/7/13
 */
public class XSum extends AbstractAggregation implements Sum {

	RelationProvider relation;
	String columnName;
	public Aggregation setInput(RelationProvider relation, String columnName) {
        resetInput();
		init();
		this.relation = relation;
		this.columnName = columnName;
		addInput(relation);
		return this;
	}

	public Object aggregate() {
		double sum = 0;
        RowIterator iterator = relation.iterator();
        while(iterator.hasNext()){
            XRelation.XRow row = (XRelation.XRow)iterator.nextRow();
            if(row.get(columnName) != null){
                sum += Double.parseDouble(row.get(columnName).toString());
            }
        }
        return sum;
	}

	public void init() {
		//TODO method implementation
	}

}
