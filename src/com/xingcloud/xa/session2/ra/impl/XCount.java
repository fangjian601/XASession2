package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Aggregation;
import com.xingcloud.xa.session2.ra.Count;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.RowIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Author: mulisen
 * Date:   2/7/13
 */
public class XCount extends AbstractAggregation implements Count {
	RelationProvider relation;
	public Aggregation setInput(RelationProvider relation) {
        resetInput();
		init();
		this.relation = relation;
		addInput(relation);
		return this;
	}

	public Object aggregate() {
        int count = 0;
        RowIterator iterator = relation.iterator();
        while(iterator.hasNext()){
            count++;
            iterator.nextRow();
        }
        return count;
	}

	public void init() {
		//TODO method implementation
	}
}
