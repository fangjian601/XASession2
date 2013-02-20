package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.*;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.util.ArrayList;
import java.util.List;


/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XSelection extends AbstractOperation implements Selection{

	RelationProvider relation;
	Expression expression;

	public Selection setInput(RelationProvider relation, Expression e) {
		resetInput();
		this.relation = relation;
		this.expression = e;
		addInput(relation);
		return this;
	}

	public Relation evaluate() {
        if(result == null) {
            List<Object[]> rows = new ArrayList<Object[]>();
            RowIterator iterator = relation.iterator();
            while(iterator.hasNext()){
                XRelation.XRow row = (XRelation.XRow) iterator.nextRow();
                if(expression == null){
                    rows.add(row.rowData);
                } else {
                    Object value = expression.evaluate(row);
                    if(value instanceof Boolean && ((Boolean) value).booleanValue()){
                        rows.add(row.rowData);
                    }
                }
            }
            result = new XRelation(relation.getColumnIndex(), rows);
        }
        return result;
	}

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}
}
