package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.*;
import com.xingcloud.xa.session2.ra.expr.AggregationExpr;
import com.xingcloud.xa.session2.ra.expr.ColumnValue;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class XProjection extends AbstractOperation implements Projection{

	RelationProvider relation;
	Expression[] projections;

	public Relation evaluate() {
        if(result == null){
            Map<String, Integer> columnIndex = new TreeMap<String, Integer>();
            int index = 0;
            for(int i = 0; i < projections.length; i++) {
                StringBuilder sb = new StringBuilder();
                InlinePrint.printExpression(projections[i], sb);
                if(projections[i] instanceof ColumnValue && ((ColumnValue)projections[i]).columnName.equals("*")){
                    for(String column : relation.getColumnIndex().keySet()){
                        columnIndex.put('`'+column+'`', index+relation.getColumnIndex().get(column));
                    }
                    index+=relation.getColumnIndex().size();
                } else {
                    columnIndex.put(sb.toString(), index++);
                }

            }
            List<Object[]> evaluatedRows = new ArrayList<Object[]>();
            RowIterator iterator = relation.iterator();
            while(iterator.hasNext()){
                Row row = iterator.nextRow();
                Object[] evaluatedRow = new Object[columnIndex.size()];
                index = 0;
                for(int i = 0; i < projections.length; i++) {
                    if(projections[i] instanceof ColumnValue && ((ColumnValue)projections[i]).columnName.equals("*")){
                        for(Object data: ((XRelation.XRow)row).rowData){
                            evaluatedRow[index++] = data;
                        }
                    } else {
                        evaluatedRow[index++] = projections[i].evaluate(row);
                    }
                }
                evaluatedRows.add(evaluatedRow);
                boolean hasAggregation = false;
                for(Expression expr: projections){
                    if(expr instanceof AggregationExpr){
                        hasAggregation = true;
                        break;
                    }
                }
                if(hasAggregation) break;
            }
            result = new XRelation(columnIndex, evaluatedRows);
        }
		return result;
	}

	public Projection setInput(RelationProvider relation, Expression ... projections) {
        resetInput();
		this.relation = relation;
		this.projections = projections;
		addInput(relation);
		return this;
	}

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}
}
