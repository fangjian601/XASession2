package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.*;
import com.xingcloud.xa.session2.ra.expr.AggregationExpr;
import com.xingcloud.xa.session2.ra.expr.Expression;
import com.xingcloud.xa.session2.util.StringUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author: mulisen
 * Date:   2/7/13
 */
public class XGroup extends AbstractOperation implements Group {


	RelationProvider relation;

	Expression[] groupingExpressions;

	Expression[] projectionExpressions;

	public Relation evaluate() {
        if(result == null){
            RowIterator iterator = relation.iterator();
            Map<String, List<Object[]>> groups = new HashMap<String, List<Object[]>>();
            while(iterator.hasNext()){
                XRelation.XRow row = (XRelation.XRow)iterator.nextRow();
                StringBuilder key = new StringBuilder();
                for(Expression expr: groupingExpressions){
                    key.append(StringUtil.getMD5(expr.evaluate(row).toString()));
                }
                if(groups.containsKey(key.toString())){
                    List<Object[]> rows = groups.get(key.toString());
                    rows.add(row.rowData);
                } else {
                    List<Object[]> rows = new ArrayList<Object[]>();
                    rows.add(row.rowData);
                    groups.put(key.toString(), rows);
                }
            }
            List<Object[]> rows = new ArrayList<Object[]>();
            Map<String, Integer> columnIndex = null;
            for(List<Object[]> groupRows: groups.values()){
                XRelation groupRelation = new XRelation(relation.getColumnIndex(), groupRows);
                updateProjectionExpressions(groupRelation);
                Projection projection = PlanFactory.getInstance().newProjection();
                projection.setInput(groupRelation, projectionExpressions);
                Relation projectionRelation = projection.evaluate();
                if(columnIndex == null) columnIndex = projectionRelation.getColumnIndex();
                RowIterator projectionIterator = projectionRelation.iterator();
                if(projectionIterator.hasNext()){
                    XRelation.XRow projectionRow = (XRelation.XRow) projectionIterator.nextRow();
                    rows.add(projectionRow.rowData);
                }
            }
            result = new XRelation(columnIndex, rows);
        }
		return result;
	}

    public Group setInput(RelationProvider relation, Expression[] groupingExpressions, Expression[] projectionExpressions) {
		resetInput();
        this.relation = relation;
		this.groupingExpressions = groupingExpressions;
		this.projectionExpressions = projectionExpressions;
		addInput(relation);
        return this;
    }

	@Override
	public String toString() {
		return IndentPrint.print(this);
	}

    private void updateProjectionExpressions(RelationProvider relationProvider){
        for(Expression expr: projectionExpressions){
            if(expr instanceof AggregationExpr){
                Aggregation aggregation = ((AggregationExpr) expr).aggregation;
                List<RelationProvider> inputs = ((AbstractAggregation)aggregation).getInputs();
                RelationProvider oldRelationProvider = inputs.get(0);
                if(oldRelationProvider instanceof XDistinct){
                    XDistinct distinct = (XDistinct)oldRelationProvider;
                    distinct.setInput(relationProvider, distinct.expressions);
                    distinct.result = null;
                } else{
                    if(aggregation instanceof XCount){
                        XCount count = (XCount)aggregation;
                        count.setInput(relationProvider);
                    } else if (aggregation instanceof XSum){
                        XSum sum = (XSum)aggregation;
                        sum.setInput(relationProvider, sum.columnName);
                    }
                }
            }
        }
    }
}
