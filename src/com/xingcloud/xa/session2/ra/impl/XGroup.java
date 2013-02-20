package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.*;
import com.xingcloud.xa.session2.ra.expr.Expression;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
                    key.append(getMD5(expr.evaluate(row).toString()));
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

    private String getMD5(String input){
        MessageDigest m = null;
        try {
            m = MessageDigest.getInstance("MD5");
            m.reset();
            m.update(input.getBytes());
            byte[] digest = m.digest();
            BigInteger bigInt = new BigInteger(1,digest);
            String hashText = bigInt.toString(16);
            while(hashText.length() < 32 ){
                hashText = "0"+hashText;
            }
            return hashText;
        } catch (NoSuchAlgorithmException e) {
            return "";
        }
    }

}
