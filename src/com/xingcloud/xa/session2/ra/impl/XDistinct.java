package com.xingcloud.xa.session2.ra.impl;

import com.xingcloud.xa.session2.ra.Distinct;
import com.xingcloud.xa.session2.ra.Relation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.RowIterator;
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
 * Date:   2/6/13
 */
public class XDistinct extends AbstractOperation implements Distinct {

	RelationProvider relation;
	Expression[] expressions;

	public Relation evaluate() {
        if(result == null){
            if(expressions.length > 0){
                Map<String, Object[]> distinctMap = new HashMap<String, Object[]>();
                RowIterator iterator = relation.iterator();
                while(iterator.hasNext()){
                    XRelation.XRow row = (XRelation.XRow)iterator.nextRow();
                    StringBuilder key = new StringBuilder();
                    for(Expression e: expressions){
                        key.append(getMD5(e.evaluate(row).toString()));
                    }
                    if(!distinctMap.containsKey(key.toString())){
                        distinctMap.put(key.toString(), row.rowData);
                    }
                }
                result = new XRelation(relation.getColumnIndex(), new ArrayList<Object[]>(distinctMap.values()));
            } else {
                List<Object[]> rows = new ArrayList<Object[]>();
                RowIterator iterator = relation.iterator();
                while(iterator.hasNext()){
                    XRelation.XRow row = (XRelation.XRow)iterator.nextRow();
                    rows.add(row.rowData);
                }
                result = new XRelation(relation.getColumnIndex(), rows);
            }
        }
		return result;
	}

	public Distinct setInput(RelationProvider relation, Expression ... expressions ) {
		resetInput();
        this.relation = relation;
		this.expressions = expressions;
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
