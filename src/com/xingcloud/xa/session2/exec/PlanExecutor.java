package com.xingcloud.xa.session2.exec;

import com.xingcloud.xa.session2.ra.Operation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.RowIterator;
import com.xingcloud.xa.session2.ra.impl.AbstractOperation;
import com.xingcloud.xa.session2.ra.impl.XRelation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class PlanExecutor {

	public static RelationProvider executePlan(Operation root){
        List<AbstractOperation> inputs = new ArrayList<AbstractOperation>();
        Stack<RelationProvider> tempStack = new Stack<RelationProvider>();
        tempStack.push(root);
        while(!tempStack.empty()){
            RelationProvider relationProvider = tempStack.pop();
            if(relationProvider instanceof AbstractOperation){
                if(((AbstractOperation) relationProvider).getInputs().size() == 0){
                    inputs.add((AbstractOperation) relationProvider);
                }
                for(int i=0; i<((AbstractOperation) relationProvider).getInputs().size(); i++){
                    tempStack.push(((AbstractOperation) relationProvider).getInputs().get(i));
                }
            }
        }
        for(AbstractOperation operation : inputs){
            operation.evaluate();
        }
        printResult((XRelation)root.evaluate());
		return null;
	}

    public static void printResult(XRelation relation){
        StringBuilder sb = new StringBuilder();
        Map<String, Integer> columnIndex = relation.getColumnIndex();
        String[] columns = new String[columnIndex.size()];
        for(String column: columnIndex.keySet()){
            columns[columnIndex.get(column)] = column;
        }
        for(String column: columns){
            sb.append(column+"\t");
        }
        sb.append("\n");
        RowIterator iterator = relation.iterator();
        while(iterator.hasNext()){
            XRelation.XRow row = (XRelation.XRow) iterator.nextRow();
            for(Object data: row.rowData){
                sb.append(data+"\t");
            }
            sb.append("\n");
        }
        System.out.println(sb.toString());
    }
}
