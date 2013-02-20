package com.xingcloud.xa.session2.exec;

import com.xingcloud.xa.session2.ra.Operation;
import com.xingcloud.xa.session2.ra.RelationProvider;
import com.xingcloud.xa.session2.ra.impl.AbstractOperation;

import java.util.ArrayList;
import java.util.List;
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
		return root.evaluate();
	}
}
