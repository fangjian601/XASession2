package com.xingcloud.xa.session2.ra.expr;

import com.xingcloud.xa.session2.ra.Row;

/**
 * Author: mulisen
 * Date:   2/6/13
 */
public class Multiple implements Expression {

	Expression left;
	Expression right;

	public Multiple(Expression left, Expression right) {
		this.left = left;
		this.right = right;
	}

	public Object evaluate(Row input) {
		return (Double)left.evaluate(input) * (Double)right.evaluate(input);
	}
}
