package hw1;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute() throws IllegalArgumentException {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}

		if (statement == null || !(statement instanceof Select)) {
			throw new IllegalArgumentException("Invalid SQL query");
		}

		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		
		Table mainTable = (Table) sb.getFromItem();
		Relation mainRelation = Database.getCatalog().getRelation(mainTable.getName());
		
		// Handle JOIN
		List<Join> joins = sb.getJoins();
		if (joins != null) {
			for (Join j : joins) {
				Table joinTable = (Table) j.getRightItem();
				Relation joinRelation = Database.getCatalog().getRelation(joinTable.getName());

				mainRelation = mainRelation.join(joinRelation, j.getOnExpression());
			}
		}

		// Handle the WHERE clause
		Expression where = sb.getWhere();
		if (where != null) {
			WhereExpressionVisitor wev = new WhereExpressionVisitor();
			where.accept(wev);

			String colName = wev.getLeft();
			Field val = wev.getRight();
			RelationalOperator op = wev.getOp();

			int fieldIndex = getIndexOfField(mainRelation, colName);

			mainRelation = mainRelation.select(fieldIndex, op, val);

			 
		}

		// Handle the SELECT clause
		List<SelectItem> selectItems = sb.getSelectItems();
		ArrayList<Integer> fields = new ArrayList<>();
		ArrayList<String> names = new ArrayList<>();

		if (selectItems != null) {
			for (SelectItem si : selectItems) {
				if (si.toString().equals("*")) {
					for (int i = 0; i < mainRelation.getDesc().numFields(); i++) {
						fields.add(i);
						names.add(mainRelation.getDesc().getFieldName(i));
					}
				} else {
					if (si instanceof SelectExpressionItem) {
						SelectExpressionItem sei = (SelectExpressionItem) si;
						if (sei.getAlias() != null) {
							names.add(sei.getAlias().getName());
						} else {
							names.add(sei.getExpression().toString());
						}
					} else {
						names.add(si.toString());
					}

					String colName = si.toString();
					int fieldIndex = getIndexOfField(mainRelation, colName);
					if (fieldIndex == -1) {
						throw new IllegalArgumentException("Invalid column name");
					}
					fields.add(fieldIndex);
				}
			}

			mainRelation = mainRelation.project(fields, names);
		}

		// GROUP BY
		List<Expression> groupBy = sb.getGroupByColumnReferences();
		AggregateOperator aggOp = null;

		for (SelectItem si : sb.getSelectItems()) {
			if (si.toString().contains("COUNT")) {
				aggOp = AggregateOperator.COUNT;
			} else if (si.toString().contains("AVG")) {
				aggOp = AggregateOperator.AVG;
			} else if (si.toString().contains("SUM")) {
				aggOp = AggregateOperator.SUM;
			} else if (si.toString().contains("MIN")) {
				aggOp = AggregateOperator.MIN;
			} else if (si.toString().contains("MAX")) {
				aggOp = AggregateOperator.MAX;
			}

			if (aggOp != null) {
				break;
			}
		}

		if (aggOp == null) {
			return mainRelation;
		}

		if (groupBy != null && groupBy.size() > 0) {
			mainRelation = mainRelation.aggregate(aggOp, true);
		} else {
			mainRelation = mainRelation.aggregate(aggOp, false);
		}

		return mainRelation;
	}

	private int getIndexOfField(Relation relation, String colName) {
		TupleDesc td = relation.getDesc();
		for (int i = 0; i < td.numFields(); i++) {
			if (td.getFieldName(i).equals(colName)) {
				return i;
			}
		}
		return -1;
	}

}

