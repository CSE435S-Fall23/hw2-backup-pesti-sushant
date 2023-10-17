// Sushant Ramesh and Anders Pesti

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
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
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
			System.out.println("Parsed SQL query: " + q);
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
		int mainTableId = Database.getCatalog().getTableId(mainTable.getName());
		HeapFile mainRelationFile = Database.getCatalog().getDbFile(mainTableId);
		
		ArrayList<Tuple> mainTuples = mainRelationFile.getAllTuples();
		TupleDesc mainDesc = mainRelationFile.getTupleDesc();
		Relation mainRelation = new Relation(mainTuples, mainDesc);
		System.out.println("Main Relation Tuple Count: " + mainRelation.getTuples().size());
		System.out.println("Main Relation Description: " + mainRelation.getDesc());

		
		// Handle JOIN
		mainTable = null; //!!!
		List<Join> joins = sb.getJoins();
		if (joins != null) {
			for (Join j : joins) {
				Table rightTable = (Table) j.getRightItem();
				
				
				//Table joinTable = (Table) j.getRightItem();
				int joinTableId = Database.getCatalog().getTableId(rightTable.getName());
				HeapFile joinRelationFile = Database.getCatalog().getDbFile(joinTableId);
				
				ArrayList<Tuple> joinTuples = joinRelationFile.getAllTuples();
				TupleDesc joinDesc = joinRelationFile.getTupleDesc();
				Relation joinRelation = new Relation(joinTuples, joinDesc);
				
				if (j.getOnExpression() instanceof EqualsTo) {
					EqualsTo equalsExp = (EqualsTo) j.getOnExpression();
					Column leftCol = (Column) equalsExp.getLeftExpression();
					Column rightCol = (Column) equalsExp.getRightExpression();
					
					String mainFieldName = leftCol.getColumnName();
					String joinFieldName = rightCol.getColumnName();
					
					int mainFieldNum = mainRelationFile.getTupleDesc().nameToId(mainFieldName);
					int joinFieldNum = joinRelationFile.getTupleDesc().nameToId(joinFieldName);
					
					mainRelation = mainRelation.join(joinRelation, mainFieldNum, joinFieldNum);
				}
				
			}
		}
		
		if (mainTable == null) {
			mainTable = (Table) sb.getFromItem();
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
		// ...
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
					
					if (colName.startsWith("SUM(") && colName.endsWith(")")) {
						colName = colName.substring(4, colName.length() - 1);
					}
					
					if (colName.contains(" AS ")) {
						colName = colName.split(" AS ")[0].trim();
					}
					
					System.out.println("Extracted column name: " + colName);

					
					int fieldIndex = getIndexOfField(mainRelation, colName);
					if (fieldIndex == -1) {
					    System.out.println("Available columns in mainRelation: " + mainRelation.getDesc().getFieldName(mainTableId));

						System.out.println("Failed to find column: " + colName);
						throw new IllegalArgumentException("Invalid column name");
					}
					fields.add(fieldIndex);
				}
			}

			mainRelation = mainRelation.project(fields);
			mainRelation = mainRelation.rename(fields, names);

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

