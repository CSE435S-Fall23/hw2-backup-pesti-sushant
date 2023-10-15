package hw1;

import java.util.ArrayList;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		this.tuples = l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		ArrayList<Tuple> res = new ArrayList<>();
		for (Tuple tuple : tuples) {
			if (tuple.getField(field).compare(op, operand)) {
				res.add(tuple);
			}
		}
		return new Relation(res, td);
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		String[] newNames = new String[td.numFields()];
		Type[] types = new Type[td.numFields()];

		for (int i = 0; i < td.numFields(); i++) {
			newNames[i] = td.getFieldName(i);
			types[i] = td.getType(i);
		}

		for (int i = 0; i < fields.size(); i++) {
			newNames[fields.get(i)] = names.get(i);
		}

		TupleDesc newTupleDesc = new TupleDesc(types, newNames);

		ArrayList<Tuple> newTups = new ArrayList<>(tuples);

		for (Tuple t : newTups) {
			t.setDesc(newTupleDesc);
		}

		return new Relation(newTups, newTupleDesc);

	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		
		String[] newNames = new String[fields.size()];
		Type[] types = new Type[fields.size()];

		for (int i = 0; i < fields.size(); i++) {
			newNames[i] = td.getFieldName(fields.get(i));
			types[i] = td.getType(fields.get(i));
		}

		TupleDesc newTupleDesc = new TupleDesc(types, newNames);

		ArrayList<Tuple> newTups = new ArrayList<>();

		for (Tuple t : tuples) {
			Tuple newTuple = new Tuple(newTupleDesc);
			for (int i = 0; i < fields.size(); i++) {
				newTuple.addField(t.getField(fields.get(i)));
			}
			newTups.add(newTuple);
		}
		
		return new Relation(newTups, newTupleDesc);
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		TupleDesc newTupleDesc = other.getDesc();

		String[] joinNames = new String[td.numFields() + other.getDesc().numFields()];\
		Type[] joinTypes = new Type[joinNames.length];

		for (int i = 0; i < td.numFields(); i++) {
			joinNames[i] = td.getFieldName(i);
			joinTypes[i] = td.getType(i);
		}

		for (int i = 0; i < other.getDesc().numFields(); i++) {
			joinNames[i + td.numFields()] = newTupleDesc.getFieldName(i);
			joinTypes[i + td.numFields()] = newTupleDesc.getType(i);
		}

		TupleDesc joinTupleDesc = new TupleDesc(joinTypes, joinNames);
		ArrayList<Tuple> joinTuples = new ArrayList<>();

		for (Tuple tuple_1 : tuples) {
			for (Tuple tuple_2 : other.getTuples()) {
				if (tuple_1.getField(field1).compare(RelationalOperator.EQ, tuple_2.getField(field2))) {
					Tuple newTuple = new Tuple(joinTupleDesc);
					for (int i = 0; i < td.numFields(); i++) {
						newTuple.addField(tuple_1.getField(i));
					}
					for (int i = 0; i < newTupleDesc.numFields(); i++) {
						newTuple.addField(tuple_2.getField(i));
					}
					joinTuples.add(newTuple);
				}
			}
		}
		return new Relation(joinTuples, joinTupleDesc);
	}	
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		Aggregator agg = new Aggregator(op, groupBy, td);
		for (Tuple t : tuples) {
			agg.merge(t);
		}
		ArrayList<Tuple> aggedTuples = agg.getResults();

		return new Relation(aggedTuples, aggedTuples.get(0).getDesc());
	}
	
	public TupleDesc getDesc() {
		return this.td;
	}
	
	public ArrayList<Tuple> getTuples() {
		return this.tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();

		sb.append(td.toString()).append('\n');
		for (Tuple t : tuples) {
			sb.append(t.toString()).append('\n');
		}
		return sb.toString();
	}
}
