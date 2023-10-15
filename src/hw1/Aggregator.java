package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {

	private AggregateOperator op;
	private boolean groupBy;
	private TupleDesc td;
	private HashMap<Field, ArrayList<Integer>> groups;

	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		this.op = o;
		this.groupBy = groupBy;
		this.td = td;
		this.groups = new HashMap<>();
	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		Field field = null;
		if (groupBy) {
			field = t.getField(0);
		}
		int value = ((IntField) t.getField(groupBy ? 1 : 0)).getValue();

		if (!groups.containsKey(field)) {
			groups.put(field, new ArrayList<>());
		}
		groups.get(field).add(value);
	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		ArrayList<Tuple> res = new ArrayList<>();

		for (Field group : groups.keySet()) {
			int aggRes = 0;
			ArrayList<Integer> values = groups.get(group);
			for (int value : values) {
				switch (op) {
					case MIN:
						aggRes = Math.min(aggRes, value);
						break;
					case MAX:
						aggRes = Math.max(aggRes, value);
						break;
					case SUM:
						aggRes += value;
						break;
					case AVG:
						aggRes += value;
						break;
					case COUNT:
						aggRes++;
						break;
				}
			}
			if (op == AggregateOperator.AVG) {
				aggRes /= values.size();
			}
			Tuple resTuple = new Tuple(td);
			if (groupBy) {
				resTuple.setField(0, group);
				resTuple.setField(1, new IntField(aggRes));
			} else {
				resTuple.setField(0, new IntField(aggRes));
			}

			res.add(resTuple);
		}
		return res;
	}
}
