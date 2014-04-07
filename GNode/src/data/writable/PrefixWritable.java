package data.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;

import org.apache.hadoop.io.WritableComparable;

import data.writable.TraverseJobParameters.TraversalMethod;

public class PrefixWritable implements WritableComparable<PrefixWritable> {

	public LinkedList<Integer> data;

	public PrefixWritable() {
		data = new LinkedList<>();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		for (int i = 0; i < length; i++) {
			data.add(new Integer(in.readInt()));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(data.size());
		for (Integer value : data) {
			out.writeInt(value.intValue());
		}
	}

	@Override
	public int compareTo(PrefixWritable o) {
		int smallerSize = Math.min(this.data.size(), o.data.size());
		Iterator<Integer> t = this.data.iterator();
		Iterator<Integer> ot = o.data.iterator();
		for (int i = 0; i < smallerSize; i++) {
			Integer a = t.next();
			Integer b = ot.next();
			if (a < b) {
				return -1;
			} else if (a > b) {
				return 1;
			}
		}
		if (this.data.size() < o.data.size()) {
			return -1;
		} else if (this.data.size() > o.data.size()) {
			return 1;
		}
		return 0;
	}

	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		for (Integer value : data) {
			b.append(value + "-");
		}
		b.deleteCharAt(b.length() - 1);
		return b.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((data == null) ? 0 : data.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PrefixWritable other = (PrefixWritable) obj;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		return true;
	}

	public boolean equalString(String obj) {
		String[] value = obj.split("-");
		if (data.size() != value.length)
			return false;
		int i = 0;
		for (Integer item : data) {
			if (!(item.toString().equals(value[i]))) {
				return false;
			}
			i++;
		}
		return true;
	}

	public PrefixWritable cloneObj() {
		PrefixWritable pw = new PrefixWritable();
		for (Integer value : data) {
			pw.data.add(new Integer(value));
		}
		return pw;
	}

	public static PrefixWritable fromString(String obj) {
		PrefixWritable pw = new PrefixWritable();
		String[] value = obj.split("-");
		for (String v : value) {
			pw.data.add(new Integer(v));
		}
		return pw;
	}

	public boolean containsPrefix(PrefixWritable fromPrefix,
			TraverseJobParameters.TraversalMethod mode) {

		if (data.size() > fromPrefix.data.size()) {
			Iterator<Integer> fromI = fromPrefix.data.iterator();
			Iterator<Integer> thisI = data.iterator();
			boolean isSame = true;
			boolean first = true;
			while (fromI.hasNext()) {
				if (!(thisI.next().equals(fromI.next()))) {
					if (!(first && mode == TraversalMethod.BFS)) {
						isSame = false;
						break;
					} else {
						first = false;
					}
				}
			}
			return isSame;
		} else {
			return false;
		}

	}

	public void replacePrefix(PrefixWritable fromPrefix, PrefixWritable toPrefix, TraversalMethod mode) {
		
		ListIterator<Integer> revIterator = toPrefix.data.listIterator(toPrefix.data.size());
		
		int i = 0;
		
		int levelDiff = 0;
		if (mode == TraversalMethod.BFS) {
			levelDiff = data.getFirst() - fromPrefix.data.getFirst();
		}
		
		while (revIterator.hasPrevious()) {
			data.remove(i);
			data.addFirst(revIterator.previous());
			i++;
		}
		if (mode == TraversalMethod.BFS) {
			data.set(0, data.getFirst() + levelDiff);
		}
		
	}

}
