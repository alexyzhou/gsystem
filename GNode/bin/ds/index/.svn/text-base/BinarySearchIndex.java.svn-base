package ds.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Vector;

import org.apache.hadoop.io.Writable;

public class BinarySearchIndex<E extends Comparable<E>> implements Writable {

	//Test GitHub
	private Vector<E> values;
	private Vector<Vector<Long>> offsets;
	private String valueTypeName;

	private String dsID = null;
	private String dschemaID = null;
	private String attriName = null;
	
	public BinarySearchIndex() {
		values = new Vector<E>();
		offsets = new Vector<Vector<Long>>();
	}

	public BinarySearchIndex(String dsID, String dschemaID, String attriName,
			String cls) {
		super();
		values = new Vector<E>();
		offsets = new Vector<Vector<Long>>();

		this.dsID = dsID;
		this.dschemaID = dschemaID;
		this.attriName = attriName;
		this.valueTypeName = cls;
	}

	protected E parseObject(String val) {
		try {
			Class<?> cls = Class.forName(this.valueTypeName);
			Constructor<?> cons = cls.getConstructor(String.class);
			E e = (E) cons.newInstance(val);
			return e;
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (NoSuchMethodException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (SecurityException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InstantiationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IllegalArgumentException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (InvocationTargetException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return null;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.dsID = in.readUTF();
		this.dschemaID = in.readUTF();
		this.attriName = in.readUTF();

		this.valueTypeName = in.readUTF();

		// for values
		int count = in.readInt();
		for (int i = 0; i < count; i++) {
			values.add(parseObject(in.readUTF()));
		}

		// for offsets
		count = in.readInt();
		for (int i = 0; i < count; i++) {
			int miniCount = in.readInt();
			Vector<Long> offList = new Vector<Long>(miniCount);
			for (int j = 0; j < miniCount; j++) {
				offList.add(in.readLong());
			}
			offsets.add(offList);
		}

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.dsID);
		out.writeUTF(this.dschemaID);
		out.writeUTF(this.attriName);

		out.writeUTF(this.valueTypeName);

		// for values
		out.writeInt(values.size());
		for (E e : values) {
			out.writeUTF(e.toString());
		}

		// for offsets
		out.writeInt(offsets.size());
		for (Vector<Long> l : offsets) {
			out.writeInt(l.size());
			for (Long ll : l) {
				out.writeLong(ll);
			}
		}
	}

	/*
	 * <,<=,!= 最大值,or value >,>= 最小值
	 */
	protected Vector<Long> search(String query) {
		String[] queryValues = query.split(",");
		if (queryValues.length == 2) {
			if (queryValues[0].equals("=")) {
				int target = pointSearch(parseObject(queryValues[1]));
				if (target != -1) {
					return offsets.get(target);
				}
			} else if (queryValues[0].equals("!=")) {
				int target = pointSearch(parseObject(queryValues[1]));
				Vector<Long> result = new Vector<Long>();
				for (int i = 0; i < offsets.size(); i++) {
					if (i != target) {
						result.addAll(offsets.get(i));
					}
				}
				return result;
			} else if (queryValues[0].equals("<")
					|| queryValues[0].equals("<=")
					|| queryValues[0].equals(">")
					|| queryValues[0].equals(">=")) {
				boolean isMin = queryValues[0].equals(">")
						|| queryValues[0].equals(">=");
				int target = rangeSearch(
						parseObject(queryValues[1]),
						queryValues[0].equals("<=")
								|| queryValues[0].equals(">="), isMin);
				if (target != -1) {
					int start = 0;
					int end = offsets.size() - 1;
					if (isMin) {
						start = target;
					} else {
						end = target;
					}
					Vector<Long> result = new Vector<Long>();
					for (int i = start; i <= end; i++) {
						result.addAll(offsets.get(i));
					}
					return result;
				}
			}
		} else if (queryValues.length == 4) {
			// for max
			int maxTarget = rangeSearch(parseObject(queryValues[1]),
					queryValues[0].equals("<="), false);
			// for min
			int minTarget = rangeSearch(parseObject(queryValues[3]),
					queryValues[2].equals(">="), true);
			if (maxTarget != -1 && minTarget != -1) {
				Vector<Long> result = new Vector<Long>();
				for (int i = minTarget; i <= maxTarget; i++) {
					result.addAll(offsets.get(i));
				}
				return result;
			}
		}
		return null;
	}

	protected int rangeSearch(E val, boolean equal, boolean isMin) {
		int start = 0;
		int end = values.size() - 1;
		int target = -1;
		while (start <= end) {
			int mid = (end - start) / 2 + start;
			if (val.compareTo(values.get(mid)) < 0) {
				if (isMin == true)
					target = mid;
				end = mid - 1;
			} else if (val.compareTo(values.get(mid)) > 0) {
				if (isMin == false)
					target = mid;
				start = mid + 1;
			} else if (equal == true) {
				return mid;
			} else {
				if (isMin == true) {
					start = mid + 1;
				} else {
					end = mid - 1;
				}
			}
		}
		return target;
	}

	protected int pointSearch(E val) {
		int mid = this.values.size() / 2;
		if (val.compareTo(values.get(mid)) == 0) {
			return mid;
		}

		int start = 0;
		int end = values.size() - 1;
		while (start <= end) {
			mid = (end - start) / 2 + start;
			if (val.compareTo(values.get(mid)) < 0) {
				end = mid - 1;
			} else if (val.compareTo(values.get(mid)) > 0) {
				start = mid + 1;
			} else {
				return mid;
			}
		}
		return -1;
	}

	public Vector<E> getValues() {
		return values;
	}

	public void setValues(Vector<E> values) {
		this.values = values;
	}

	public Vector<Vector<Long>> getOffsets() {
		return offsets;
	}

	public void setOffsets(Vector<Vector<Long>> offsets) {
		this.offsets = offsets;
	}

	public String getValueTypeName() {
		return valueTypeName;
	}

	public void setValueTypeName(String valueTypeName) {
		this.valueTypeName = valueTypeName;
	}

	public String getDsID() {
		return dsID;
	}

	public void setDsID(String dsID) {
		this.dsID = dsID;
	}

	public String getDschemaID() {
		return dschemaID;
	}

	public void setDschemaID(String dschemaID) {
		this.dschemaID = dschemaID;
	}

	public String getAttriName() {
		return attriName;
	}

	public void setAttriName(String attriName) {
		this.attriName = attriName;
	}
}
