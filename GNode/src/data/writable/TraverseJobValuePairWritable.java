package data.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TraverseJobValuePairWritable implements WritableComparable<TraverseJobValuePairWritable> {
	
	public PrefixWritable prefix;
	public String value;
	public Boolean isFixValue = false;
	
	public TraverseJobValuePairWritable() {
		prefix = new PrefixWritable();
		isFixValue = false;
	}

	public TraverseJobValuePairWritable(PrefixWritable prefix, String value) {
		this.prefix = prefix;
		this.value = value;
		this.isFixValue = false;
	}
	
	public TraverseJobValuePairWritable(PrefixWritable prefix, String value, boolean fixValue) {
		this.prefix = prefix;
		this.value = value;
		this.isFixValue = fixValue;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		prefix.readFields(in);
		value = in.readUTF();
		isFixValue = in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		prefix.write(out);
		out.writeUTF(value);
		out.writeBoolean(isFixValue);
	}

	@Override
	public int compareTo(TraverseJobValuePairWritable o) {
		return prefix.compareTo(o.prefix);
	}
	

}
