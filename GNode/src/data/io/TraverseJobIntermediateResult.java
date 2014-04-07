package data.io;

import java.util.HashSet;
import java.util.LinkedList;

import data.writable.PrefixWritable;
import data.writable.TraverseJobParameters;
import data.writable.TraverseJobValuePairWritable;
import data.writable.TraverseJobParameters.TraversalMethod;

public class TraverseJobIntermediateResult {

	public TraverseJobParameters param;
	public LinkedList<TraverseJobValuePairWritable> result;
	public int remainingJobsUnfinished;
	public String parentIP;

	public TraverseJobIntermediateResult() {
		param = null;
	}

	public TraverseJobIntermediateResult(TraverseJobParameters param,
			int remainingJobsUnfinished, String parentIP) {
		this.param = param;
		this.result = new LinkedList<>();
		this.remainingJobsUnfinished = remainingJobsUnfinished;
		this.parentIP = parentIP;
	}

	public void appendResults(TraverseJobValuePairWritable[] set) {
		for (TraverseJobValuePairWritable value : set) {
			result.add(value);
		}
	}

	public void cleanResults() {
		
		HashSet<TraverseJobValuePairWritable> toRemoveSet = new HashSet<>();
		
		for (TraverseJobValuePairWritable v : result) {
			if (v.isFixValue) {
				boolean isFixed = false;
				for (TraverseJobValuePairWritable item : result) {
					if (item.isFixValue == false
							&& item.prefix.equalString(v.value)) {
						// Fix it!
						item.prefix = v.prefix.cloneObj();
						for (TraverseJobValuePairWritable child : result) {
							if (child.isFixValue == false
									&& child.prefix
											.containsPrefix(PrefixWritable
													.fromString(v.value), param.method)) {
								child.prefix.replacePrefix(PrefixWritable
													.fromString(v.value), v.prefix, param.method);
							}
						}
						isFixed = true;
						break;
					}
				}
				if (isFixed) {
					//tag
					toRemoveSet.add(v);
				}
			}
		}
		
		for (TraverseJobValuePairWritable e : toRemoveSet) {
			result.remove(e);
		}
	}

	public String genResultString() {
		StringBuilder sb = new StringBuilder();
		for (TraverseJobValuePairWritable v : result) {
			sb.append(v.value + ",");
		}
		return sb.toString();
	}

}
