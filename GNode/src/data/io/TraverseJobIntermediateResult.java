package data.io;

import java.util.HashSet;
import java.util.Set;

import data.writable.TraverseJobParameters;

public class TraverseJobIntermediateResult {
	
	public TraverseJobParameters param;
	public StringBuilder result;
	public int remainingJobsUnfinished;
	public String parentIP;
	public Set<String> visitedVertices = new HashSet<String>();
	
	public TraverseJobIntermediateResult() {
		param = null;
	}

	public TraverseJobIntermediateResult(TraverseJobParameters param,
			String result, int remainingJobsUnfinished, String parentIP) {
		this.param = param;
		this.result = new StringBuilder(result);
		this.remainingJobsUnfinished = remainingJobsUnfinished;
		this.parentIP = parentIP;
	}

}
