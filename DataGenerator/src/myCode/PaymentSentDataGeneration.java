package myCode;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PaymentSentDataGeneration {

	public PaymentSentDataGeneration(String file, int num) {
		super();
		this.file = new File(file);
		this.num = num;
	}

	//private List<Long> list = new ArrayList<Long>();

	private final static char CHAR = ',';

	File file;

	FileWriter output;

	/**
	 * num can be set to 100000 or larger, size of txns should be num *25
	 */
	private int num = 1000;
	private int transIDIncrement = 0;
	BufferedWriter boutput;

	Random random = new Random();

	public void setUp() throws IOException {
		/*for (long i = 10000000; i < 10000000 + num; i++) {
			list.add(i + 1);
		}*/
		output = new FileWriter(file);
		boutput = new BufferedWriter(output);
	}
	
	protected Long listget(int value) {
		return 10000001l+value;
	}

	protected void writeOneTrans(int senderIDIndex) throws IOException {

		// EDGE ID
		StringBuilder sb = new StringBuilder(1000);
		sb.append((++transIDIncrement)).append(CHAR); // Payment_transid

		// SENDER ID
		// cust_id good
		Long cust_id = 10000001l+senderIDIndex;
		sb.append(cust_id).append(CHAR); // cust_id

		// -- (RECEIVER ID)
		int nextInt = random.nextInt(num);
		if (nextInt == 0) {
			nextInt = random.nextInt(1000) + 1;
		}
		int cp_index = (nextInt + senderIDIndex) % num;
		Long rece_id = 10000001l+cp_index;

		// Is sender already existing?

		// SENDER RESTRICTION FLAG
		// int is_restricted_random = random.nextInt(senderIDIndex + 1);
		// is_restricted_random = random.nextInt(is_restricted_random + 1);
		// int is_restricted_random_str = is_restricted_random % 99 == 1 ? 1
		// : 0;
		int is_restricted_random_str = cust_id % 99 == 1 ? 1 : 0;
		sb.append(is_restricted_random_str).append(CHAR); // is_restricted

		// SENDER ACCOUNT CREATION TIME
		// int vertex_creation_time = random.nextInt(1000000);
		// vertex_creation_time += 1000000000;
		int vertex_creation_time = 1000000 + cust_id.intValue();
		sb.append(vertex_creation_time).append(CHAR);

		// EMAIL DOMAIN
		sb.append(cust_id + "@email.com").append(CHAR);

		// RECEIVER ID
		// cp
		sb.append(rece_id).append(CHAR); // cp_cust_id

		// RECEIVER RESTRICTION FLAG
//		int is_receiver_restricted_random = random.nextInt(cust_id_index + 1);
//		is_receiver_restricted_random = random
//				.nextInt(is_receiver_restricted_random + 1);
//		int is_receiver_restricted_random_str = is_receiver_restricted_random % 99 == 1 ? 1
//				: 0;
		int is_receiver_restricted_random_str = rece_id % 99 == 1 ? 1 : 0;
		sb.append(is_receiver_restricted_random_str).append(CHAR); // is_restricted

		// RECEIVER ACCOUNT CREATION TIME
//		int receiver_creation_time = random.nextInt(1000000);
//		receiver_creation_time += 1001000000;
		int receiver_creation_time = 1000000 + rece_id.intValue();
		sb.append(receiver_creation_time).append(CHAR);

		// // date
		// Calendar cal = Calendar.getInstance();
		// cal.add(Calendar.YEAR, -1);
		// cal.add(Calendar.MONTH, 5);
		// cal.add(Calendar.DATE, 8);
		// cal.add(Calendar.HOUR, -6);
		// cal.add(Calendar.MINUTE, 28);
		// cal.add(Calendar.SECOND, -17);
		// cal.add(Calendar.DATE, random.nextInt(30) - 15);
		// cal.add(Calendar.HOUR, random.nextInt(300) - 150);
		// cal.add(Calendar.MINUTE, random.nextInt(3000) - 1500);
		// cal.add(Calendar.SECOND, random.nextInt(30000) - 15000);
		//
		// sb.append(dateFormat.format(cal.getTime())).append(CHAR); // date

		// RECEIVER EMAIL DOMAIN
		sb.append(rece_id+"@email.com").append(CHAR);

		// TRANSACTION TIME
		int transaction_time = random.nextInt(500000);
		transaction_time += 1001500000;
		sb.append(transaction_time).append(CHAR);

		// sender ip
		int ip_index = senderIDIndex;
		if (ip_index % 5 == 1) {
			ip_index = random.nextInt(ip_index + 1);
		}
		sb.append(listget(ip_index)).append(CHAR); // sndr_last_login_ip

		// reciever ip
		ip_index = cp_index;
		if (ip_index % 6 == 1) {
			ip_index = random.nextInt(ip_index + 1);
		}
		sb.append(listget(ip_index)).append(CHAR); // rcvr_last_login_ip

		// pymt_amt_usd, random is ok
		sb.append(random.nextInt(10000)).append(CHAR); // pymt_amt_usd

		// these two columns are not used
		sb.append(senderIDIndex % 10 == 1 ? 1 : 0).append(CHAR); // cc_used
		sb.append(listget(senderIDIndex) + 2000).append(CHAR); // cc_id

		// is buyer bad
		int is_bad_random = random.nextInt(senderIDIndex + 1);
		is_bad_random = random.nextInt(is_bad_random + 1);
		sb.append(is_bad_random % 99 == 1 ? 1 : 0); // is_bad

		sb.append("\r\n");
		boutput.write(sb.toString());

	}

	public void genData() throws IOException {
		// SimpleDateFormat dateFormat = new SimpleDateFormat(
		// "yyyy-MM-dd HH:mm:ss");

		transIDIncrement = 0;
		
		int percent = 0;
		int percentOne = (int) ((float)num * 0.01f);
		int percentCount = 0;

		for (int i = 0; i < num; i++) {
			// for every sender, the count of trans maybe 0~50
			percentCount++;
			if (percentCount == percentOne) {
				percent++;
				System.out.println(percent+"%");
				percentCount=0;
			}
			
			int transSum = random.nextInt(50);
			for (int j = 0; j < transSum; j++) {
				// for every trans
				writeOneTrans(i);
			}
		}
		
	}

	public void close() throws IOException {
		boutput.close();
		output.close();
	}

	/**
	 * args[0] is file name args[1] is number of customers Number of
	 * transactions is 25 * args[1]
	 */
	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.println("Usage: java -jar generator.jar <path> <count>");
			//return;
		}
		
		String filename = "/Users/alex/Documents/exData/";
		//String filename = args[0];

		//for (int i = 0; i < 5; i++) {

			//int count_customers = Integer.parseInt(args[1]);
			int count_customers = Integer.parseInt("500000");
		
			PaymentSentDataGeneration dg = new PaymentSentDataGeneration(
					filename + count_customers + ".csv", count_customers);
			dg.setUp();
			dg.genData();
			dg.close();
		//}
	}

}