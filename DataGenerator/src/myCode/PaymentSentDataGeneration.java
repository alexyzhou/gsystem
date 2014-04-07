package myCode;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.security.auth.login.AccountException;

public class PaymentSentDataGeneration {

	public PaymentSentDataGeneration(String file, int num) {
		super();
		this.file = new File(file);
		this.num = num;
	}

	private List<Long> list = new ArrayList<Long>();
	
	private Map<Long, String[]> accouts = new HashMap<Long, String[]>();
	private Set<String> existingTrans = new HashSet<>();

	private final static char CHAR = ',';

	File file;

	FileWriter output;

	/**
	 * num can be set to 100000 or larger, size of txns should be num *25
	 */
	private int num = 1000;

	BufferedWriter boutput;

	Random random = new Random();

	public void setUp() throws IOException {
		for (long i = 100000; i < 100000 + num; i++) {
			list.add(i + 1);
		}
		output = new FileWriter(file);
		boutput = new BufferedWriter(output);
	}

	public void genData() throws IOException {
//		SimpleDateFormat dateFormat = new SimpleDateFormat(
//				"yyyy-MM-dd HH:mm:ss");
		
		accouts.clear();
		existingTrans.clear();

		for (long i = 100000; i < 100000 + num * 100; i++) {

			// EDGE ID
			StringBuilder sb = new StringBuilder(1000);
			sb.append(i).append(CHAR); // Payment_transid

			// SENDER ID
			// cust_id good
			int cust_id_index = random.nextInt(num);
			Long cust_id = list.get(cust_id_index);
			sb.append(cust_id).append(CHAR); // cust_id
			
			// -- (RECEIVER ID)
			int nextInt = random.nextInt(num);
			if (nextInt == 0) {
				nextInt = random.nextInt(1000) + 1;
			}
			int cp_index = (nextInt + cust_id_index) % num;
			Long rece_id = list.get(cp_index);
			
			// Is this transaction existing?
			if (existingTrans.contains(cust_id + ";" + rece_id)) {
				continue;
			} else {
				existingTrans.add(cust_id + ";" + rece_id);
			}
			
			// Is sender already existing?
			if (accouts.containsKey(cust_id)) {
				
				System.err.println("Bingo!");
				
				String[] value = accouts.get(cust_id);
				
				// (**TRUE**)SENDER RESTRICTION FLAG
				sb.append(value[0]).append(CHAR); // is_restricted

				// (**TRUE**)SENDER ACCOUNT CREATION TIME
				sb.append(value[1]).append(CHAR);
			} else {
				
				// SENDER RESTRICTION FLAG
				int is_restricted_random = random.nextInt(cust_id_index + 1);
				is_restricted_random = random.nextInt(is_restricted_random + 1);
				int is_restricted_random_str = is_restricted_random % 99 == 1 ? 1 : 0;
				sb.append(is_restricted_random_str).append(CHAR); // is_restricted

				// SENDER ACCOUNT CREATION TIME
				int vertex_creation_time = random.nextInt(1000000);
				vertex_creation_time += 1000000000;
				sb.append(vertex_creation_time).append(CHAR);
				
				String[] values = new String[2];
				values[0] = Integer.toString(is_restricted_random_str);
				values[1] = Integer.toString(vertex_creation_time);
				
				accouts.put(cust_id, values);
			}

			// EMAIL DOMAIN
			sb.append("email.com").append(CHAR);

			// RECEIVER ID
			// cp
			sb.append(rece_id).append(CHAR); // cp_cust_id
			
			// Is receiver already existing?
			if (accouts.containsKey(rece_id)) {
				String[] value = accouts.get(rece_id);
				
				// RECEIVER RESTRICTION FLAG
				sb.append(value[0]).append(
						CHAR); // is_restricted

				// RECEIVER ACCOUNT CREATION TIME
				sb.append(value[1]).append(CHAR);
			} else {
				// RECEIVER RESTRICTION FLAG
				int is_receiver_restricted_random = random
						.nextInt(cust_id_index + 1);
				is_receiver_restricted_random = random
						.nextInt(is_receiver_restricted_random + 1);
				int is_receiver_restricted_random_str = is_receiver_restricted_random % 99 == 1 ? 1 : 0;
				sb.append(is_receiver_restricted_random_str).append(
						CHAR); // is_restricted

				// RECEIVER ACCOUNT CREATION TIME
				int receiver_creation_time = random.nextInt(1000000);
				receiver_creation_time += 1001000000;
				sb.append(receiver_creation_time).append(CHAR);
				
				String[] values = new String[2];
				values[0] = Integer.toString(is_receiver_restricted_random_str);
				values[1] = Integer.toString(receiver_creation_time);
				
				accouts.put(rece_id, values);
			}



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
			sb.append("email.com").append(CHAR);

			// TRANSACTION TIME
			int transaction_time = random.nextInt(500000);
			transaction_time += 1001500000;
			sb.append(transaction_time).append(CHAR);

			// sender ip
			int ip_index = cust_id_index;
			if (ip_index % 5 == 1) {
				ip_index = random.nextInt(ip_index + 1);
			}
			sb.append(list.get(ip_index)).append(CHAR); // sndr_last_login_ip

			// reciever ip
			ip_index = cust_id_index;
			if (ip_index % 6 == 1) {
				ip_index = random.nextInt(ip_index + 1);
			}
			sb.append(list.get(ip_index)).append(CHAR); // rcvr_last_login_ip

			// pymt_amt_usd, random is ok
			sb.append(random.nextInt(10000)).append(CHAR); // pymt_amt_usd

			// these two columns are not used
			sb.append(cust_id_index % 10 == 1 ? 1 : 0).append(CHAR); // cc_used
			sb.append(list.get(cust_id_index) + 2000).append(CHAR); // cc_id

			// is buyer bad
			int is_bad_random = random.nextInt(cust_id_index + 1);
			is_bad_random = random.nextInt(is_bad_random + 1);
			sb.append(is_bad_random % 99 == 1 ? 1 : 0); // is_bad

			sb.append("\r\n");
			boutput.write(sb.toString());
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
		String filename = "/Users/alex/Documents/exData/";

		for (int i = 0; i < 1; i++) {

			int count_customers = 100 + 10000 * (i);

			PaymentSentDataGeneration dg = new PaymentSentDataGeneration(
					filename + count_customers + ".csv", count_customers);
			dg.setUp();
			dg.genData();
			dg.close();
		}
	}

}