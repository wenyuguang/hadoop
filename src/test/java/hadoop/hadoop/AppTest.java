package hadoop.hadoop;

import java.util.Date;

import hadoop.util.IpUtil;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {
	/**
	 * Create the test case
	 *
	 * @param testName
	 *            name of the test case
	 */
	public AppTest(String testName) {
		super(testName);
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite() {
		return new TestSuite(AppTest.class);
	}

	/**
	 * Rigourous Test :-)
	 */
	public void testApp() {
		assertTrue(true);
		long runtime = new Date().getTime();
		String ip = IpUtil.getRandomIp();
		String msg = runtime + ",www.example.com," + ip;
		System.out.println(msg);


		System.out.println("[+");
	}
}
