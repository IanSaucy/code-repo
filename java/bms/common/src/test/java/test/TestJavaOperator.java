package test;

public class TestJavaOperator {

	public static String toBinaryString(int a) {
		StringBuilder sb = new StringBuilder(32);
		
		for (int i = 0; i < 32; i++) {
			int t = (a & 0x80000000 >>> i) >>> (31 - i);
			sb.append(t);
		}
		
		return sb.toString();
	}
	
	
	public static void main(String[] args) throws InterruptedException {
		int a = 10;
		int b = -20;
		System.out.println(Integer.toBinaryString(a >>> 1));
		System.out.println(Integer.toBinaryString(b >> 1));
		System.out.println(Integer.toBinaryString(b >>> 1));
		System.out.println(toBinaryString(-6));
		
	}

}
