package test;

public class TestJavaClass {
	private static String staticName = "staticName";
	private String name;
	
	public TestJavaClass(String name) {
		this.name = name;
		System.out.println("TestJavaClass");
	}
	
	class InnerClass {
		private String innerName;
		
		InnerClass() {
			this.innerName = "InnerClass";
			System.out.println("InnerClass()");
			System.out.println("内部类中访问外部类中变量：" + TestJavaClass.this.name);
		}
	}
	
	static class StaticInnerClass {
		private String innerName;
		
		StaticInnerClass() {
			this.innerName = "StaticInnerClass";
			System.out.println("StaticInnerClass()");
			System.out.println("内部类中访问外部类中静态变量：" + TestJavaClass.staticName);
		}
	}
	
	public void test1() {
		class MethodInnerClass implements Runnable {

			public void run() {
				System.out.println("方法内部类，MethodInnerClass::run()");
			}
			
		}
		
		(new MethodInnerClass()).run();
	}

	public static void main(String[] args) {
		TestJavaClass tjc1 = new TestJavaClass("123");
		tjc1.test1();
		TestJavaClass.InnerClass ic1 = tjc1.new InnerClass();
		System.out.println(ic1.innerName);
		TestJavaClass.StaticInnerClass sic1 = new StaticInnerClass();
		
		new Runnable() {

			public void run() {
				System.out.println("匿名内部类，Runnable::run()");
			}
			
		}.run();
	}
}