package mapreduce.exceptions;

public class ArrayMismatchException extends ArrayIndexOutOfBoundsException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String exception;
	public ArrayMismatchException(String exp){
		super(exp);
		this.exception=exp;
	}
	public String getException(){
		return this.exception;
	}
}
