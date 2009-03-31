package mapreduce.test;
import mapreduce.exceptions.*;
/**
 * This will be the general way all the machine learning algorithms will be implemented
 * A number of static methods that calculate the various components of the functions
 * These functions will be independent of the mapreduce implementation. Later these methods must
 * be moved to interfaces that dictate general methods in unsupervised/supervised and semi-supervised methods
 * like supervised classifiers of the generative categories generally model the Class conditional probability.
 * The Map reduce will then polymorphically just call methods on these interfaces. With the actual
 * algorithm class only becoming apparent at run time.
 * The variation in learning techniques is a lot. Like multivariarte/Univariate, Supervised/unsupervised/Semi
 * discriminative, generative, discriminant. logistic/linear/non-linear.
 * */
public class LRUtils {
	/**
	 * The Error gradient
	 * @param w weight vector generally same as the number of features
	 * @param t the target vector
	 * @param phi_n the feature vector that represents a single input
	 * @throws ArrayMismatchException 
	 * @throws ArrayMismatchException 
	 * */
	public static double[] calculateGradient(double[] w ,double t, double[] phi_n)  {
		double[] deltaError = new double[phi_n.length];
		try {
			double temp = sigmoid(w,phi_n) -t;
			for(int i=0; i<phi_n.length; i++){
				deltaError[i] = temp*phi_n[i];
			}
		} catch (ArrayMismatchException e) {
			System.out.println(e.getException());
			e.printStackTrace();
		}
		return deltaError;
	}
	/**
	 * Calculate the sigmoid function given w and phi_n
	 * */
	public static double sigmoid(double[] w,double[] phi_n )throws ArrayMismatchException{
		if(w==null || phi_n ==null || w.length!=phi_n.length){
			throw new ArrayMismatchException("Mismatch in array dimensions in sigmoid function");
		}
		double sum=0;
		for(int j =0; j<w.length;j++){
			sum+= w[j]*phi_n[j];
		}
		return 1/(1+Math.exp(-sum));
	}
	public static double hessian(double[] w,double[] phi_n){
		
		double hessian = 0;
		double sum2 = 0;
		for(int i=0;i<phi_n.length;i++){
			sum2 +=phi_n[i]*phi_n[i]; 
		}
		try{
			for(int j =0; j<w.length;j++){
				hessian+= sigmoid(w,phi_n)*(1-sigmoid(w,phi_n))*sum2;
		   }
		}catch(ArrayMismatchException e){
			System.out.println(e.getException());
			e.printStackTrace();
		}
		return hessian;
	}
	public static double[] upDateWeight(double[] w, double[] update ) throws ArrayMismatchException{
		if(w==null || update ==null || w.length!=update.length){
			throw new ArrayMismatchException("Mismatch in array dimensions in upDateWeight function");
		}
		for(int i=0;i<w.length;i++){
			w[i] = w[i] -update[i];
		}
		return w;
	}
}
