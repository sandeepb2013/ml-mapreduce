package mlmr.utils;

import org.apache.hadoop.io.DoubleWritable;

import mlmr.exceptions.ArrayMismatchException;


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
public class LRUtil {
	private DoubleWritable prod1 =null;
	private DoubleWritable prod2 =null;
	/**
	 * The Error gradient
	 * @param w weight vector generally same as the number of features
	 * @param t the target vector
	 * @param phi_n the feature vector that represents a single input
	 * @throws ArrayMismatchException 
	 * @throws ArrayMismatchException 
	 * */
	public static DoubleWritable[] calculateGradient(DoubleWritable[] w ,DoubleWritable t, DoubleWritable[] phi_n)  {
		DoubleWritable[] deltaError = new DoubleWritable[phi_n.length];
		try {
			double temp = sigmoid(w,phi_n).get() -t.get();
			System.out.println("sigmoid(w,phi_n) -t: "+ temp);
			for(int i=0; i<phi_n.length; i++){
				deltaError[i] = new DoubleWritable(0);
				if(Double.isNaN(temp)){
					deltaError[i].set(Double.MIN_VALUE);
				}else{
					deltaError[i].set(temp*phi_n[i].get());
				}
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
	public static DoubleWritable sigmoid(DoubleWritable[] w,DoubleWritable[] phi_n )throws ArrayMismatchException{
		if(w==null || phi_n ==null || w.length!=phi_n.length){
			throw new ArrayMismatchException("Mismatch in array dimensions in sigmoid function");
		}
		double sum=0;
		for(int j =0; j<w.length;j++){
			
			sum+= w[j].get()*phi_n[j].get();
		}
		
		return new DoubleWritable(1/(1+Math.exp(-sum)));
	}
	
/**
 * If the Hessian is too large we call this method from within a for loop avoiding thereby a 
 * large temporary DoubleWritable matrix of DoubleWritables in the memory. We strip it into two parts.
 * Since  sigmoid(w,phi_n)*(1-sigmoid(w,phi_n)) is constant for given phi and w 
 * we calculate it separately. And pass it to the variable part.  
 * 
 * */
	public static DoubleWritable efficientHessianConstant(DoubleWritable[] w,DoubleWritable[] phi_n){
		return new DoubleWritable(sigmoid(w,phi_n).get()*(1-sigmoid(w,phi_n).get()));
	}
	
	public static  DoubleWritable efficientHessianVariable(DoubleWritable constant, DoubleWritable[] phi_n, int rowIndex,int columnIndex){
			double value= phi_n[rowIndex].get()*phi_n[columnIndex].get()*constant.get();
			if (Double.isNaN(value)){
				return  new DoubleWritable(Double.MIN_VALUE);
			}else{
				return new DoubleWritable(phi_n[rowIndex].get()*phi_n[columnIndex].get()*constant.get());

			}
	}
	/**
	 * Should be used for small size of phi_n. 
	 * */
	public static DoubleWritable[][] hessian(DoubleWritable[] w,DoubleWritable[] phi_n){
		
		DoubleWritable[][] hessian = new DoubleWritable[phi_n.length][phi_n.length];
		DoubleWritable[][] sum2 = new DoubleWritable[phi_n.length][phi_n.length];
		for(int j=0;j<phi_n.length;j++){
			for(int i=0;i<phi_n.length;i++){
				sum2[j][i]= new DoubleWritable(0);
				sum2[j][i].set(phi_n[j].get()*phi_n[i].get()); 
			}
		}
		DoubleWritable product =new DoubleWritable(sigmoid(w,phi_n).get()*(1-sigmoid(w,phi_n).get()));
		try{
			for(int i=0; i<phi_n.length;i++){
				for(int j =0; j<phi_n.length;j++){
					hessian[i][j] = new DoubleWritable();
					hessian[i][j].set(product.get()*sum2[i][j].get());
			   }
			}
		}catch(ArrayMismatchException e){
			System.out.println(e.getException());
			e.printStackTrace();
		}
		return hessian;
	}
	public static DoubleWritable[] upDateWeight(DoubleWritable[] w, DoubleWritable[] update ) throws ArrayMismatchException{
		if(w==null || update ==null || w.length!=update.length){
			throw new ArrayMismatchException("Mismatch in array dimensions in upDateWeight function");
		}
		for(int i=0;i<w.length;i++){
			w[i].set(w[i].get()-update[i].get());
		}
		return w;
	}
	
}