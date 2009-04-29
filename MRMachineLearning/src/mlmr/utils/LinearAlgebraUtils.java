package mlmr.utils;


public class LinearAlgebraUtils {
	

	private boolean DEBUG = false;

	private boolean INFO = false;
	private int iDF = 0;

	
	public double[][] UpperTriangle(double[][] m) {
		
	//System.out.println("Converting to Upper Triangle...");
	
	double f1 = 0;
	double temp = 0;
	int tms = m.length; // get This Matrix Size (could be smaller than
						// global)
	int v = 1;

	iDF = 1;

	for (int col = 0; col < tms - 1; col++) {
		for (int row = col + 1; row < tms; row++) {
			v = 1;

			outahere: while (m[col][col] == 0) // check if 0 in diagonal
			{ // if so switch until not
				if (col + v >= tms) // check if switched all rows
				{
					iDF = 0;
					break outahere;
				} else {
					for (int c = 0; c < tms; c++) {
						temp = m[col][c];
						m[col][c] = m[col + v][c]; // switch rows
						m[col + v][c] = temp;
					}
					v++; // count row switchs
					iDF = iDF * -1; // each switch changes determinant
									// factor
				}
			}

			if (m[col][col] != 0) {
				if (DEBUG) {
					System.out.println("tms = " + tms + "   col = " + col
							+ "   row = " + row);
				}

				try {
					f1 = (-1) * m[row][col] / m[col][col];
					for (int i = col; i < tms; i++) {
						m[row][i] = f1 * m[col][i] + m[row][i];
					}
				} catch (Exception e) {
					System.out.println("Still Here!!!");
				}

			}

		}
	}

	return m;
	}
	public double[][] Adjoint(double[][] a) throws Exception {
		if (INFO) {
			System.out.println("Performing Adjoint...");
		}
		int tms = a.length;

		double m[][] = new double[tms][tms];

		int ii, jj, ia, ja;
		double det;

		for (int i = 0; i < tms; i++)
			for (int j = 0; j < tms; j++) {
				ia = ja = 0;

				double ap[][] = new double[tms - 1][tms - 1];

				for (ii = 0; ii < tms; ii++) {
					for (jj = 0; jj < tms; jj++) {

						if ((ii != i) && (jj != j)) {
							ap[ia][ja] = a[ii][jj];
							ja++;
						}

					}
					if ((ii != i) && (jj != j)) {
						ia++;
					}
					ja = 0;
				}

				det = Determinant(ap);
				m[i][j] = (double) Math.pow(-1, i + j) * det;
			}

		m = Transpose(m);

		return m;
	}
// --------------------------------------------------------------
	
	public double[][] Transpose(double[][] a) {
		if (INFO) {
			System.out.println("Performing Transpose...");
		}
		
		double m[][] = new double[a[0].length][a.length];

		for (int i = 0; i < a.length; i++)
			for (int j = 0; j < a[i].length; j++)
				m[j][i] = a[i][j];
		return m;
	}
	
	public double Determinant(double[][] matrix) {
		if (INFO) {
			System.out.println("Getting Determinant...");
		}
		int tms = matrix.length;
	
		double det = 1;
	
		matrix = UpperTriangle(matrix);
	
		for (int i = 0; i < tms; i++) {
			det = det * matrix[i][i];
		} // multiply down diagonal
	
		det = det * iDF; // adjust w/ determinant factor
	
		if (INFO) {
			System.out.println("Determinant: " + det);
		}
		return det;
	}
	
	public double[][] Inverse(double[][] a) throws Exception {
		// Formula used to Calculate Inverse:
		// inv(A) = 1/det(A) * adj(A)
		if (INFO) {
			System.out.println("Performing Inverse...");
		}
		int tms = a.length;
	
		double m[][] = new double[tms][tms];
		double mm[][] = Adjoint(a);
	
		double det = Determinant(a);
		double dd = 0;
	
		if (det == 0) {
			System.out.println("Determinant Equals 0, Not Invertible.");
			if (INFO) {
				System.out.println("Determinant Equals 0, Not Invertible.");
			}
		} else {
			dd = 1 / det;
		}
	
		for (int i = 0; i < tms; i++)
			for (int j = 0; j < tms; j++) {
				m[i][j] = dd * mm[i][j];
			}
	
		return m;
	}
}