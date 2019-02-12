package miis.Lab4.Proves;
//package miis.Lab4;

public class Prova {
	public static void main(String[] args) throws Exception {
		int i,j;
		int n = 3;
		int m = 5;
		//int [] [] matrix = new int [n] [m];
		for (i=0; i<n; i++) {
			for (j=0; j<m; j++) {
				//matrix[i][j] = (int)(Math.random()*10);
				System.out.println("A,"+i+","+j+","+(int)(Math.random()*10));
			}
		}
	}
}
