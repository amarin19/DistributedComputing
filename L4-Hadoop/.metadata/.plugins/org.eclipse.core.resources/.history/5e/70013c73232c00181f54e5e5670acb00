//package miis.Lab4;

public class MatrixGenerator {
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.err.println("ERROR: wrong arguments");
			System.err.println("Usage: java MatrixGenerator <Matrix name> <Num rows> <Num cols> [> File]");
			System.exit(1);
		}
		
		String name = args[0];
		int n = Integer.parseInt(args[1]);
		int m = Integer.parseInt(args[2]);
		int i,j;
		//int [] [] matrix = new int [n] [m];
		for (i=0; i<n; i++) {
			for (j=0; j<m; j++) {
				System.out.println(name+","+i+","+j+","+(int)(Math.random()*10));
			}
		}
	}
}
