package miis.Lab4.Proves;

import java.util.HashMap;

public class Hashprova {
	public static void main(String[] args) throws Exception {
		HashMap<Tuple<Integer,Integer>, Integer> A = new HashMap<Tuple<Integer,Integer>, Integer>();
		Tuple<Integer,Integer> index = new Tuple(0,0);
		Object key = index;
		
		A.put(index, 3);
		index = new Tuple(1,1);
		A.put(index,2);
		System.out.println(A);
		index = new Tuple(0,0);
		System.out.println(A.get(key));
	}
}
