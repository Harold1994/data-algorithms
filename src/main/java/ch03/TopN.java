package ch03;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author lihe
 * @Title:
 * @Description: java sortmap实现
 * @date 2018/6/11上午11:17
 */
public class TopN<T>  {

      public SortedMap<Integer, T> topN(List<Tuple2<T, Integer>> L, int N) {
        if ((L == null) || (L.isEmpty())) {
            return null;
        }
        SortedMap<Integer, T> topN = new TreeMap<Integer, T>();
        for (Tuple2<T, Integer> element : L) {
            topN.put(element._2, element._1);
            if (topN.size() > N) {
                topN.remove(topN.firstKey());
            }
        }
        return topN;
    }

    public static void main(String[] args) {
        List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
        list.add(new Tuple2<String, Integer>("a",13));
        list.add(new Tuple2<String, Integer>("b",1223));
        list.add(new Tuple2<>("c",14));
        list.add(new Tuple2<>("d",3));
        list.add(new Tuple2<>("e",23));
        list.add(new Tuple2<>("f",53));
        list.add(new Tuple2<>("g",423));
        list.add(new Tuple2<>("h",1423));
        list.add(new Tuple2<>("z",1263));
        TopN<String> t = new TopN<String>();
        SortedMap map = t.topN(list,5);
        System.out.println(map);

    }
}
