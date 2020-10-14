import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Vector;

public class Reduce4 extends Reducer<Text,Text,Text,Text> {
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map4_1      输出key：className                    输出value：1/0
    //reducer4_1  输出key：className                  输出value：correct rate

    @Override
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        Vector<Integer> vector=new Vector<Integer>();
        for (Text v : value) {
            vector.add(Integer.parseInt(v.toString()));
        }
        int correctNum=0;
        for(int i=0;i<vector.size();i++){
            correctNum+=vector.get(i);
        }
        double rate=((double)(100*correctNum))/((double)vector.size());
        DecimalFormat d=new DecimalFormat("#.00");
        String OutKey=vector.size()+"\t"+d.format(rate)+"%";
        context.write(key,new Text(OutKey));
    }
}
