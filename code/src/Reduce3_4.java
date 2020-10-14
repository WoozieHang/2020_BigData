import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Vector;

public class Reduce3_4 extends Reducer<Text,Text,Text,Text> {
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map3_4      输出key：className                    输出value：vector
    //reducer3_4  输出key：className                  输出value：totalClassNum;mean vector;variance vector

    @Override
    public void reduce(Text key,Iterable<Text> value,Context context) throws IOException,InterruptedException {
        int dimension=50000;
        int totalClassNum=0;
        Vector<Double> vectorMean=new Vector<Double>();
        Vector<Double> vectorVariance=new Vector<Double>();
        Vector<Double > vectorMean2=new Vector<Double>();
        for(int i=0;i<dimension;i++){
            vectorMean.add(0.0);
            vectorVariance.add(0.0);
            vectorMean2.add(0.0);
        }

        for(Text v:value) {
            totalClassNum++;
            String []Content=v.toString().split("&")[0].split(",");
            String []Index=v.toString().split("&")[1].split(",");
            for(int i=0;i<Index.length;i++){
                int id=Integer.parseInt(Index[i]);
                Double con=Double.parseDouble(Content[i]);
                vectorMean.set(id,vectorMean.get(id)+con);
                vectorMean2.set(id,vectorMean2.get(id)+con*con);
            }
        }

        for(int i=0;i<dimension;i++){
            vectorMean.set(i,vectorMean.get(i)/dimension);
            vectorMean2.set(i,vectorMean2.get(i)/dimension);
            double E_x2=vectorMean2.get(i);
            double Ex_2=vectorMean.get(i);
            Ex_2*=Ex_2;
            vectorVariance.set(i,E_x2-Ex_2);
        }

        String outValue=totalClassNum+"\t"+vectorMean.get(0);
        for(int i=1;i<vectorMean.size();i++){
            outValue+=","+vectorMean.get(i);
        }
        outValue+="\t"+vectorVariance.get(0);
        for(int i=1;i<vectorVariance.size();i++){
            outValue+=","+vectorVariance.get(i);
        }
        context.write(key,new Text(outValue));
    }
}
