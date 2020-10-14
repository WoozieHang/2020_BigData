import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;

public class Map3_5 extends Mapper<Object, Text,Text,Text>
{
    //默认读取：每行的行号作为key，每行的内容作为value
    //Map3_5 输出key：actual className      输出value：forecast className
    //reducer3_5 输出key：actual className     输出value：forecast className

    private static Vector<String> vectorClassName=new Vector<String>();
    private static Vector<String> vectorTotalNum=new Vector<String>();
    private static Vector<String>vectorMean=new Vector<String>();
    private static Vector<String>vectorVariance=new Vector<String>();
    private Path[] localFiles;

    @Override
    public void setup(Context context) throws IOException,InterruptedException{
        Configuration conf=context.getConfiguration();
        localFiles= context.getLocalCacheFiles();
        //将缓存文件内容读入到当前Map Task的全局变量中
        String lineStr;
        BufferedReader br=new BufferedReader(new FileReader(localFiles[0].toString()));
        while((lineStr=br.readLine())!=null){
            vectorClassName.add(lineStr.split("\t")[0]);
            vectorTotalNum.add(lineStr.split("\t")[1]);
            vectorMean.add(lineStr.split("\t")[2]);
            vectorVariance.add(lineStr.split("\t")[3]);
        }
        br.close();
    }

    @Override
    public void map(Object key,Text value,Context context) throws IOException,InterruptedException
    {
        String str=value.toString();
        String testClassName=str.split("\t")[0];
        String []testVectorContent=str.split("\t")[1].split("&")[0].split(",");
        String []testVectorId=str.split("\t")[1].split("&")[1].split(",");
        Vector<Double>testVector=new Vector<Double>();
        int classNum=vectorClassName.size();
        int dimension=50000;

        for(int i=0;i<dimension;i++){
            testVector.add(0.0);
        }
        for(int i=0;i<testVectorId.length;i++){
            int id=Integer.parseInt(testVectorId[i]);
            double con=Double.parseDouble(testVectorContent[i]);
            testVector.set(id,con);
        }

        Vector<Double> vectorPossibility=new Vector<Double>();
        for(int i=0;i<classNum;i++){
            double result=Math.log(Double.parseDouble(vectorTotalNum.get(i)));
            String []tmpMeanStr=vectorMean.get(i).split(",");
            String []tmpVarianceStr=vectorVariance.get(i).split(",");
            for(int j=0;j<dimension;j++){
                double tmpMean=Double.parseDouble(tmpMeanStr[j]);
                double tmpVariance= Double.parseDouble(tmpVarianceStr[j])+0.0000000001;
                double tmpX=testVector.get(j);
                double tmp1 = tmpX - tmpMean;
                tmp1 = tmp1 * tmp1;
                double tmp2 = tmpVariance * tmpVariance * (2);
                double tmp3=-1*(tmp1/tmp2);
                double tmp4=Math.log(Math.sqrt(2 * Math.PI) * tmpVariance);
                double tmp5=tmp3-tmp4;
                result += tmp5;
            }
            vectorPossibility.add(result);
        }

        int max_id=0;
        double max=vectorPossibility.get(0);
        for(int i=1;i<classNum;i++){
            if(vectorPossibility.get(i)>max){
                max_id=i;
                max=vectorPossibility.get(i);
            }
            //context.write(new Text("debug"),new Text(testClassName+"\t"+vectorClassName.get(i)+"\t"+vectorPossibility.get(i)));
        }
        context.write(new Text(testClassName),new Text(vectorClassName.get(max_id)));
    }
}
