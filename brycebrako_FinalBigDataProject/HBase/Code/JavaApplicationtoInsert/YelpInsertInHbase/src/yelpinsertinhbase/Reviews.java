/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package yelpinsertinhbase;

import au.com.bytecode.opencsv.CSVReader;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author bryce
 */
public class Reviews {
    
    public void reviewinsert() throws IOException
    {
        
        System.out.println("Inserting into review table");
        Configuration config = HBaseConfiguration.create();
        
        HTable table = new HTable(config, "reviewtable");
        
//        BufferedReader b = new BufferedReader(new FileReader("/home/bryce/Documents/BigDataFinalProject/data/smallcsv/review.csv"));            
//        String line1;
//        String header1 = b.readLine();
//        
//        String[]header = header1.split(",");
        
         CSVReader reader = new CSVReader(new FileReader("/home/bryce/Documents/BigDataFinalProject/small_csv2_final/data/review_small.csv"));
         String [] header = reader.readNext();
         
         String[] line;
        //while((line1 = b.readLine())!=null)
        while((line = reader.readNext()) != null) 
        {
            //String[] line = line1.split(",");
            //String[] line = line1.replaceAll("^\"", "").split("\"?(,|$)(?=(([^\"]*\"){2})*[^\"]*$) *\"?");
            Put p = new Put(Bytes.toBytes(line[1]));
//            System.out.println(""+line[0]);
//            System.out.println(""+line[1]);
//            System.out.println(""+line[2]);
//            System.out.println(""+line[3]);
//            System.out.println(""+line[4]);
            p.add(Bytes.toBytes("rinfo"), Bytes.toBytes(header[0]),Bytes.toBytes(line[0]));
            p.add(Bytes.toBytes("rinfo"), Bytes.toBytes(header[2]),Bytes.toBytes(line[2]));                                    
            p.add(Bytes.toBytes("rinfo"), Bytes.toBytes(header[4]),Bytes.toBytes(line[4]));
            p.add(Bytes.toBytes("rinfo"), Bytes.toBytes(header[6]),Bytes.toBytes(line[6]));
            p.add(Bytes.toBytes("rinfo"), Bytes.toBytes(header[7]),Bytes.toBytes(line[7]));
            p.add(Bytes.toBytes("rinfo"), Bytes.toBytes(header[8]),Bytes.toBytes(line[8]));
            
            
            p.add(Bytes.toBytes("votes"), Bytes.toBytes(header[3]),Bytes.toBytes(line[3]));
            p.add(Bytes.toBytes("votes"), Bytes.toBytes(header[5]),Bytes.toBytes(line[5]));
            p.add(Bytes.toBytes("votes"), Bytes.toBytes(header[9]),Bytes.toBytes(line[9]));
            
            table.put(p);
            
        }
        
        
        
    }
            
    
}
