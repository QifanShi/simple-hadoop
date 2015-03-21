import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author shiqifanshiqifan
 */
public class test {

    public static void main(String[] args) throws ParseException {
        String s = "1350610.973	415402.812	401851";
        String[] token = s.split("\t");

        double x = Double.parseDouble(token[0]);
        double y = Double.parseDouble(token[1]);

        // coordinates of 3803 Forbes Ave.  
        double x2 = 1354326.897;
        double y2 = 411447.7828;
        // compute distance
        double dist = Math.sqrt(Math.pow((x - x2), 2) + Math.pow((y - y2), 2));
        System.out.println(dist);
    }
}
