/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.vtys;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author veysi
 */
public class MyApp {

    public static void main(String[] args) throws Exception {
        birinciSoru();
        ikinciSoru();
        ucuncuSoru();
        dorduncuSoru();

    }

    public static Connection getConnection() throws Exception {
        try {
            String driver = "com.mysql.jdbc.Driver";
            String url = "jdbc:mysql://localhost:3306/db";
            String username = "root";
            String password = "";
            Class.forName(driver);

            Connection conn = DriverManager.getConnection(url, username, password);
            System.out.println("Connected");
            return conn;
        } catch (Exception e) {
            System.out.println(e);
        }

        return null;
    }

    public static void birinciSoru() throws Exception {
        try {

            // Veri tabanına bağlanamk için getConnect() methodunu çağırdık
            // Bu methodun return ettiği Connection Clasından con nesnesi ile
            // veriTabanında yapacağımız işlemler için bize bağlantı kuruyor.
            Connection con = getConnection();
            // soru birin tablosunun oluşturduk
            PreparedStatement create = con.prepareStatement("CREATE TABLE IF NOT EXISTS AutherPaper( "
                    + "id MEDIUMINT NOT NULL AUTO_INCREMENT,author varchar(550),paper varchar(550), date int(11), conference_id int(11), PRIMARY KEY (id))");
            create.executeUpdate();
            // Başlangıçta bize verilen ieee_explorer tablosundan authors,title,date,conferenceid  bilgilerni aldık
            PreparedStatement statement = con.prepareStatement("SELECT authors,title,date,conferenceid FROM ieee_explorer");
            ResultSet result = statement.executeQuery();

            while (result.next()) {
                /*
               Bazı makaleleri tek kişi bazılarını ise birden fazla kişi yazmış
                birden fazla kişiler arasına , konulmuş
                Aşağıdaki dört satır aouthors hücresinde ,  varda 
                isVirgul True değerini alacak
                yoksa tek kişi yazmışsa false değerin alacak
                 */
                String authors = result.getString("authors");
                String virgul = ",";
                Pattern pattern = Pattern.compile(virgul);
                Matcher matcher = pattern.matcher(authors);
                Boolean isVirgul = matcher.find();
// tek kişi yapmışşsa tek bi ekleme yaptık
                if (!isVirgul) {
                    System.out.println("Birnici soru verileri tabloya ekleniyor");
                    PreparedStatement posted;
                    posted = con.prepareStatement("INSERT INTO AutherPaper (author,paper,date,conference_id) VALUES "
                            + "('" + authors + "','" + result.getString("title") + "','" + result.getString("date") + "','" + result.getString("conferenceid") + "')");

                    posted.executeUpdate();
                } else {
                    // makaleyi birden fazla kişi yazmışsa split methodu ile yazarları , göre yakayıp bir diziye attık
                    String[] elemanlar = authors.split(",");

                    for (int i = 0; i < elemanlar.length; i++) {
                        // bir yazara karşılık bir makale olacak şekilde tabloya ekledik
                        PreparedStatement posted;
                        posted = con.prepareStatement("INSERT INTO AutherPaper (author,paper,date,conference_id) VALUES "
                                + "('" + elemanlar[i] + "','" + result.getString("title") + "','" + result.getString("date") + "','" + result.getString("conferenceid") + "')");

                        posted.executeUpdate();
                        System.out.println("Birnici soru verileri tabloya ekleniyor");
                    }
                }

            }
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            System.out.println("Birinci soru sonlandı");
        }
    }

    public static void ikinciSoru() throws Exception {
        try {
            // Veri tabanına bağlanamk için getConnect() methodunu çağırdık
            // Bu methodun return ettiği Connection Clasından con nesnesi ile
            // veriTabanında yapacağımız işlemler için bize bağlantı kuruyor.
            Connection con = getConnection();
            // soru AuthorPaperCount tablosunun oluşturduk

            PreparedStatement create = con.prepareStatement("CREATE TABLE IF NOT EXISTS AuthorPaperCount( "
                    + "author varchar(550),paperCount int(11))");
            create.executeUpdate();

            // AutherPaper tablosundan auther ve akç tane olduğunu çekip AutherPaerCount tablosuna ekledik
            PreparedStatement statement = con.prepareStatement("INSERT INTO AuthorPaperCount  SELECT author, COUNT(*) AS paperCount FROM AutherPaper GROUP BY author");
            statement.executeUpdate();

        } catch (Exception e) {
            System.out.println(e);
        } finally {
            System.out.println("ikinci soru sonlandı");
        }
    }

    public static void ucuncuSoru() throws Exception {
        try {

            // Veri tabanına bağlanamk için getConnect() methodunu çağırdık
            // Bu methodun return ettiği Connection Clasından con nesnesi ile
            // veriTabanında yapacağımız işlemler için bize bağlantı kuruyor.
            Connection con = getConnection();
            PreparedStatement statement = con.prepareStatement("SELECT date,conferencename,count(title) AS paper_count FROM ieee_explorer group by conferencename");
            ResultSet result = statement.executeQuery();

            while (result.next()) {
                System.out.println("Date_conferance=" + result.getString("date") + "\n "
                        + "Conferance_name= " + result.getString("conferencename") + " \n "
                        + "Published_paper_count= " + result.getString("paper_count")
                );
                System.out.println("****");
            }

        } catch (Exception e) {
            System.out.println(e);
        } finally {
            System.out.println("ucuncusoru soru sonlandı");
        }
    }

    public static void dorduncuSoru() throws Exception {
        try {

            // Veri tabanına bağlanamk için getConnect() methodunu çağırdık
            // Bu methodun return ettiği Connection Clasından con nesnesi ile
            // veriTabanında yapacağımız işlemler için bize bağlantı kuruyor.
            Connection con = getConnection();
            PreparedStatement statement = con.prepareStatement("SELECT author,GROUP_CONCAT(author) AS CO_NAME, COUNT(*) AS AuthorCount FROM AutherPaper group by paper");
            ResultSet result = statement.executeQuery();

            while (result.next()) {
                System.out.println("Author=" + result.getString("author"));
                System.out.println("Co_aouthors_count= " + result.getString("AuthorCount"));
                System.out.println("Co_authors_naems= " + result.getString("CO_NAME") );
                
                System.out.println("****");
            }

        } catch (Exception e) {
            System.out.println(e);
        } finally {
            System.out.println("doruncu soru sonlandı");
        }
    }
}
