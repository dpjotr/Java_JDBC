/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DBProcessingPackage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author pjotr
 */


public class MainClass {
    
    
    
    
    public static void main(String[]args){
       
        
        
        
        Connection connection=null;    
        //Open connection
        try {
            connection = DriverManager
                    .getConnection("jdbc:derby://localhost:1527/JavaDALab6DB", "adm", "adm");
            System.out.println("Connection done");
        } catch (SQLException ex) {
            System.out.println("Error: " + ex.getMessage());            
        }
        
        
        
        //Create tables
        Statement statement=null;       
        
        try {
            statement = connection.createStatement();
        } catch (SQLException ex) {
            Logger.getLogger(MainClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        try {
            statement.execute("CREATE TABLE Users (id INTEGER PRIMARY KEY, "
                    + "login CHAR(8) unique)");
        } catch (SQLException ex) {
            System.out.println("The table seems to be already created");           
        }

        try {
            statement.execute(
                    "CREATE TABLE Registrations (code INTEGER PRIMARY KEY, "
                    + "id INTEGER NOT NULL, "
                    + "role VARCHAR(16), "
                    + "date timestamp, "
                    + "CONSTRAINT Registration_To_Users FOREIGN KEY (id) "
                            + "REFERENCES Users(id) ON DELETE CASCADE, "
                    + "UNIQUE (id, role, date)"
                    + ")");
        } catch (SQLException ex) {
            System.out.println("The table seems to be already created");
        }
        
        //Parse
        String fileContent="";
        try{
            fileContent=readFile(
                    "/home/pjotr/NetBeansProjects/JavaDALab6/data.log"); 
        }
        catch(IOException e){e.getMessage();}
        
        System.out.println("Initial file:\n"+fileContent);       
        
        String[] lines=fileContent.split("\r\n");
        
        ArrayList<String[]> splittedData = new ArrayList();        
        for(String x:lines) splittedData.add(x.split(" "));        
       
        //users- Results for table Users
        Map<String,String>users=new HashMap<String,String>();
        int userCounter=0;
        for(String[] x:splittedData) 
            if(!users.containsValue(x[0])) 
                users.put(x[0], ""+userCounter++);
        
        Integer regId=0;
        
        //registrations- Results for table Registrations
        ArrayList<String[]> registrations = new ArrayList();  
        for(String[] x:splittedData) 
            registrations.add(
                    new String[] {""+regId++
                                    , users.get(x[0])
                                    , x[1]
                                    , x[2].replace('.', '-')
                                            .replaceAll("^(\\d*)(-\\d*-)(\\d*)$", "'$3$2$1'")
                                            +", " 
                                            +"'"+x[3].replace(':', '.')+"'"});
        //Fill table Users        
         for (Map.Entry<String,String> x : users.entrySet()) {

            try {
                statement.execute(
                        "INSERT INTO Users (id, login) "
                                + "VALUES ("+x.getValue()+",'"+x.getKey()+"')");
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }    
         }
         
         //Fill table Registrations
         
         for(String[]x: registrations){
              try {
            statement.execute(
                        "INSERT INTO Registrations (code, id, role, date) "
                                + "VALUES (" 
                                   +x[0]
                                +","+x[1]
                                +",'"+x[2]
                                +"', TIMESTAMP("+x[3]
                                +"))");
                

            } catch (SQLException e) {
                System.out.println(e.getMessage());

            }   
         }         
         

        System.out.println("Results:\n");
        System.out.println("Task_1- Like initial file content:");
        ResultSet result=null;
        try {
            result = statement.executeQuery("SELECT login, role, date\n"
                                            +"FROM Registrations R "
                                                + "JOIN Users U "
                                                    + "ON R.id=U.id\n"
                                            +"ORDER BY code");
        } catch (SQLException ex) {
            Logger.getLogger(MainClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        

        try {
            while(result.next())
                System.out.println(
                        result.getString("login").trim()+" "
                       +result.getString("role").trim()+" "
                       +(new SimpleDateFormat("dd.MM.yyyy hh:mm:ss").format(result.getTimestamp("date"))) );
        } catch (SQLException ex) {
            Logger.getLogger(MainClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        

        System.out.println("Task_2- Users that used the system first time since 01.10.2012:");
        try {
            result = statement.executeQuery("SELECT   DISTINCT login\n" +
                                            "FROM Registrations R JOIN Users U ON R.id=U.id\n" +
                                            "WHERE date>TIMESTAMP('2012-10-01', '00.00.00')\n");
        } catch (SQLException ex) {
            Logger.getLogger(MainClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        try {
            while(result.next())
                System.out.println(
                        result.getString("login").trim());
        } catch (SQLException ex) {
            Logger.getLogger(MainClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        System.out.println("Task_3- Users that have more than 1 role:");
        try {
                result = statement.executeQuery("SELECT   DISTINCT login\n" +
                                                "FROM Registrations R JOIN Users U ON R.id=U.id\n" +
                                                "WHERE R.id in ( SELECT   DISTINCT R1.id\n" +
                                                "                FROM registrations R1 JOIN registrations R2 ON r1.id=r2.id\n" +
                                                "                WHERE R1.role<>R2.role)");
        } catch (SQLException ex) {
            Logger.getLogger(MainClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        try {
            while(result.next())
                System.out.println(
                        result.getString("login").trim());
        } catch (SQLException ex) {
            Logger.getLogger(MainClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        System.out.println("Task_4- Dates with more than one registration:");
        try {
            result = statement.executeQuery("SELECT   DATE(date)\n" +
                                            "FROM Registrations \n" +
                                            "GROUP BY DATE(date)\n" +
                                            "HAVING COUNT(id)>1");
        } catch (SQLException ex) {
            Logger.getLogger(MainClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        try {
            while(result.next())
                System.out.println(
                        new SimpleDateFormat("dd.MM.yyyy").format(result.getTimestamp(1)));
        } catch (SQLException ex) {
            Logger.getLogger(MainClass.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        
        try {
                statement.execute("DELETE FROM Users");
                statement.execute("DELETE FROM Registrations");

        } catch (SQLException e) {
                System.out.println(e.getMessage());
          }   
         
        //Close connection
        try {
            connection.close();
            System.out.println("Connection closed");
        } catch (SQLException ex) {
            Logger.getLogger(MainClass.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    public static String readFile(String path) throws IOException{ 
        return new String(Files.readAllBytes(Paths.get(path)));    
    }
}
