package iqiyi;



import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.List;

/**
 * Created by gaoronglei_sx on 2017/8/24.
 */
public class ObjectStreamDemo
{

   public static void main(String[] args)
   {
       BufferedReader bufr = null;
       try {
           Path p = Paths.get("E:\\weibo.txt");
//           List<String> content = Files.readAllLines(p,Charset.forName("utf-8"));
//           Path path2 = Paths.get("E:\\aaa.txt");
//           Files.write(path2,content);
          bufr = Files.newBufferedReader(p,Charset.forName("utf-8"));
          String line = null;
          while ((line = bufr.readLine())!= null)
          {
              System.out.println(line);
          }

       } catch (IOException e) {
           e.printStackTrace();
       }finally {
           if(bufr != null)
           {
               try {
                   bufr.close();
               } catch (IOException e) {
                   e.printStackTrace();
               }
           }
       }
   }

   public static void findFiles(Path path)
   {
       try {
           DirectoryStream<Path> paths = Files.newDirectoryStream(path);
           for(Path p : paths)
           {

           }
       } catch (IOException e) {
           e.printStackTrace();
       }
   }

   public void inputObject()
   {
       ObjectInputStream objInPut = null;
       try {
           objInPut = new ObjectInputStream(new FileInputStream("E:"+ File.separator+"Employee.txt"));
           Employee employee = null;
           while((employee = (Employee)objInPut.readObject()) != null)
           {
               System.out.println(employee.toString());
           }
       } catch (IOException e) {
           e.printStackTrace();
       }catch (ClassNotFoundException e)
       {
           e.printStackTrace();
       }
       finally {
           if(objInPut != null)
           {
               try {
                   objInPut.close();
               } catch (IOException e) {
                   e.printStackTrace();
               }
           }
       }
   }
   public void outputObject()
   {
       ObjectOutputStream objOutPut = null;
//       try {
//           objOutPut = new ObjectOutputStream(new FileOutputStream("E:"+ File.separator+"Employee.txt"));
//           Employee lilei = new Employee("lilei",18,9000.000);
//           Employee haimeimei = new Employee("hanmeimei",18,8000.000);
//           objOutPut.writeObject(lilei);
//           objOutPut.writeObject(haimeimei);
//
//       } catch (IOException e) {
//           e.printStackTrace();
//       }
//       finally {
//           if(objOutPut != null)
//           {
//               try {
//                   objOutPut.close();
//               } catch (IOException e) {
//                   e.printStackTrace();
//               }
//           }
//       }
   }
}

class Employee implements Serializable{
    private String Name;
    private int Age;
    private double Salary;
    Employee(String name,int age,double salary)
    {
        Name = name;
        Age = age;
        Salary = salary;
    }

    public String toString()
    {
        String valueStr = "Name="+Name+";Age="+Age+";Salary="+Salary;
        return valueStr;
    }

}

