# DataVerylite
DataVeryLite is a lightweight *Persistence Framework*. 

# Superiority

   Easy to use
   No config
# NuGet

   PM> Install-Package DataVeryLite
# Example lite

      using System;
      public class HelloWorld
      {
         public static void Main(params string[] args)
         {
             var p=Models.Xe.Person();
             p.Load(By.Id(1));
             p.Del();
             Console.WriteLine(p.Name+","+p.Age);
         }
      }
