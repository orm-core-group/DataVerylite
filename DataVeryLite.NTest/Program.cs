using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using DataVeryLite.Core;

namespace DataVeryLite.NTest
{
    class Program
    {
        static void Main(string[] args)
        {
            //DataVeryLite.Core.Configure.EnableSync = true;

            ////Trace.Listeners.Add(new ConsoleTraceListener());
            //Trace.Listeners.Add(new MyTraceListener());
            //var tr2 = new TextWriterTraceListener(AppDomain.CurrentDomain.BaseDirectory + "\\dlog.txt");
            //Trace.Listeners.Add(tr2);

            //try
            //{
            //    var test = new Sqlite();
            //    Console.WriteLine("init");
            //    test.Init();
            //    Console.WriteLine("a_Save");
            //    test.a_Save();
            //    Console.WriteLine("b_Load");
            //    test.b_Load();
            //    Console.WriteLine("c_Update");
            //    test.c_Update();
            //    Console.WriteLine("d_Del");
            //    test.d_Del();
            //    Console.WriteLine("e_BatchSave");
            //    test.e_BatchSave();
            //    Console.WriteLine("f_List");
            //    test.f_List();
            //    Console.WriteLine("g_BatchUpdate");
            //    test.g_BatchUpdate();
            //    Console.WriteLine("z_BatchDel");
            //    test.y_BatchDel();
            //    test.z_ShowTime();
            //    Console.WriteLine("Dispose");
            //    test.Dispose();
            //}
            //catch (Exception ex)
            //{
            //    File.AppendAllText(AppDomain.CurrentDomain.BaseDirectory+"\\log.txt",ex.ToString());
            //}
            //Console.WriteLine("over");
            //Console.ReadLine();

            var lu = new LocalUser();
            lu.Load(By.Id(1));
        }
    }


    [DataVeryLite.DataBase(Key = "sqlite1")]
    public class LocalUserDb : EntityPool
    {

    }
    [DataVeryLite.Table(EnableSync = true, Key = "sqlite1")]
    public partial class LocalUser : DataVeryLite.Core.Entity
    {
        [DataVeryLite.Column(Name = "Id", IsAutoGrow = true, IsPrimaryKey = true)]
        public int Id { get; set; }


        [DataVeryLite.Column]
        public string Name { get; set; }

        [DataVeryLite.Column]
        public int IsActive { get; set; }


        [DataVeryLite.Column]
        public DateTime CreateTime { get; set; }

        [DataVeryLite.Column]
        public DateTime LastLoinTime { get; set; }


    }
}
