using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using DataVeryLite.Bys;
using DataVeryLite.Core;
//using DataVeryLite.Test.Models.DandelionExt;
using DataVeryLite.Util;

namespace DataVeryLite.Test
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            //Debug.Listeners.Add(new ConsoleTraceListener());
            ShowExecuteTime("SqliteTestDetail", () => TestDetail<Models.Localite.Person, Models.Localite.Localite>("Sqlite"));
            //ShowExecuteTime("Db2TestDetail", () => TestDetail<Models.Sample.Person, Models.Sample.Sample>("Db2"));
            //ShowExecuteTime("PostgresqlTestDetail", () => TestDetail<Models.MyDb.Person, Models.MyDb.MyDb>("Postgresql"));
            //ShowExecuteTime("MysqlTestDetail", () => TestDetail<Models.Test.Person, Models.Test.Test>("Mysql"));
            //ShowExecuteTime("OracleTestDetail", () => TestDetail<Models.Xe.Person, Models.Xe.Xe>("Oracle"));
            //ShowExecuteTime("AccessTestDetail", () => TestDetail<Models.LocalAccess.Person, Models.LocalAccess.LocalAccess>("Access"));
            //ShowExecuteTime("SqlServerDetail", () => TestDetail<Models.DandelionExt.Person, Models.DandelionExt.DandelionExt>("SqlServer"));
            Console.WriteLine("The test program is end.");
            Console.ReadLine();
        }

		public static void TestDetail<TEntity, TEntityPool>(string title)
            where TEntity : Entity
            where TEntityPool : EntityPool
        {
            string _left = ProviderManager.GetProvider(title.ToLower()).GetLeftEscape();
            string _right = ProviderManager.GetProvider(title.ToLower()).GetRightEscape();
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("=======" + title + " test running=======");

            var lo = Activator.CreateInstance<TEntityPool>();
            DbTransaction tran = lo.BeginTransaction();

            #region Betch Deal

            ShowExecuteTime("TestSave person 1000", () =>
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        dynamic person = Activator.CreateInstance<TEntity>();
                        person.Name = "China" + i;
                        person.Save(tran);
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        //Console.WriteLine("Save person " + i + " sucessfully!");
                    }
                });

            ShowExecuteTime("TestListSave no id person 1000", () =>
                {
                    var persons = new List<TEntity>();
                    for (int i = 0; i < 1000; i++)
                    {
                        dynamic person = Activator.CreateInstance<TEntity>();
                        person.Name = "China" + i;
                        persons.Add(person);
                    }
                    lo.SaveOnly(tran, persons);
                });
            ShowExecuteTime("TestListSave get id person 1000", () =>
                {
                    var persons = new List<TEntity>();
                    for (int i = 0; i < 1000; i++)
                    {
                        dynamic person = Activator.CreateInstance<TEntity>();
                        person.Name = "China" + i;
                        persons.Add(person);
                    }
                    lo.Save(tran, persons);
                });

            #endregion
            lo.Commit();
            #region One

            dynamic p = Activator.CreateInstance<TEntity>();
            ShowExecuteTime("Load one person", () =>
                {
                    p.Load(By.Id(3021));
                });
            ShowExecuteTime("Load partly condition", () =>
                {
                    p.Load(Partly.Columns("Id", "Name"), Condition.Where("Id", "Name"));
                });
            p.Name = "12中国";
            ShowExecuteTime("Update one person", () =>
                {
                    p.Update();
                });
            ShowExecuteTime("Save one person", () =>
                {
                    p.Save();
                });
            ShowExecuteTime("Load by sql, person", () =>
                {
                    p.Load(By.Sql("select * from " + _left + "Person" + _right + " order by " + _left + "Id" + _right + "  desc"));
                });
            ShowExecuteTime("Del one person", ()=>p.Del());
            ShowExecuteTime("Get person count", () =>
                {
                    //var recordCount = TEntity
                });
            //no trans
            p.Load(By.Id(3021));
            p.Del();
            p.Update();

            #endregion

            #region List test

            ShowExecuteTime("ListTest", () =>
                {
                    var a = lo.List("select * from " + _left + "Person" + _right + "");
                    //var b = lo.List<TEntity>(By.All());
                   // var b1 = lo.List<TEntity>(By.All(false));
                    //var c = lo.List<TEntity>(By.Between(1, 10));
                    //var c1 = lo.List<TEntity>(By.Between(1, 10, false));
                    //var d = lo.List<TEntity>(By.Sql("select * from " + _left + "Person" + _right + ""));
                    //var d1 = lo.List<TEntity>(By.Sql("select " + _left + "Name" + _right + "  from " + _left + "Person" + _right + ""));
                    //var e = lo.List<TEntity>(By.Top(10));
                    //var e1 = lo.List<TEntity>(By.Top(10, false));
                    //var l = lo.List<TEntity>(By.Page(1, 10));
                    //var l1 = lo.List<TEntity>(By.Page(1, 10, false));
                });

            #endregion


            dynamic p1 = Activator.CreateInstance<TEntity>();

            dynamic p2 = Activator.CreateInstance<TEntity>();
            p2.Name = "p2n";

            ShowExecuteTime("TestListSave person 2", ()=>lo.Save(p1, p2));
            p1.Name = "p1g";
            p2.Name = "p2g";
            ShowExecuteTime("TestListUpate person 2", ()=>lo.Update(p1, p2));
            ShowExecuteTime("TestListDel person 2", ()=>lo.Del(p1, p2));
        }

        public static void ShowExecuteTime(string name, Action action)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(name + " start,please wait.");
            Debug.WriteLine(name + " start,please wait.");
            int start = Environment.TickCount;
            action();
            int during = Environment.TickCount - start;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(name + ":During time is " + during/1000.0 + " s");
            Debug.WriteLine(name + ":During time is " + during/1000.0 + " s");
            Console.WriteLine();
        }
    }
}
