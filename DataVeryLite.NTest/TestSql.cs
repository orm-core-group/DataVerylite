using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using DataVeryLite.Core;

namespace DataVeryLite.NTest
{
    public class TestSql
    {
        private DandelionExt _dandelionExt;
        private DbTransaction _transaction;
        private List<Person> _entities;
        private double _time;

        public void Init()
        {
            _entities = new List<Person>();
            _time = Environment.TickCount;
            _dandelionExt = new DandelionExt();
            _transaction = _dandelionExt.BeginTransaction();
        }

        public void a_Save()
        {
            var person = new Person { name1 = "天大地大", Phone = "11001100" };
            person.Save();
            _entities.Add(person);
        }

        public void b_Load()
        {
            var p = _entities.FirstOrDefault();
            var p1 = new Person();
            p1.Load(By.Id(p.id));
        }

        public void c_Update()
        {
            var p = _entities.FirstOrDefault();
            p.Email = "qq.com";
            p.SetValue("Email", "qq.com");
            p.Update();
        }

        public void d_Del()
        {
            var p = _entities.FirstOrDefault();
            p.Del();
        }

        public void e_BatchSave()
        {
            _entities.Clear();
            for (int i = 0; i < DandelionExt.Count; i++)
            {
                _entities.Add(new Person() { Email = i + "gg@gmail.com", name1 = "save" + i });
            }
            _dandelionExt.Save(_entities, true, _transaction);

            _entities.Clear();
            for (int i = 0; i < DandelionExt.Count; i++)
            {
                _entities.Add(new Person() { Email = i + "gg@gmail.com", name1 = "save" + i });
            }
            _dandelionExt.SaveOnly(_entities, true, _transaction);
        }

        public void f_List()
        {
            var ps = _dandelionExt.List<Person>(By.Page(1, 10, true, _transaction));
            foreach (var person in ps)
            {
                Console.WriteLine(person.id + "-" + person.name1);
            }
            ps = _dandelionExt.List<Person>(By.Between(1, 10, false, _transaction));
            foreach (var person in ps)
            {
                Console.WriteLine(person.id + "-" + person.name1);
            }
            ps = _dandelionExt.List<Person>(By.Top(5, true, _transaction));
            foreach (var person in ps)
            {
                Console.WriteLine(person.id + "-" + person.name1);
            }
            //Console.WriteLine(new Person().Count);
            _entities.Clear();
            ps = _dandelionExt.List<Person>(By.All(true, _transaction));
            _entities.AddRange(ps);
            var list = _dandelionExt.List("select * from Person", _transaction);
            foreach (var o in list)
            {
                Console.WriteLine(o.Name);
            }
        }

        public void g_BatchUpdate()
        {
            int i = 0;
            foreach (var entity in _entities)
            {
                i++;
                entity.name1 = "update" + i;
                entity.SetValue("name1", "update" + i);
            }
            _dandelionExt.Update(_entities, _transaction);
        }
        public void z_BatchDel()
        {
            _dandelionExt.Del(_entities, _transaction);
        }

        public void Dispose()
        {
            if (_transaction != null)
            {
                var conn = _transaction.Connection;
                _transaction.Commit();
                _transaction.Dispose();
                conn.Close();
                conn.Dispose();
            }
        }
        public void ShowTime()
        {
            _time = (Environment.TickCount - _time) * 1.0 / 1000;
            Console.WriteLine("Total second is " + _time);
        }
    }
}
