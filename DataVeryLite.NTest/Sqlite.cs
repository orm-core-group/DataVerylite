using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using DataVeryLite.Core;

namespace DataVeryLite.NTest
{
    public  class Sqlite
    {
        readonly TestSql _testSql = new TestSql();

        [NUnit.Framework.TestFixtureSetUp]
        public void Init()
        {
            Configure.SetKey = (sender) => "sqlite1";
            _testSql.Init();
        }

        [NUnit.Framework.Test]
        public void a_Save()
        {
            _testSql.a_Save();
        }

        [NUnit.Framework.Test]
        public void b_Load()
        {
            _testSql.b_Load();
        }

        [NUnit.Framework.Test]
        public void c_Update()
        {
            _testSql.c_Update();
        }

        [NUnit.Framework.Test]
        public void d_Del()
        {
            _testSql.d_Del();
        }

        [NUnit.Framework.Test]
        public void e_BatchSave()
        {
            _testSql.e_BatchSave();
        }

        [NUnit.Framework.Test]
        public void f_List()
        {
            _testSql.f_List();

        }

        [NUnit.Framework.Test]
        public void g_BatchUpdate()
        {
            _testSql.g_BatchUpdate();
        }
        [NUnit.Framework.Test]
        public void y_BatchDel()
        {
            _testSql.z_BatchDel();
        }
        [NUnit.Framework.Test]
        public void z_ShowTime()
        {
            _testSql.ShowTime();
        }

        [NUnit.Framework.TestFixtureTearDown]
        public void Dispose()
        {
            _testSql.Dispose();
        }
    }
}
