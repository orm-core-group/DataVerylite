using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using DataVeryLite.Core;

namespace DataVeryLite.NTest
{
    [DataVeryLite.DataBase]
    public class DandelionExt : DataVeryLite.Core.EntityPool
    {
        static DandelionExt()
        {
            Configure.LogLevel = TraceLevel.Verbose;
        }
        public static readonly int Count = 100*1;
    }

    [DataVeryLite.DataBase(Key = "sqlite1")]
    public class DandelionNew : DataVeryLite.Core.EntityPool<DandelionNew>
    {

    }

    [DataVeryLite.Table(Name = "Work2",EnableSync = true)]
    public partial class Work : DataVeryLite.Core.Entity
    {
        [DataVeryLite.Column(Name = "Id")]
        public uint Id { get; set; }

        [DataVeryLite.Column(Name = "Email")]
        public string Email { get; set; }

        [DataVeryLite.Column(Name = "WorkName",IsNullAble = false,Length = 15)]
        public string WorkName { get; set; }

        [DataVeryLite.Column(Name = "Phone")]
        public string Phone { get; set; }

        [DataVeryLite.Column(Name = "Id3",IsNullAble = false)]
        public uint Id3 { get; set; }

        [DataVeryLite.Column(Name = "Id4")]
        public string Id4 { get; set; }
    }

    [DataVeryLite.Table(EnableSync = true)]
    public partial class Person : DataVeryLite.Core.Entity
    {
        [DataVeryLite.Column(Name = "Id",IsAutoGrow = true,IsPrimaryKey = true)]
        public int id { get; set; }

        [DataVeryLite.Column(Name = "Name", Length = 222)]
        public string name1 { get; set; }

        [DataVeryLite.Column(Name = "Sex")]
        public string Sex { get; set; }

        [DataVeryLite.Column(Name = "Phone")]
        public string Phone { get; set; }

        [DataVeryLite.Column(Name = "Email")]
        public string Email { get; set; }

        [DataVeryLite.Column]
        public DateTime CreateDate { get; set; }

        [DataVeryLite.Column]
        public Guid AGuid { get; set; }
    }

    [DataVeryLite.Table(Name = "select",EnableSync = true)]
    public partial class Job : DataVeryLite.Core.Entity
    {
        [DataVeryLite.Column(IsAutoGrow = true, IsPrimaryKey = true)]
        public int Id { get; set; }

        [DataVeryLite.Column(Name = "delete",Length = 50)]
        public string JobName { get; set; }

        [DataVeryLite.Column]
        public uint AuInt { get; set; }

        [DataVeryLite.Column]
        public short AShort { get; set; }

        [DataVeryLite.Column]
        public ushort AuShort { get; set; }

        [DataVeryLite.Column]
        public long ALong { get; set; }

        [DataVeryLite.Column]
        public ulong AuLong { get; set; }

        [DataVeryLite.Column]
        public float AFloat { get; set; }

        [DataVeryLite.Column]
        public double ADouble { get; set; }

        [DataVeryLite.Column]
        public decimal ADecimal {get; set; }

        [DataVeryLite.Column]
        public bool ABool { get; set; }

        [DataVeryLite.Column]
        public byte AByte { get; set; }

        [DataVeryLite.Column]
        public byte[] ABytes { get; set; }

        [DataVeryLite.Column]
        public DateTime CreateDate { get; set; }

        [DataVeryLite.Column(Type = "varchar",Length = 37)]
        public string ANvarchar { get; set; }

        [DataVeryLite.Column(Length = int.MaxValue)]
        public string AText { get; set; }

        [DataVeryLite.Column(Type = "text")]
        public string ANtext { get; set; }

        [DataVeryLite.Column]
        public Guid AGuid { get; set; }

        [DataVeryLite.Column]
        public Guid AGuid1{ get; set; }

        [DataVeryLite.Column]
        public Guid AGuid2 { get; set; }
    }

    public class MyTraceListener : TextWriterTraceListener
    {
        public override void WriteLine(string message)
        {
            if (message != null && message.Contains("[Info]"))
            {
                Console.ForegroundColor = ConsoleColor.Green;
            }
            else if (message != null && message.Contains("[Warning]"))
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
            }
            else if (message != null && message.Contains("[Error]"))
            {
                Console.ForegroundColor = ConsoleColor.Red;
            }
            Console.WriteLine(message + base.Name);
            Console.ResetColor();
            base.WriteLine(message);
        }
    }

    /*public class MyConnectionStringProvider : ConnectionStringSettingsProvider
    {
        public override List<ConnectionStringSettings> ToConnectionString()
        {
            var sqlServverConnStr = new ConnectionStringSettings
                {
                    Name = "sqlserver1",
                    ConnectionString = "Data Source=host;Initial Catalog=db;Integrated Security=True",
                    ProviderName = "sqlserver"
                };
            var mysqlConnStr = new ConnectionStringSettings
            {
                Name = "mysql1",
                ConnectionString = "server = host; user id = name; password = pwd; database = db;",
                ProviderName = "sqlserver"
            };
            return new List<ConnectionStringSettings>() { sqlServverConnStr,mysqlConnStr };
        }
    }*/
}