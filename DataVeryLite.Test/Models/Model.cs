
using System;
using DataVeryLite.Util;
namespace DataVeryLite.Test.Models
{
 namespace Localite
  {
    [DataVeryLite.DataBase(Name = "Localite",Key = "sqlite")]
	public class Localite:DataVeryLite.EntityPools.SqliteEntityPool
	{
		
	}
    [DataVeryLite.Table(Name = "sqlite_sequence",EntityPool = typeof(Localite))]
	public partial class Sqlite_Sequence : DataVeryLite.Entities.SqliteEntity
	{
		[DataVeryLite.Column(Name = "name")]
		public string Name { get; set; }
		[DataVeryLite.Column(Name = "seq")]
		public string Seq { get; set; }
	}
    [DataVeryLite.Table(Name = "Person",EntityPool = typeof(Localite))]
	public partial class Person : DataVeryLite.Entities.SqliteEntity
	{
		[DataVeryLite.Column(Name = "Id",IsPrimaryKey = true)]
		public long Id { get; set; }
		[DataVeryLite.Column(Name = "Name")]
		public string Name { get; set; }
		[DataVeryLite.Column(Name = "Age")]
		public long Age { get; set; }
		[DataVeryLite.Column(Name = "Address")]
		public string Address { get; set; }
		[DataVeryLite.Column(Name = "Money")]
		public double Money { get; set; }
		[DataVeryLite.Column(Name = "Image")]
		public byte[] Image { get; set; }
	}
  }
}