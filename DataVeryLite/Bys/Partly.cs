namespace DataVeryLite.Bys
{
    public class Partly
    {
        private Partly()
        {
        }

        public string[] Fields { get; set; }

        public static Partly Columns(params string[] fields)
        {
            return new Partly {Fields = fields};
        }
    }
}
