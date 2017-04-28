namespace DataVeryLite.Bys
{
    public class Condition
    {
        private Condition()
        {
        }

        public string[] Fields { get; set; }

        public static Condition Where(params string[] fields)
        {
            return new Condition { Fields = fields };
        }
    }
}
