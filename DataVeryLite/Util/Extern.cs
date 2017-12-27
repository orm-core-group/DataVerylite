using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using DataVeryLite.Core;

namespace DataVeryLite.Util
{
    public static class Extern
    {
        public static object ChangeType(this object value, Type type)
        {
            if (type == typeof(int))
            {
                value = int.Parse(value.ToString());
            }
            else if (type == typeof(long))
            {
                value = long.Parse(value.ToString());
            }
            else if (type == typeof(short))
            {
                value = short.Parse(value.ToString());
            }
            else if (type == typeof(decimal))
            {
                value = decimal.Parse(value.ToString());
            }
            else if (type == typeof(float))
            {
                value = float.Parse(value.ToString());
            }
            else if (type == typeof(double))
            {
                value = double.Parse(value.ToString());
            }
            else if (type == typeof(DBNull))
            {
                value = null;
            }
            return value;
        }

        public static string FirstLetterToUpper(this string value)
        {
            if (string.IsNullOrEmpty(value.Trim()))
            {
                return value;
            }
            value = value.Trim();
            if (value.Length != 1)
            {
                value = value.Substring(0, 1).ToUpper() + value.Substring(1).ToLower();
            }
            else
            {
                value = value.Substring(0, 1).ToUpper();
            }
            return value;
        }

        public static string TrimAndLower(this String input)
        {
            if (input == null) return "";
            return input.Trim().ToLower();
        }

        public static string ToCamelCase1(this String input)
        {
            if (input == null) return "";

            System.Globalization.TextInfo textInfo = new System.Globalization.CultureInfo("en-US", false).TextInfo;
            string result = textInfo.ToTitleCase(input.Trim()).Replace(" ", "");

            return result;
        }

        public static bool IsDigital(this Type value)
        {
            if (value == typeof (int)
                || value == typeof (uint)
                || value == typeof (long)
                || value == typeof (ulong)
                || value == typeof (short)
                || value == typeof (ushort)
                || value == typeof (decimal)
                || value == typeof (float)
                || value == typeof (double))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public static bool IsString(this Type value)
        {
            if (value == typeof(string))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public static bool IsByteArrary(this Type value)
        {
            if (value == typeof(byte[]))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public static string ToYesNo(this bool value)
        {
            if (value)
            {
                return "YES";
            }
            else
            {
                return "NO";
            }
        }

        public static string ToYN(this bool value)
        {
            if (value)
            {
                return "Y";
            }
            else
            {
                return "N";
            }
        }

        public static long To01(this bool value)
        {
            if (value)
            {
                return 0;
            }
            else
            {
                return 1;
            }
        }

        public static string ToTrueFalse(this bool value)
        {
            if (value)
            {
                return "True";
            }
            else
            {
                return "False";
            }
        }
    }
    public static class TextUtility
    {
        private static Regex WordRegex = new Regex(@"\p{Lu}\p{Ll}+|\p{Lu}+(?!\p{Ll})|\p{Ll}+|\d+");

        public static string ToPascalCase(this string input)
        {
            return WordRegex.Replace(input, EvaluatePascal);
        }

        public static string ToCamelCase(this string input)
        {
            string pascal = ToPascalCase(input);
            return WordRegex.Replace(pascal, EvaluateFirstCamel, 1);
        }

        private static string EvaluateFirstCamel(Match match)
        {
            return match.Value.ToLower();
        }

        private static string EvaluatePascal(Match match)
        {
            string value = match.Value;
            int valueLength = value.Length;

            if (valueLength == 1)
                return value.ToUpper();
            else
            {
                if (valueLength <= 2 && IsWordUpper(value))
                    return value;
                else
                    return value.Substring(0, 1).ToUpper() + value.Substring(1, valueLength - 1).ToLower();
            }
        }

        private static bool IsWordUpper(string word)
        {
            bool result = true;

            foreach (char c in word)
            {
                if (Char.IsLower(c))
                {
                    result = false;
                    break;
                }
            }

            return result;
        }
    }

    public static class Warp
    {
        public static T ShieldLogSql<T>(Func<T> func)
        {
            try
            {
                Configure.EnableLogSql = false;
                return func.Invoke();
            }
            finally
            {
                Configure.EnableLogSql = true;
            }
            
        }
    }
}
