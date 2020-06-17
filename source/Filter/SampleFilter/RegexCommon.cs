using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace JHWork.DataMigration.Filter.Sample
{
    /// <summary>
    /// 正则表达式公共方法类
    /// </summary>
    public static class RegexCommon
    {
        /// <summary>
        /// 从指定字符串提取二代身份证号，数据类型匹配 IDataFilter.GetValue()
        /// </summary>
        /// <param name="s">待提取字符串</param>
        /// <returns>二代身份证号清单</returns>
        public static string[] ExtractIDNum(object s)
        {
            if (s == DBNull.Value || s == null)
                return new string[] { };
            else
                return ExtractIDNum(s.ToString());
        }

        /// <summary>
        /// 从指定字符串提取二代身份证号
        /// </summary>
        /// <param name="s">待提取字符串</param>
        /// <returns>二代身份证号清单</returns>
        public static string[] ExtractIDNum(string s)
        {
            string pattern = "[0-9xX]+";
            Regex reg = new Regex(pattern);
            MatchCollection lst = reg.Matches(s);
            List<string> rst = new List<string>();

            foreach (Match m in lst)
                if (m.Value.Length == 18) rst.Add(m.Value);

            return rst.ToArray();
        }

        /// <summary>
        /// 从指定字符串提取指定长度数字串，数据类型匹配 IDataFilter.GetValue()
        /// </summary>
        /// <param name="s">待提取字符串</param>
        /// <param name="len">数字串长度</param>
        /// <returns>数字串清单</returns>
        public static string[] ExtractNum(object s, int len)
        {
            if (s == DBNull.Value || s == null)
                return new string[] { };
            else
                return ExtractNum(s.ToString(), len);
        }

        /// <summary>
        /// 从指定字符串提取指定长度数字串
        /// </summary>
        /// <param name="s">待提取字符串</param>
        /// <param name="len">数字串长度</param>
        /// <returns>数字串清单</returns>
        public static string[] ExtractNum(string s, int len)
        {
            string pattern = "[0-9]+";
            Regex reg = new Regex(pattern);
            MatchCollection lst = reg.Matches(s);
            List<string> rst = new List<string>();

            foreach (Match m in lst)
                if (m.Value.Length == len) rst.Add(m.Value);

            return rst.ToArray();
        }
    }
}
