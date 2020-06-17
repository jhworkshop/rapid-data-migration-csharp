using System;
using System.Text;

namespace JHWork.DataMigration.Filter.Sample
{
    /// <summary>
    /// 数据转换公共方法类
    /// </summary>
    public static class DataCommon
    {
        /// <summary>
        /// 获取指定字符串转换成指定编码的字节数，数据类型匹配 IDataFilter.GetValue()
        /// </summary>
        /// <param name="s">字符串</param>
        /// <param name="encoding">编码</param>
        /// <returns>字节数</returns>
        public static int GetLength(object s, Encoding encoding)
        {
            if (s == DBNull.Value || s == null)
                return 0;
            else
                return GetLength(s.ToString(), encoding);
        }

        /// <summary>
        /// 获取指定字符串转换成指定编码的字节数
        /// </summary>
        /// <param name="s">字符串</param>
        /// <param name="encoding">编码</param>
        /// <returns>字节数</returns>
        public static int GetLength(string s, Encoding encoding)
        {
            if (string.IsNullOrEmpty(s))
                return 0;
            else
                return encoding.GetByteCount(s);
        }

        /// <summary>
        /// 指定字符编码及长度截取内容，数据类型匹配 IDataFilter.GetValue()
        /// </summary>
        /// <param name="s">待截取内容</param>
        /// <param name="encoding">字符编码</param>
        /// <param name="maxLen">字节数</param>
        /// <returns>截取的内容</returns>
        public static object SubString(object s, Encoding encoding, int maxLen)
        {
            if (s == DBNull.Value || s == null)
                return s;
            else
                return SubString(s.ToString(), encoding, maxLen);
        }

        /// <summary>
        /// 指定字符编码及长度截取内容
        /// </summary>
        /// <param name="s">待截取内容</param>
        /// <param name="encoding">字符编码</param>
        /// <param name="maxLen">字节数</param>
        /// <returns>截取的内容</returns>
        public static string SubString(string s, Encoding encoding, int maxLen)
        {
            int count = GetLength(s, encoding);

            if (count <= maxLen) return s;

            StringBuilder sb = new StringBuilder();

            count = 0;
            foreach (char c in s)
            {
                count += encoding.GetByteCount(new char[] { c });
                if (count >= maxLen) break;
                sb.Append(c);
            }

            return sb.ToString();
        }
    }
}
