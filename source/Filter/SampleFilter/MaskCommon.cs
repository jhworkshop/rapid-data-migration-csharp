using System;

namespace JHWork.DataMigration.Filter.Sample
{
    /// <summary>
    /// 脱敏公共方法类
    /// </summary>
    public static class MaskCommon
    {
        /// <summary>
        /// 脱敏指定字符串，脱敏后字符串长度保持不变
        /// </summary>
        /// <param name="s">待脱敏字符串</param>
        /// <param name="prefix">前保留字符数</param>
        /// <param name="suffix">后保留字符数</param>
        /// <returns>脱敏后的字符串</returns>
        private static string Mask(string s, int prefix, int suffix)
        {
            // 达不到长度要求（非合规数据），无需脱敏
            if (string.IsNullOrEmpty(s) || s.Length <= prefix + suffix) return s;

            char[] buf = s.ToCharArray();

            for (int i = prefix; i < buf.Length - suffix; i++)
                buf[i] = '*';

            return new string(buf);
        }

        /// <summary>
        /// 脱敏地址，保留前两位及后四位，数据类型匹配 IDataFilter.GetValue()
        /// </summary>
        /// <param name="addr">地址</param>
        /// <returns>脱敏后的地址</returns>
        public static object MaskAddress(object addr)
        {
            if (addr == DBNull.Value || addr == null)
                return addr;
            else
                return MaskAddress(addr.ToString());
        }

        /// <summary>
        /// 脱敏地址，保留前两位及后四位
        /// </summary>
        /// <param name="addr">地址</param>
        /// <returns>脱敏后的地址</returns>
        public static string MaskAddress(string addr)
        {
            return Mask(addr, 2, 4);
        }

        /// <summary>
        /// 脱敏卡号，保留首位及后四位，数据类型匹配 IDataFilter.GetValue()
        /// </summary>
        /// <param name="card">卡号</param>
        /// <returns>脱敏后的卡号</returns>
        public static object MaskBankCard(object card)
        {
            if (card == DBNull.Value || card == null)
                return card;
            else
                return MaskBankCard(card.ToString());
        }

        /// <summary>
        /// 脱敏卡号，保留首位及后四位
        /// </summary>
        /// <param name="card">卡号</param>
        /// <returns>脱敏后的卡号</returns>
        public static string MaskBankCard(string card)
        {
            return Mask(card, 1, 4);
        }

        /// <summary>
        /// 脱敏邮箱地址，名称段只保留首位，域名段保留全部，数据类型匹配 IDataFilter.GetValue()
        /// </summary>
        /// <param name="email">邮箱地址</param>
        /// <returns>脱敏后的邮箱地址</returns>
        public static object MaskEMail(object email)
        {
            if (email == DBNull.Value || email == null)
                return email;
            else
                return MaskEMail(email.ToString());
        }

        /// <summary>
        /// 脱敏邮箱地址，名称段只保留首位，域名段保留全部
        /// </summary>
        /// <param name="email">邮箱地址</param>
        /// <returns>脱敏后的邮箱地址</returns>
        public static string MaskEMail(string email)
        {
            string[] parts = email.Split('@');

            if (parts.Length == 2)
                return Mask(parts[0], 1, 0) + "@" + parts[1];
            else
                return email;
        }

        /// <summary>
        /// 脱敏证件号，保留首位及后四位，数据类型匹配 IDataFilter.GetValue()
        /// </summary>
        /// <param name="idNum">证件号</param>
        /// <returns>脱敏后的证件号</returns>
        public static object MaskIDNum(object idNum)
        {
            if (idNum == DBNull.Value || idNum == null)
                return idNum;
            else
                return MaskIDNum(idNum.ToString());
        }

        /// <summary>
        /// 脱敏证件号，保留首位及后四位
        /// </summary>
        /// <param name="idNum">证件号</param>
        /// <returns>脱敏后的证件号</returns>
        public static string MaskIDNum(string idNum)
        {
            return Mask(idNum, 1, 4);
        }

        /// <summary>
        /// 脱敏手机号，保留前三位及后四位，数据类型匹配 IDataFilter.GetValue()
        /// </summary>
        /// <param name="mobile">手机号</param>
        /// <returns>脱敏后的手机号</returns>
        public static object MaskMobile(object mobile)
        {
            if (mobile == DBNull.Value || mobile == null)
                return mobile;
            else
                return MaskMobile(mobile.ToString());
        }

        /// <summary>
        /// 脱敏手机号，保留前三位及后四位
        /// </summary>
        /// <param name="mobile">手机号</param>
        /// <returns>脱敏后的手机号</returns>
        public static string MaskMobile(string mobile)
        {
            return Mask(mobile, 3, 4);
        }

        /// <summary>
        /// 脱敏姓名，数据类型匹配 IDataFilter.GetValue()
        /// </summary>
        /// <param name="name">姓名</param>
        /// <returns>脱敏后的姓名</returns>
        public static object MaskName(object name)
        {
            if (name == DBNull.Value || name == null)
                return name;
            else
                return MaskName(name.ToString());
        }

        /// <summary>
        /// 脱敏姓名
        /// </summary>
        /// <param name="name">姓名</param>
        /// <returns>脱敏后的姓名</returns>
        public static string MaskName(string name)
        {
            if (string.IsNullOrEmpty(name) || name.Length == 1)
                return name;
            else if (name.Length == 2)
                return Mask(name, 1, 0);
            else  // Length >= 3，检查是否存在姓名分段
            {
                char sep = '\0';
                string[] parts = name.Split(' ');

                if (parts.Length > 1)
                {
                    sep = ' ';
                    parts[0] = Mask(parts[0], 1, 0);
                }
                else
                {
                    parts = name.Split('·');
                    if (parts.Length > 1)
                    {
                        sep = '·';
                        parts[0] = Mask(parts[0], 1, 0);
                    }
                    else // 不存在姓名分段
                        parts[0] = Mask(parts[0], 1, 1);
                }

                return string.Join(sep.ToString(), parts);
            }
        }
    }
}
