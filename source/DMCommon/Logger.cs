using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Windows.Forms;

namespace JHWork.DataMigration.Common
{
    /// <summary>
    /// 异步日志类
    /// </summary>
    public class Logger
    {
        private static readonly List<StringBuilder> bufLog = new List<StringBuilder>();
        private static readonly List<StringBuilder> bufRpt = new List<StringBuilder>();
        private static readonly object lockLog = new object();
        private static readonly object lockRpt = new object();
        private static string rptFile = "";
        private static readonly string rptPath = AppDomain.CurrentDomain.BaseDirectory + "Report\\";
        private static readonly string logPath = AppDomain.CurrentDomain.BaseDirectory + "Log\\";
        private static bool StopFlag { get; set; } = false;

        static Logger()
        {
            if (!Directory.Exists(logPath)) Directory.CreateDirectory(logPath);
            if (!Directory.Exists(rptPath)) Directory.CreateDirectory(rptPath);
            Application.ApplicationExit += ApplicationExit;

            new Thread(WriteData).Start();
        }

        private static void ApplicationExit(object sender, EventArgs e)
        {
            StopFlag = true;
        }

        /// <summary>
        /// 设定报表文件名
        /// </summary>
        /// <param name="rpt">报表文件名，不包含路径</param>
        public static void SetRptFile(string rpt)
        {
            rptFile = rptPath + rpt;
            WriteRptInternal("时间", "服务器", "数据库", "表", "状态", "记录数/失败原因");
        }

        private static void Sleep(uint ms)
        {
            ulong tick = WinAPI.GetTickCount();

            while (WinAPI.GetTickCount() - tick < ms && !StopFlag)
                Thread.Sleep(1);
        }

        private static void WriteData()
        {
            while (!StopFlag)
            {
                Sleep(500);

                // 写报表
                if (bufRpt.Count > 0)
                {
                    List<StringBuilder> sb = new List<StringBuilder>();

                    lock (lockRpt)
                    {
                        sb.AddRange(bufRpt);
                        bufRpt.Clear();
                    }

                    WriteFile(rptFile, sb, Encoding.Default);
                }

                // 写日志
                if (bufLog.Count > 0)
                {
                    List<StringBuilder> sb = new List<StringBuilder>();

                    lock (lockLog)
                    {
                        sb.AddRange(bufLog);
                        bufLog.Clear();
                    }

                    WriteFile(logPath + DateTime.Now.ToString("yyMMdd") + ".log", sb, Encoding.Default);
                }
            }
        }

        private static void WriteFile(string file, List<StringBuilder> content, Encoding encoding)
        {
            try
            {
                using (FileStream fs = new FileStream(file, FileMode.Create | FileMode.Append))
                {
                    using (StreamWriter writer = new StreamWriter(fs, encoding))
                    {
                        writer.BaseStream.Seek(0, SeekOrigin.End);
                        foreach (StringBuilder sb in content)
                            writer.WriteLine(sb.ToString());
                        writer.Flush();
                    }
                }
            }
            catch (Exception) { }
        }

        /// <summary>
        /// 输出日志
        /// </summary>
        /// <param name="title">标题</param>
        /// <param name="content">内容</param>
        public static void WriteLog(string title, string content)
        {
            StringBuilder sb = new StringBuilder(DateTime.Now.ToString("HH:mm:ss.fff"))
                .Append(" [").Append(title).Append("] - ").Append(content);

            lock (lockLog)
            {
                bufLog.Add(sb);
            }
        }

        /// <summary>
        /// 记录异常日志
        /// </summary>
        /// <param name="title">标题</param>
        /// <param name="ex">异常</param>
        public static void WriteLogExcept(string title, Exception ex)
        {
            WriteLog(title, $"{ex.Message}\n{ex.StackTrace}");
        }

        /// <summary>
        /// 输出报表，CSV 格式
        /// </summary>
        /// <param name="server">服务器</param>
        /// <param name="db">数据库</param>
        /// <param name="table">表</param>
        /// <param name="state">状态</param>
        /// <param name="reason">记录数/失败原因</param>
        public static void WriteRpt(string server, string db, string table, string state, string reason)
        {
            WriteRptInternal(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"), server, db, table, state, reason);
        }

        private static void WriteRptInternal(string time, string server, string db, string table, string state,
            string reason)
        {
            StringBuilder sb = new StringBuilder().Append(time).Append(",").Append(server)
                .Append(",").Append(db).Append(",").Append(table).Append(",").Append(state)
                .Append(",");

            if (reason.IndexOf(',') >= 0)
                sb.Append("\"").Append(reason.Replace("\"", "\"\"")).Append("\"");
            else
                sb.Append(reason);

            lock (lockRpt)
            {
                bufRpt.Add(sb);
            }
        }
    }
}
