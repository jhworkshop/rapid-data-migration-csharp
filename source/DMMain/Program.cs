using DataMigration.Common;
using System;
using System.Threading;
using System.Windows.Forms;

namespace DataMigration
{
    static class Program
    {
        /// <summary>
        /// 应用程序的主入口点。
        /// </summary>
        [STAThread]
        static void Main()
        {
            Logger.WriteLog("系统", "系统启动");

            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            Application.ThreadException += new ThreadExceptionEventHandler(Application_ThreadException);
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(
                CurrentDomain_UnhandledException);

            Application.Run(new MainForm());
        }

        private static void Application_ThreadException(object sender, ThreadExceptionEventArgs e)
        {
            try
            {
                Logger.WriteLogExcept("系统", e.Exception);
            }
            catch { }
        }

        private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            try
            {
                Exception ex = e.ExceptionObject as Exception;
                Logger.WriteLogExcept("系统", ex);
            }
            catch { }
        }
    }
}
