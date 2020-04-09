using System;
using System.Runtime.InteropServices;

namespace DataMigration.Common
{
    /// <summary>
    /// Windows API 声明
    /// </summary>
    public class WinAPI
    {
        /// <summary>
        /// GetTickCount64(), 要求 Windows vista 或更新版本。
        /// </summary>
        /// <returns></returns>
        [DllImport("KERNEL32.DLL", CharSet = CharSet.Unicode, EntryPoint = "GetTickCount64",
            CallingConvention = CallingConvention.StdCall)]
        public static extern ulong GetTickCount();
    }
}
