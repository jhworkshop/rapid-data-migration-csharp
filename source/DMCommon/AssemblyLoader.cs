using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;

namespace JHWork.DataMigration.Common
{
    /// <summary>
    /// 程序集加载器接口，用于识别动态加载程序集
    /// </summary>
    public interface IAssemblyLoader
    {
        /// <summary>
        /// 获取程序集名称，用于识别及显示
        /// </summary>
        /// <returns>程序集名称，不区分大小写</returns>
        string GetName();
    }

    /// <summary>
    /// 程序集信息
    /// </summary>
    internal class AssemblyInfo
    {
        public Assembly Asm { get; set; }       // 程序集
        public string Name { get; set; }        // 匹配名称，小写
        public string DisplayName { get; set; } // 显示名称
    }

    /// <summary>
    /// 泛型（接口）程序集加载器
    /// </summary>
    /// <typeparam name="T">接口泛型</typeparam>
    public class AssemblyLoader<T>
    {
        private readonly Dictionary<string, AssemblyInfo> lst = new Dictionary<string, AssemblyInfo>();

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="basePath">程序集所在文件夹，不区分大小写，是否以路径分隔符结尾均可</param>
        /// <param name="intfName">接口名称</param>
        public AssemblyLoader(string basePath, string intfName)
        {
            if (Directory.Exists(basePath))
            {
                string[] files = Directory.GetFiles(basePath, "*.dll", SearchOption.TopDirectoryOnly);

                foreach (string file in files)
                    if (!BlackList.Contains(file))
                        try
                        {
                            Assembly asm = Assembly.LoadFrom(file);

                            foreach (Type t in asm.ExportedTypes)
                                if (t.GetInterface("IAssemblyLoader") != null && t.GetInterface(intfName) != null)
                                    if (asm.CreateInstance(t.FullName, true) is IAssemblyLoader loader)
                                        lst.Add(loader.GetName().ToLower(),
                                            new AssemblyInfo() {
                                                Asm = asm,
                                                Name = t.FullName,
                                                DisplayName = loader.GetName()
                                            });
                        }
                        catch (Exception ex)
                        {
                            BlackList.Add(file);
                            Logger.WriteLogExcept($"AssemblyLoader({file})", ex);
                        }
            }
        }

        /// <summary>
        /// 按名称获取实例
        /// </summary>
        /// <param name="name">名称</param>
        /// <returns>如果类存在则返回实例，否则返回 null</returns>
        public T GetInstanceByName(string name)
        {
            name = name.ToLower();

            if (lst.ContainsKey(name))
            {
                AssemblyInfo info = lst[name];

                return (T)info.Asm.CreateInstance(info.Name, true);
            }
            else
                return default;

        }

        /// <summary>
        /// 获取名称清单
        /// </summary>
        /// <returns>名称清单</returns>
        public string[] GetInstanceNames()
        {
            List<string> names = new List<string>();

            foreach (AssemblyInfo info in lst.Values)
                names.Add(info.DisplayName);

            return names.ToArray();
        }
    }

    /// <summary>
    /// 黑名单：同一个文件夹可能扫描多次，加载不成功的文件放进黑名单，不再试图加载
    /// </summary>
    internal static class BlackList
    {
        private static readonly List<string> lst = new List<string>();

        public static void Add(string s)
        {
            lst.Add(s);
        }

        public static bool Contains(string s)
        {
            return lst.Contains(s);
        }
    }
}
