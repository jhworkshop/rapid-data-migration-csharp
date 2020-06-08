using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;

namespace JHWork.DataMigration.Common
{
    /// <summary>
    /// 数据状态，用于标记执行的状态。
    /// </summary>
    public enum DataStates
    {
        Idle = 0,           // 空闲
        Running = 1,        // 执行中
        Done = 0x10,        // 完成
        Error = 0x20,       // 错误
        RunningError = 0x21 // 执行中有错误
    }

    /// <summary>
    /// 写入模式
    /// </summary>
    public enum WriteModes
    {
        Append, // 追加模式
        Update  // 更新模式
    }

    /// <summary>
    /// 实例，多个实例并行执行。
    /// </summary>
    public class Instance
    {
        public string Name { get; set; }  // 名称
        public Task[] Tasks { get; set; } // 任务
    }

    /// <summary>
    /// 数据表配置
    /// </summary>
    public class Table
    {
        public string SourceName { get; set; }      // 源表名
        public string DestName { get; set; }        // 目标表名
        public int Order { get; set; }              // 排序，从小到大
        public uint PageSize { get; set; }          // 每批次记录数
        public string OrderSQL { get; set; }        // 排序脚本
        public string WhereSQL { get; set; }        // 条件脚本
        public WriteModes WriteMode { get; set; }   // 写入模式
        public string[] KeyFields { get; set; }     // 主键字段
        public string[] SkipFields { get; set; }    // 跳过字段
        public string Filter { get; set; }          // 过滤器
        public bool KeepIdentity { get; set; }      // 保留自增值，MSSQL 专用
        public bool UseReplace { get; set; }        // 使用 REPLACE 语句更新，MySQL 专用
        public string[] SourceFields { get; set; }  // 源字段
        public string[] DestFields { get; set; }    // 目标字段
        public string[] References { get; set; }    // 外键表
        public ulong Progress { get; set; }         // 进度记录数
        public ulong Total { get; set; }            // 总记录数
    }

    /// <summary>
    /// 数据表排序对比类
    /// </summary>
    public class TableComparer : IComparer<Table>
    {
        /// <summary>
        /// 从小到大排序比对
        /// </summary>
        /// <param name="x">数据表</param>
        /// <param name="y">数据表</param>
        /// <returns>从小到大排序比对结果</returns>
        public int Compare(Table x, Table y)
        {
            int rst = x.Order - y.Order;

            if (rst == 0)
            {
                ulong weightX = x.Total * (uint)x.DestFields.Length, weightY = y.Total * (uint)y.DestFields.Length;

                if (weightX > weightY)
                    return -1;
                else if (weightX < weightY)
                    return 1;
                else
                    return string.Compare(x.DestName, y.DestName);
            }
            else
                return rst;
        }
    }

    /// <summary>
    /// 任务，同一个实例的任务串行执行。
    /// </summary>
    public class Task
    {
        public string Name { get; set; }       // 名称  
        public ulong Progress { get; set; }    // 进度记录数
        public ulong Total { get; set; }       // 总记录数
        public DataStates Status { get; set; } // 状态
        public ulong StartTick { get; set; }   // 开始时间
        public string ErrorMsg { get; set; }   // 错误信息
    }

    /// <summary>
    /// 脱敏实例
    /// 
    /// MaskingRunner 的实例结构如下：
    ///
    /// MaskingInstance
    ///   + MaskingTask[] （串行）
    ///       + MaskingTable[] （并行）
    ///
    /// 即，每个实例包含一组串行任务，每任务包含一组并行迁移表。
    /// </summary>
    public class MaskingInstance : Instance { }

    /// <summary>
    /// 脱敏表
    /// </summary>
    public class MaskingTable : Table
    {
        public string[] MaskFields { get; set; } // 脱敏字段
        public DataStates Status { get; set; }   // 状态
    }

    /// <summary>
    /// 脱敏任务
    /// </summary>
    public class MaskingTask : Task
    {
        public Database Source = new Database();   // 源库
        public string Params { get; set; }         // 参数脚本
        public MaskingTable[] Tables { get; set; } // 表
        public uint ReadPages { get; set; }        // 每次读取数据批次数
        public uint Threads { get; set; }          // 并行迁移表数
    }

    /// <summary>
    /// 汇集实例
    /// 
    /// IntegrationRunner 的实例结构如下：
    /// 
    /// IntegrationInstance
    ///   + IntegrationTask[][] (并行)
    ///       + IntegrationTable
    ///
    /// 即，每个实例包含一组串行任务（二维数组结构用于解决表之间的依赖关系），每个任务可包含一组并行汇集表
    /// </summary>
    public class IntegrationInstance : Instance
    {
        public IntegrationTask[][] ActualTasks { get; set; } // 任务清单
        public uint Threads { get; set; }                    // 并行表数
    }

    /// <summary>
    /// 汇集表
    /// </summary>
    public class IntegrationTable : Table { }

    /// <summary>
    /// 汇集任务。
    /// </summary>
    public class IntegrationTask : Task
    {
        public Database[] Sources { get; set; }     // 源库
        public Database Dest { get; set; }          // 目标库
        public string Params { get; set; }          // 参数脚本
        public IntegrationTable Table { get; set; } // 表
        public uint ReadPages { get; set; }         // 每次读取数据批次数
    }

    /// <summary>
    /// 迁移实例
    /// 
    /// MigrationRunner 的实例结构如下：
    ///
    /// MigrationInstance
    ///   + MigrationTask[] （串行）
    ///       + MigrationTable[][] （并行）
    ///
    /// 即，每个实例包含一组串行任务，每任务包含一组并行迁移表（二维数组结构用于解决表之间的依赖关系）。
    /// </summary>
    public class MigrationInstance : Instance { }

    /// <summary>
    /// 迁移表
    /// </summary>
    public class MigrationTable : Table
    {
        public DataStates Status { get; set; } // 状态
    }

    /// <summary>
    /// 迁移任务，每个任务对应一组库。
    /// </summary>
    public class MigrationTask : Task
    {
        public Database Source { get; } = new Database(); // 源库
        public Database Dest { get; } = new Database();   // 目标库
        public string Params { get; set; }                // 参数脚本
        public MigrationTable[][] Tables { get; set; }    // 表
        public uint ReadPages { get; set; }               // 每次读取数据批次数
        public uint Threads { get; set; }                 // 并行迁移表数
    }

    /// <summary>
    /// 执行器分析器接口
    /// </summary>
    public interface IRunnerAnalyzer
    {
        /// <summary>
        /// 解析实例配置
        /// </summary>
        /// <param name="objs">实例配置</param>
        /// <param name="inherited">继承项</param>
        /// <param name="path">文件路径</param>
        /// <returns>实例清单</returns>
        Instance[] AnalyseInstance(JArray objs, JObject inherited, string path);

        /// <summary>
        /// 重置实例状态
        /// </summary>
        /// <param name="ins">实例</param>
        void Reset(Instance ins);
    }

    /// <summary>
    /// 执行器助理接口
    /// </summary>
    public interface IRunnerAssistant
    {
        /// <summary>
        /// 加载示例配置
        /// </summary>
        /// <param name="ins">实例</param>
        /// <param name="source">源</param>
        /// <param name="dest">目标</param>
        /// <param name="tables">表清单</param>
        /// <param name="param">参数</param>
        void LoadSample(Instance ins, Database source, Database dest, List<Table> tables, out string param);

        /// <summary>
        /// 保存示例配置
        /// </summary>
        /// <param name="source">源</param>
        /// <param name="dest">目标</param>
        /// <param name="tables">表清单</param>
        /// <param name="param">参数</param>
        /// <param name="path">路径</param>
        /// <param name="file">文件名</param>
        void SaveSample(Database source, Database dest, List<Table> tables, string param, string path, string file);
    }

    /// <summary>
    /// 执行器执行接口
    /// </summary>
    public interface IRunnerExecutor
    {
        /// <summary>
        /// 执行
        /// </summary>
        /// <param name="ins">实例</param>
        /// <param name="status">停止状态接口</param>
        void Execute(Instance ins, IStopStatus status);

        /// <summary>
        /// 执行预读取
        /// </summary>
        /// <param name="ins">实例</param>
        /// <param name="status">停止状态接口</param>
        void Prefetch(Instance ins, IStopStatus status);
    }

    /// <summary>
    /// 停止状态接口
    /// </summary>
    public interface IStopStatus
    {
        bool Stopped { get; }
    }

    /// <summary>
    /// 执行器工厂
    /// </summary>
    public class RunnerFactory
    {
        private static readonly string RunnerBasePath = AppDomain.CurrentDomain.BaseDirectory + "Runner";
        private static readonly AssemblyLoader<IRunnerAnalyzer> analyzer = new AssemblyLoader<IRunnerAnalyzer>(
            RunnerBasePath, "IRunnerAnalyzer");
        private static readonly AssemblyLoader<IRunnerAssistant> assistant = new AssemblyLoader<IRunnerAssistant>(
            RunnerBasePath, "IRunnerAssistant");
        private static readonly AssemblyLoader<IRunnerExecutor> executor = new AssemblyLoader<IRunnerExecutor>(
            RunnerBasePath, "IRunnerExecutor");

        /// <summary>
        /// 按名称获取执行器分析器
        /// </summary>
        /// <param name="name">名称</param>
        /// <returns>执行器分析器实例</returns>
        public static IRunnerAnalyzer GetRunnerAnalyzerByName(string name)
        {
            return analyzer.GetInstanceByName(name);
        }

        /// <summary>
        /// 按名称获取执行器助理
        /// </summary>
        /// <param name="name">名称</param>
        /// <returns>执行器助理实例</returns>
        public static IRunnerAssistant GetRunnerAssistantByName(string name)
        {
            return assistant.GetInstanceByName(name);
        }

        /// <summary>
        /// 获取执行器助理名称清单
        /// </summary>
        /// <returns>执行器助理名称清单</returns>
        public static string[] GetRunnerAssistantNames()
        {
            return assistant.GetInstanceNames();
        }

        /// <summary>
        /// 按名称获取执行器执行实例
        /// </summary>
        /// <param name="name">名称</param>
        /// <returns>执行器执行实例</returns>
        public static IRunnerExecutor GetRunnerExecutorByName(string name)
        {
            return executor.GetInstanceByName(name);
        }
    }
}
