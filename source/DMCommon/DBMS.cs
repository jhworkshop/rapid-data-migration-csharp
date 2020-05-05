using System;
using System.Collections.Generic;

namespace JHWork.DataMigration.Common
{
    /// <summary>
    /// Microsoft SQL Server WITH 类型
    /// </summary>
    public enum WithEnums
    {
        None = 0,
        NoLock = 1  // NOLOCK
    }

    /// <summary>
    /// 数据库连接信息
    /// </summary>
    public class Database
    {
        public string DBMS { get; set; }    // 数据库类型
        public string Server { get; set; }  // 服务器
        public uint Port { get; set; }      // 端口
        public string DB { get; set; }      // 数据库
        public string User { get; set; }    // 登录用户
        public string Pwd { get; set; }     // 登录密码
        public string CharSet { get; set; } // 字符集
        public bool Encrypt { get; set; }   // 加密传输
        public bool Compress { get; set; }  // 压缩传输
    }

    /// <summary>
    /// 表信息
    /// </summary>
    public class TableInfo
    {
        public string Name { get; set; }         // 表名
        public string[] KeyFields { get; set; }  // 主键字段
        public int Order { get; set; }           // 排序，从小到大
        public string[] References { get; set; } // 外键引用表
    }

    /// <summary>
    /// 表信息排序对比类
    /// </summary>
    public class TableInfoComparer : IComparer<TableInfo>
    {
        /// <summary>
        /// 从小到大排序比对
        /// </summary>
        /// <param name="x">表信息</param>
        /// <param name="y">表信息</param>
        /// <returns>从小到大排序比对结果</returns>
        public int Compare(TableInfo x, TableInfo y)
        {
            int rst = x.Order - y.Order;

            if (rst == 0)
                return string.Compare(x.Name, y.Name);
            else
                return rst;
        }
    }

    /// <summary>
    /// 进度接口
    /// </summary>
    public interface IProgress
    {
        /// <summary>
        /// 进度更新
        /// </summary>
        /// <param name="total">总量</param>
        /// <param name="progress">进度</param>
        void OnProgress(int total, int progress);
    }

    /// <summary>
    /// 数据库接口基类接口
    /// </summary>
    public interface IDBMSBase
    {
        /// <summary>
        /// 关闭
        /// </summary>
        void Close();

        /// <summary>
        /// 连接数据库
        /// </summary>
        /// <param name="db">数据库连接信息</param>
        /// <returns>连接成功则返回 true，否则返回 false</returns>
        bool Connect(Database db);

        /// <summary>
        /// 获取最后一次出错的错误信息
        /// </summary>
        /// <returns>最后一次出错的错误信息</returns>
        string GetLastError();
    }

    /// <summary>
    /// 数据库配置助手接口
    /// </summary>
    public interface IDBMSAssistant : IDBMSBase
    {
        /// <summary>
        /// 获取表清单
        /// </summary>
        /// <returns>成功则返回 true，并返回表清单，否则返回 false</returns>
        bool GetTables(IProgress progress, List<TableInfo> lst);
    }

    /// <summary>
    /// 数据库读写接口基类
    /// </summary>
    public interface IDBMSRWBase : IDBMSBase
    {
        /// <summary>
        /// 获取字段名称清单
        /// </summary>
        /// <param name="tableName">表名</param>
        /// <param name="fieldNames">字段名称清单</param>
        /// <returns>成功获取则返回 true，否则返回 false</returns>
        bool GetFieldNames(string tableName, out string[] fieldNames);
    }

    /// <summary>
    /// 数据库读接口
    /// </summary>
    public interface IDBMSReader : IDBMSRWBase
    {
        /// <summary>
        /// 统计记录数
        /// </summary>
        /// <param name="table">表配置</param>
        /// <param name="with">WITH 条件</param>
        /// <param name="parms">参数</param>
        /// <param name="count">记录数</param>
        /// <returns>成功统计则返回 true，并返回记录数，否则返回 false</returns>
        bool QueryCount(Table table, WithEnums with, Dictionary<string, object> parms, out ulong count);

        /// <summary>
        /// 分页查询
        /// </summary>
        /// <param name="table">表配置</param>
        /// <param name="fromRow">起始行号</param>
        /// <param name="toRow">结束行号</param>
        /// <param name="with">WITH 条件</param>
        /// <param name="parms">参数</param>
        /// <param name="reader">结果集</param>
        /// <returns>成功查询则返回 true，并返回结果集，否则返回 false</returns>
        bool QueryPage(Table table, uint fromRow, uint toRow, WithEnums with, Dictionary<string, object> parms,
            out IDataWrapper reader);
    }

    /// <summary>
    /// 数据库写接口
    /// </summary>
    public interface IDBMSWriter : IDBMSRWBase
    {
        /// <summary>
        /// 开始事务
        /// </summary>
        /// <returns>成功则返回 true，否则返回 false</returns>
        bool BeginTransaction();

        /// <summary>
        /// 构建数据脚本
        /// </summary>
        /// <param name="table">表配置</param>
        /// <param name="data">数据</param>
        /// <param name="filter">数据过滤器</param>
        /// <param name="script">数据脚本对象</param>
        /// <returns>成功构建则返回 true，并返回数据脚本对象，否则返回 false</returns>
        bool BuildScript(Table table, IDataWrapper data, IDataFilter filter, out object script);

        /// <summary>
        /// 提交事务
        /// </summary>
        /// <returns>成功则返回 true，否则返回 false</returns>
        bool CommitTransaction();

        /// <summary>
        /// 执行数据脚本
        /// </summary>
        /// <param name="table">目标表</param>
        /// <param name="script">数据脚本对象</param>
        /// <param name="count">成功写入的记录行数</param>
        /// <returns>成功则返回 true，否则返回 false</returns>
        bool ExecScript(Table table, object script, out uint count);

        /// <summary>
        /// 查询参数
        /// </summary>
        /// <param name="sql">脚本</param>
        /// <param name="parms">参数清单</param>
        /// <returns>成功查询则返回 true，并返回参数清单，否则返回 false</returns>
        bool QueryParam(string sql, Dictionary<string, object> parms);

        /// <summary>
        /// 回滚事务
        /// </summary>
        /// <returns>成功则返回 true，否则返回 false</returns>
        bool RollbackTransaction();
    }

    /// <summary>
    /// DBMS 工厂类
    /// </summary>
    public class DBMSFactory
    {
        private static readonly string DBMSBasePath = AppDomain.CurrentDomain.BaseDirectory + "DBMS";
        private static readonly AssemblyLoader<IDBMSAssistant> assist = new AssemblyLoader<IDBMSAssistant>(
            DBMSBasePath, "IDBMSAssistant");
        private static readonly AssemblyLoader<IDBMSReader> reader = new AssemblyLoader<IDBMSReader>(
            DBMSBasePath, "IDBMSReader");
        private static readonly AssemblyLoader<IDBMSWriter> writer = new AssemblyLoader<IDBMSWriter>(
            DBMSBasePath, "IDBMSWriter");

        /// <summary>
        /// 获取 IDBMSAssistant 实例接口
        /// </summary>
        /// <param name="name">名称</param>
        /// <returns>IDBMSAssistant 实例</returns>
        public static IDBMSAssistant GetDBMSAssistantByName(string name)
        {
            return assist.GetInstanceByName(name);
        }

        /// <summary>
        /// 获取 IDBMSAssistant 名称清单
        /// </summary>
        /// <returns>名称清单</returns>
        public static string[] GetDBMSAssistantNames()
        {
            return assist.GetInstanceNames();
        }

        /// <summary>
        /// 获取 IDBMSReader 实例接口
        /// </summary>
        /// <param name="name">名称</param>
        /// <returns>IDBMSReader 实例</returns>
        public static IDBMSReader GetDBMSReaderByName(string name)
        {
            return reader.GetInstanceByName(name);
        }

        /// <summary>
        /// 获取 IDBMSReader 名称清单
        /// </summary>
        /// <returns>名称清单</returns>
        public static string[] GetDBMSReaderNames()
        {
            return reader.GetInstanceNames();
        }

        /// <summary>
        /// 获取 IDBMSWriter 实例接口
        /// </summary>
        /// <param name="name">名称</param>
        /// <returns>IDBMSWriter 实例</returns>
        public static IDBMSWriter GetDBMSWriterByName(string name)
        {
            return writer.GetInstanceByName(name);
        }

        /// <summary>
        /// 获取 IDBMSWriter 名称清单
        /// </summary>
        /// <returns>名称清单</returns>
        public static string[] GetDBMSWriterNames()
        {
            return writer.GetInstanceNames();
        }
    }
}
