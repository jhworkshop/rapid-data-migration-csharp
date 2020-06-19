using System;
using System.Collections.Generic;

namespace JHWork.DataMigration.Common
{
    /// <summary>
    /// 数据库连接信息
    /// </summary>
    public class Database
    {
        public string DBMS { get; set; }    // 数据库类型
        public string Server { get; set; }  // 服务器
        public uint Port { get; set; }      // 端口
        public string DB { get; set; }      // 数据库
        public string Schema { get; set; }  // 模式
        public string User { get; set; }    // 登录用户
        public string Pwd { get; set; }     // 登录密码
        public string CharSet { get; set; } // 字符集
        public bool Encrypt { get; set; }   // 加密传输
        public bool Compress { get; set; }  // 压缩传输
        public uint Timeout { get; set; }   // 超时秒数

        public static string AnalyseDB(string db)
        {
            if (string.IsNullOrEmpty(db)) return "";

            return db.Split(new char[] { '.' }, StringSplitOptions.RemoveEmptyEntries)[0];
        }

        public static string AnalyseSchema(string db)
        {
            if (string.IsNullOrEmpty(db)) return "";

            string[] ss = db.Split(new char[] { '.' }, StringSplitOptions.RemoveEmptyEntries);

            if (ss.Length > 1)
                return ss[1];
            else
                return "";
        }

        public void Duplicate(Database source)
        {
            DBMS = source.DBMS;
            Server = source.Server;
            Port = source.Port;
            DB = source.DB;
            Schema = source.Schema;
            User = source.User;
            Pwd = source.Pwd;
            CharSet = source.CharSet;
            Encrypt = source.Encrypt;
            Compress = source.Compress;
            Timeout = source.Timeout;
        }
    }

    /// <summary>
    /// DBMS 基类，封装一些公共方法
    /// </summary>
    public abstract class DBMSBase
    {
        public string LastError { get; protected set; }
        protected string LogTitle { get; set; }
        protected string Schema { get; set; }
        protected uint Timeout { get; set; }

        /// <summary>
        /// 排除字段
        /// </summary>
        /// <param name="fields">源字段清单</param>
        /// <param name="skipFields">排除字段清单</param>
        /// <param name="skipFields2">排除字段清单2</param>
        /// <returns>字段字段</returns>
        protected string[] ExcludeFields(string[] fields, string[] skipFields, string[] skipFields2 = null)
        {
            List<string> skipList = new List<string>();

            if (skipFields != null && skipFields.Length != 0)
                foreach (string s in skipFields)
                    skipList.Add(s.ToLower());

            if (skipFields2 != null && skipFields2.Length != 0)
                foreach (string s in skipFields2)
                    skipList.Add(s.ToLower());

            if (skipList.Count == 0)
                return fields;
            else
            {
                List<string> lst = new List<string>();

                foreach (string s in fields)
                    if (!skipList.Contains(s.ToLower()))
                        lst.Add(s);

                return lst.ToArray();
            }
        }

        /// <summary>
        /// 获取表主键字段清单
        /// </summary>
        /// <param name="table">表名</param>
        /// <param name="schema">模式</param>
        /// <returns>主键字段清单</returns>
        protected abstract string[] GetTableKeys(string table, string schema);

        /// <summary>
        /// 获取表外键指向表清单
        /// </summary>
        /// <param name="table">表名</param>
        /// <param name="schema">模式</param>
        /// <returns>外键指向表清单</returns>
        protected abstract string[] GetTableRefs(string table, string schema);

        /// <summary>
        /// 获取表清单
        /// </summary>
        /// <returns>表清单</returns>
        protected abstract string[] GetTables();

        /// <summary>
        /// 返回首列值清单
        /// </summary>
        /// <param name="data">数据</param>
        /// <returns>首列值清单</returns>
        protected string[] GetValues(IDataWrapper data)
        {
            try
            {
                List<string> lst = new List<string>();

                while (data.Read())
                {
                    object obj = data.GetValue(0);

                    if (obj == DBNull.Value || obj == null)
                        lst.Add(null);
                    else
                        lst.Add(obj.ToString());
                }

                return lst.ToArray();
            }
            finally
            {
                data.Close();
            }
        }

        public bool GetTables(IProgress progress, List<TableInfo> lst)
        {
            List<TableFK> fks = new List<TableFK>();
            int total = 0, position = 0;

            // 获取所有用户表清单
            string[] tables = GetTables();

            foreach (string table in tables)
            {
                string[] parts = table.Split('.');

                fks.Add(new TableFK()
                {
                    Name = parts.Length > 1 ? parts[1] : parts[0],
                    Schema = parts.Length > 1 ? parts[0] : "",
                    Order = 0
                });
            }
            total = tables.Length * 2;

            // 获取每个表的主键字段清单
            foreach (TableFK fk in fks)
            {
                fk.KeyFields = GetTableKeys(fk.Name, fk.Schema);
                progress.OnProgress(total, ++position);
            }

            // 获取每个表的外键指向的表清单
            foreach (TableFK fk in fks)
            {
                fk.FKs.AddRange(GetTableRefs(fk.Name, fk.Schema));
                progress.OnProgress(total, ++position);
            }

            int order = 100;

            foreach (TableFK fk in fks)
                if (fk.FKs.Count == 0 || (fk.FKs.Count == 1 && fk.FKs[0].Equals(fk.Name)))
                    fk.Order = order;

            order += 100;
            while (order <= 10000) // 设定一个级别上限：100 级
            {
                int left = 0;
                List<TableFK> lastList = new List<TableFK>();

                // 创建上一轮次的结果清单
                foreach (TableFK fk in fks)
                    if (fk.Order > 0) lastList.Add(fk);

                foreach (TableFK fk in fks)
                    if (fk.Order == 0)
                    {
                        bool done = true;

                        // 检查是否所有外键指向表都在上一轮清单里面
                        foreach (string s in fk.FKs)
                        {
                            bool found = false;

                            foreach (TableFK fk2 in lastList)
                                if (fk2.Name.Equals(s))
                                {
                                    found = true;
                                    break;
                                }

                            if (!found)
                            {
                                done = false;
                                break;
                            }
                        }
                        if (done)
                            fk.Order = order;
                        else
                            left++;
                    }

                if (left == 0) break;
                order += 100;
            }

            foreach (TableFK fk in fks)
                lst.Add(new TableInfo()
                {
                    Name = fk.Name,
                    KeyFields = fk.KeyFields,
                    Order = fk.Order,
                    References = fk.FKs.ToArray()
                });

            lst.Sort(new TableInfoComparer());

            return true;
        }

        /// <summary>
        /// 判断候选项是否全部无内容
        /// </summary>
        /// <param name="item">非空内容</param>
        /// <param name="items">候选项</param>
        /// <returns>全部无内容则返回 true，否则返回 false，并通过 item 返回第一项非空白项目</returns>
        protected bool IsEmpty(out string item, params string[] items)
        {
            foreach (string s in items)
                if (!string.IsNullOrEmpty(s))
                {
                    item = s;
                    return false;
                }

            item = "";
            return true;
        }
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

    /// <summary>
    /// 数据库参数
    /// </summary>
    public class DBMSParams
    {
        public bool Server { get; set; } = true;
        public bool Port { get; set; } = true;
        public bool DB { get; set; } = true;
        public bool Schema { get; set; } = true;
        public bool User { get; set; } = true;
        public bool Pwd { get; set; } = true;
        public bool CharSet { get; set; } = true;
        public bool Encrypt { get; set; } = true;
        public bool Compress { get; set; } = true;
        public bool Timeout { get; set; } = true;
    }

    /// <summary>
    /// 数据库接口基类接口
    /// </summary>
    public interface IDBMSBase
    {
        /// <summary>
        /// 获取最后一次出错的错误信息
        /// </summary>
        string LastError { get; }

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
    }

    /// <summary>
    /// 数据库配置助手接口
    /// </summary>
    public interface IDBMSAssistant : IDBMSBase
    {
        /// <summary>
        /// 获取数据库参数
        /// </summary>
        /// <returns>数据库参数对象</returns>
        DBMSParams GetParams();

        /// <summary>
        /// 获取表清单
        /// </summary>
        /// <param name="progress">进度接口</param>
        /// <param name="lst">表清单返回数据</param>
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
        /// <param name="schema">模式</param>
        /// <param name="fieldNames">字段名称清单</param>
        /// <returns>成功获取则返回 true，否则返回 false</returns>
        bool GetFieldNames(string tableName, string schema, out string[] fieldNames);
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
    /// 表外键信息
    /// </summary>
    public class TableFK : TableInfo
    {
        public List<string> FKs { get; } = new List<string>(); // 外键指向表
    }

    /// <summary>
    /// 表信息
    /// </summary>
    public class TableInfo
    {
        public string Name { get; set; }         // 表名
        public string Schema { get; set; }       // 模式
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
    /// Microsoft SQL Server WITH 类型
    /// </summary>
    public enum WithEnums
    {
        None = 0,
        NoLock = 1  // NOLOCK
    }
}
