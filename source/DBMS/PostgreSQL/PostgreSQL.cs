using DataMigration.Common;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace DataMigration.DBMS.PostgreSQL
{
    /// <summary>
    /// 基于合并算法的更新数据脚本对象
    /// </summary>
    internal class MergeScript
    {
        public string TableName { get; set; }  // 临表名称
        public string PrepareSQL { get; set; } // 准备 SQL（创建临表）
        public DataTable Data { get; set; }    // 数据
        public string UpdateSQL { get; set; }  // 更新 SQL
        public string InsertSQL { get; set; }  // 插入 SQL
        public string CleanSQL { get; set; }   // 清理 SQL
    }

    /// <summary>
    /// 基于更新和插入 SQL 的更新数据脚本对象
    /// </summary>
    internal class UpdateScript
    {
        public string UpdateSQL { get; set; } // 更新 SQL
        public string InsertSQL { get; set; } // 插入 SQL
    }

    /// <summary>
    /// 表外键信息
    /// </summary>
    internal class TableFK : TableInfo
    {
        public List<string> FKs { get; } = new List<string>(); // 外键指向表
    }

    /// <summary>
    /// PostgreSQL
    /// </summary>
    public class PostgreSQL : IDBMSAssistant, IDBMSReader, IDBMSWriter, IAssemblyLoader
    {
        private readonly NpgsqlConnection conn = new NpgsqlConnection();
        private NpgsqlTransaction trans = null;
        private string errMsg = "";
        private string title = "PostgreSQL";


        public bool BeginTransaction()
        {
            try
            {
                trans = conn.BeginTransaction();

                return true;
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);

                return false;
            }
        }

        public bool BuildScript(Table table, IDataWrapper data, IDataFilter filter, ref object script)
        {
            // 有数据
            if (data.Read())
            {
                if (table.WriteMode == WriteModes.Append)
                    BuildScriptWithDataTable(table, data, filter, ref script);
                //BuildScriptWithInsertSQL(table, data, filter, ref script);
                else
                    BuildScriptWithMergeSQL(table, data, filter, ref script);
                //BuildScriptWithUpdateSQL(table, data, filter, ref script);

                return true;
            }

            return false;
        }

        protected void BuildScriptWithDataTable(Table table, IDataWrapper data, IDataFilter filter, ref object script)
        {
            // 创建数据表，字段清单与目标表须一致
            DataTable dt = new DataTable();

            for (int i = 0; i < table.DestFields.Length; i++)
                dt.Columns.Add(table.DestFields[i], data.GetFieldType(i));

            // 给跳过字段名称增加一个前缀，这样字段映射后取值就为 NULL
            string[] fields = ModifyFields(table.DestFields, table.SkipFields, "!!!");

            data.MapFields(fields);

            // 添加第一笔记录，仍然使用正确的字段名
            DataRow row = dt.NewRow();

            for (int i = 0; i < table.DestFields.Length; i++)
                row[i] = filter.GetValue(data, i, table.DestFields[i]);

            dt.Rows.Add(row);

            // 添加后续记录，仍然使用正确的字段名
            int r = 1;
            while (r < table.PageSize && data.Read())
            {
                row = dt.NewRow();

                for (int i = 0; i < table.DestFields.Length; i++)
                    row[i] = filter.GetValue(data, i, table.DestFields[i]);

                dt.Rows.Add(row);
                r++;
            }

            script = dt;
        }

        [Obsolete("此模式执行效率较低，请用 BuildScriptWithDataTable() 替代")]
        protected void BuildScriptWithInsertSQL(Table table, IDataWrapper data, IDataFilter filter, ref object script)
        {
            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(table.DestFields, table.SkipFields);

            data.MapFields(fields);

            sb.Append("insert into ").Append(ProcessTableName(table.DestName)).Append(" (")
                .Append(ProcessFieldNames(fields)).Append(")").AppendLine().Append("values").AppendLine()
                .Append("(").Append(GetFmtValue(filter.GetValue(data, 0, fields[0])));
            for (int i = 1; i < fields.Length; i++)
                sb.Append(", ").Append(GetFmtValue(filter.GetValue(data, i, fields[i])));
            sb.Append(")");

            int r = 1;
            while (r < table.PageSize && data.Read())
            {
                r++;
                sb.Append(",").AppendLine().Append("(")
                    .Append(GetFmtValue(filter.GetValue(data, 0, fields[0])));
                for (int i = 1; i < fields.Length; i++)
                    sb.Append(", ").Append(GetFmtValue(filter.GetValue(data, i, fields[i])));
                sb.Append(")");
            }

            script = sb.ToString();
        }

        protected void BuildScriptWithMergeSQL(Table table, IDataWrapper data, IDataFilter filter, ref object script)
        {
            string destTable = ProcessTableName(table.DestName);
            string tmpTable = $"\"{destTable.Substring(1, destTable.Length - 2)}_{Guid.NewGuid():N}\"";
            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(ExcludeFields(table.DestFields, table.KeyFields), table.SkipFields);
            string field = ProcessFieldName(fields[0]);

            sb.Append("update ").Append(destTable).Append(" A set ").Append(field).Append(" = B.").Append(field);
            for (int i = 1; i < fields.Length; i++)
            {
                field = ProcessFieldName(fields[i]);
                sb.Append(", ").Append(field).Append(" = B.").Append(field);
            }
            field = ProcessFieldName(table.KeyFields[0]);
            sb.Append(" from ").Append(tmpTable).Append(" B where ")
                .Append("A.").Append(field).Append(" = ").Append("B.").Append(field);
            for (int i = 1; i < table.KeyFields.Length; i++)
            {
                field = ProcessFieldName(table.KeyFields[i]);
                sb.Append(" and A.").Append(field).Append(" = ").Append("B.").Append(field);
            }

            string updateSQL = sb.ToString();

            fields = ExcludeFields(table.DestFields, table.SkipFields);
            sb.Length = 0;
            field = ProcessFieldName(table.KeyFields[0]);
            sb.Append("insert into ").Append(destTable).Append(" (").Append(ProcessFieldNames(fields))
                .Append(") select ").Append(ProcessFieldNames(fields, "A")).Append(" from ").Append(tmpTable)
                .Append(" A left join ").Append(destTable).Append(" B on A.").Append(field).Append(" = B.")
                .Append(field);
            for (int i = 1; i < table.KeyFields.Length; i++)
            {
                field = ProcessFieldName(table.KeyFields[i]);
                sb.Append(" and A.").Append(field).Append(" = B.").Append(field);
            }
            sb.Append(" where B.").Append(field).Append(" is null");

            BuildScriptWithDataTable(table, data, filter, ref script);

            script = new MergeScript()
            {
                TableName = tmpTable.Substring(1, tmpTable.Length - 2),
                PrepareSQL = $"select * into {tmpTable} from {destTable} where 1 = 0",
                Data = (DataTable)script,
                UpdateSQL = updateSQL,
                InsertSQL = sb.ToString(),
                CleanSQL = $"drop table {tmpTable}"
            };
        }

        [Obsolete("此模式执行效率较低，请用 BuildScriptWithMergeSQL() 替代")]
        protected void BuildScriptWithUpdateSQL(Table table, IDataWrapper data, IDataFilter filter, ref object script)
        {
            UpdateScript rst = new UpdateScript();
            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(table.DestFields, table.SkipFields);

            data.MapFields(fields);
            sb.Append("update ").Append(ProcessTableName(table.DestName)).Append(" set ")
                .Append(ProcessFieldName(fields[0])).Append(" = ")
                .Append(GetFmtValue(filter.GetValue(data, 0, fields[0])));
            for (int i = 1; i < fields.Length; i++)
                sb.Append(", ").Append(ProcessFieldName(fields[i])).Append(" = ")
                    .Append(GetFmtValue(filter.GetValue(data, i, fields[i])));

            data.MapFields(table.KeyFields);
            sb.Append(" where ")
                .Append(ProcessFieldName(table.KeyFields[0])).Append(" = ")
                .Append(GetFmtValue(filter.GetValue(data, 0, table.KeyFields[0])));
            for (int i = 1; i < table.KeyFields.Length; i++)
                sb.Append(" and ").Append(ProcessFieldName(table.KeyFields[i])).Append(" = ")
                    .Append(GetFmtValue(filter.GetValue(data, i, table.KeyFields[i])));

            rst.UpdateSQL = sb.ToString();

            fields = ExcludeFields(table.DestFields, table.SkipFields);
            data.MapFields(fields);
            sb.Length = 0;
            sb.Append("insert into ").Append(ProcessTableName(table.DestName)).Append(" (")
                .Append(ProcessFieldNames(fields)).Append(") values (")
                .Append(GetFmtValue(filter.GetValue(data, 0, fields[0])));
            for (int i = 1; i < fields.Length; i++)
                sb.Append(", ").Append(GetFmtValue(filter.GetValue(data, i, fields[i])));
            sb.Append(")");

            rst.InsertSQL = sb.ToString();

            script = rst;
        }

        private string BytesToStr(byte[] bytes)
        {
            StringBuilder sb = new StringBuilder("E'\\\\x");

            foreach (byte b in bytes)
                sb.Append(b.ToString("X2"));

            return sb.Append('\'').ToString();
        }

        public void Close()
        {
            try
            {
                conn.Close();
            }
            catch { }
        }

        public bool CommitTransaction()
        {
            try
            {
                trans.Commit();

                return true;
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);

                return false;
            }
            finally
            {
                trans = null;
            }
        }

        public bool Connect(Database db)
        {
            string encrypt = db.Encrypt ? "Require" : "Disable";

            title = $"{db.Server}/{db.DB}";

            try
            {
                conn.Close();
                conn.ConnectionString = $"Server={db.Server};Port={db.Port};Database={db.DB};Userid={db.User}"
                    + $";Password={db.Pwd};Pooling=false;Persist Security Info=True;SslMode={encrypt}";
                conn.Open();

                return true;
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);

                return false;
            }
        }

        private string[] ExcludeFields(string[] fields, string[] skipFields)
        {
            if (skipFields == null || skipFields.Length == 0)
                return fields;
            else
            {
                List<string> lst = new List<string>(), skipList = new List<string>();

                foreach (string s in skipFields)
                    skipList.Add(s.ToLower());

                foreach (string s in fields)
                    if (!skipList.Contains(s.ToLower()))
                        lst.Add(s);

                return lst.ToArray();
            }
        }

        public bool ExecScript(Table table, object script, ref uint count)
        {
            try
            {
                if (script is DataTable dt)
                {
                    using (NpgsqlBinaryImporter writer = conn.BeginBinaryImport(
                        $"copy {ProcessTableName(table.DestName)} from stdin binary"))
                    {
                        foreach (DataRow row in dt.Rows)
                        {
                            object[] values = row.ItemArray;

                            writer.StartRow();
                            for (int i = 0; i < values.Length; i++)
                            {
                                object value = values[i];

                                if (value == DBNull.Value || value == null)
                                    writer.WriteNull();
                                else if (dt.Columns[i].DataType == typeof(bool))
                                    writer.Write((bool)value, NpgsqlDbType.Bit);
                                else
                                    writer.Write(value);
                            }
                        }
                    }

                    count = (uint)dt.Rows.Count;

                    return true;
                }
                else if (script is MergeScript ms)
                {
                    if (Execute(ms.PrepareSQL, null, out _))
                        try
                        {
                            using (NpgsqlBinaryImporter writer = conn.BeginBinaryImport(
                                $"copy {ProcessTableName(ms.TableName)} from stdin binary"))
                            {
                                foreach (DataRow row in ms.Data.Rows)
                                {
                                    object[] values = row.ItemArray;

                                    writer.StartRow();
                                    for (int i = 0; i < values.Length; i++)
                                    {
                                        object value = values[i];

                                        if (value == DBNull.Value || value == null)
                                            writer.WriteNull();
                                        else if (ms.Data.Columns[i].DataType == typeof(bool))
                                            writer.Write((bool)value, NpgsqlDbType.Bit);
                                        else
                                            writer.Write(value);
                                    }
                                }
                            }

                            if (Execute(ms.UpdateSQL, null, out _) && Execute(ms.InsertSQL, null, out _))
                            {
                                count = (uint)ms.Data.Rows.Count;

                                return true;
                            }
                        }
                        finally
                        {
                            Execute(ms.CleanSQL, null, out _);
                        }
                }
                else if (script is UpdateScript sql)
                {
                    if (Execute(sql.UpdateSQL, null, out count))
                        if (count == 0)
                            return Execute(sql.InsertSQL, null, out count);
                        else
                            return true;
                }
                else if (script is string)
                    return Execute((string)script, null, out count);
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);
            }

            return false;
        }

        private bool Execute(string sql, Dictionary<string, object> parms, out uint count)
        {
            count = 0;
            try
            {
                NpgsqlCommand cmd = new NpgsqlCommand(sql, conn, trans);

                if (parms != null)
                    foreach (string key in parms.Keys)
                        cmd.Parameters.AddWithValue(key, parms[key]);

                count = (uint)cmd.ExecuteNonQuery();

                return true;
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);
                Logger.WriteLog(title, sql);

                return false;
            }
        }

        public bool GetFieldNames(string tableName, ref string[] fieldNames)
        {
            IDataWrapper data = null;

            if (Query($"select * from {ProcessTableName(tableName)} where 1 = 0", null, ref data))
                try
                {
                    fieldNames = data.GetFieldNames();

                    return true;
                }
                finally
                {
                    data.Close();
                }
            else
                return false;
        }

        private string GetFmtValue(object obj)
        {
            if (obj is DBNull)
                return "null";
            else if (obj is string)
                return ProcessString(obj as string);
            else if (obj is DateTime)
                return ProcessString(((DateTime)obj).ToString("yyyy-MM-dd HH:mm:ss.fff"));
            else if (obj is bool)
                return ((bool)obj ? "1" : "0") + "::bit";
            else if (obj is byte[])
                return BytesToStr(obj as byte[]);
            else
                return obj.ToString();
        }

        public string GetLastError()
        {
            return errMsg;
        }

        public string GetName()
        {
            return "PostgreSQL";
        }

        public bool GetTables(IProgress progress, List<TableInfo> lst)
        {
            List<TableFK> fks = new List<TableFK>();
            IDataWrapper data = null;
            int total = 0, position = 0;

            // 获取所有用户表清单
            if (Query("select tablename from pg_tables where schemaname = 'public' order by tablename asc", null,
                ref data))
                try
                {
                    while (data.Read())
                        fks.Add(new TableFK() { Name = (string)data.GetValue(0), Order = 0 });

                    total = data.ReadCount * 2;
                }
                finally
                {
                    data.Close();
                }

            // 获取每个表的主键字段清单
            foreach (TableFK fk in fks)
            {
                List<string> keys = new List<string>();

                if (Query("select C.attname from pg_constraint A join pg_class B on A.conrelid = B.oid "
                    + "join pg_attribute C on C.attrelid = B.oid and ARRAY_POSITION(A.conkey, C.attnum) > 0 where "
                    + $"B.relname = '{fk.Name}' and A.contype = 'p' order by C.attname asc", null, ref data))
                {
                    try
                    {
                        while (data.Read()) keys.Add((string)data.GetValue(0));
                    }
                    finally
                    {
                        data.Close();
                    }

                }

                fk.KeyFields = keys.ToArray();
                progress.OnProgress(total, ++position);
            }

            // 获取每个表的外键指向的表清单
            foreach (TableFK fk in fks)
            {
                if (Query("select A.relname from pg_class A "
                    + "join pg_constraint B on B.confrelid = A.oid and B.contype = 'f' "
                    + $"join pg_class C on C.oid = B.conrelid and C.relname = '{fk.Name}' "
                    + "order by A.relname asc", null, ref data))
                {
                    try
                    {
                        while (data.Read()) fk.FKs.Add((string)data.GetValue(0));
                    }
                    finally
                    {
                        data.Close();
                    }
                }
                progress.OnProgress(total, ++position);
            }

            int order = 100;

            foreach (TableFK fk in fks)
                if (fk.FKs.Count == 0 || (fk.FKs.Count == 1 && fk.FKs[0].Equals(fk.Name)))
                    fk.Order = order;

            order += 100;
            while (order < 10000)
            {
                int left = 0;

                foreach (TableFK fk in fks)
                    if (fk.Order == 0)
                    {
                        left++;

                        bool done = true;

                        foreach (string s in fk.FKs)
                        {
                            bool found = false;

                            foreach (TableFK fk2 in fks)
                            {
                                if (fk2.Name.Equals(s) && fk2.Order > 0)
                                {
                                    found = true;
                                    break;
                                }
                            }

                            if (!found)
                            {
                                done = false;
                                break;
                            }
                        }

                        if (done) fk.Order = order;
                    }

                if (left == 0) break;
                order += 100;
            }

            foreach (TableFK fk in fks)
                lst.Add(new TableInfo() { Name = fk.Name, KeyFields = fk.KeyFields, Order = fk.Order });

            lst.Sort(new TableInfoComparer());

            return true;

        }

        private string[] ModifyFields(string[] fields, string[] skipFields, string prefix)
        {
            if (skipFields == null || skipFields.Length == 0)
                return fields;
            else
            {
                List<string> lst = new List<string>(), skipList = new List<string>();

                foreach (string s in skipFields)
                    skipList.Add(s.ToLower());

                foreach (string s in fields)
                    if (skipList.Contains(s.ToLower()))
                        lst.Add(prefix + s);
                    else
                        lst.Add(s);

                return lst.ToArray();
            }
        }

        private string ProcessFieldName(string fieldName)
        {
            return $"\"{fieldName}\"";
        }

        private string ProcessFieldNames(string[] fields, string prefix = "")
        {
            if (fields == null || fields.Length == 0)
                return "";
            else
            {
                if (prefix == null)
                    prefix = "";
                else if (prefix.Length > 0)
                    prefix += ".";

                StringBuilder sb = new StringBuilder();

                sb.Append(prefix).Append(ProcessFieldName(fields[0]));
                for (int i = 1; i < fields.Length; i++)
                    sb.Append(", ").Append(prefix).Append(ProcessFieldName(fields[i]));

                return sb.ToString();
            }
        }

        private string ProcessString(string s)
        {
            if (string.IsNullOrEmpty(s))
                return "''";
            else
            {
                if (s.IndexOf('\'') >= 0)
                    s = s.Replace("'", "''");

                return $"'{s}'";
            }
        }

        private string ProcessTableName(string tableName)
        {
            return $"\"{tableName}\"";
        }

        private bool Query(string sql, Dictionary<string, object> parms, ref IDataWrapper reader)
        {
            try
            {
                NpgsqlCommand cmd = new NpgsqlCommand(sql, conn, trans);

                if (parms != null)
                    foreach (string key in parms.Keys)
                        cmd.Parameters.AddWithValue(key, parms[key]);

                reader = new IDataReaderWrapper(cmd.ExecuteReader(CommandBehavior.SingleResult));

                return true;
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);
                Logger.WriteLog(title, sql);

                return false;
            }

        }
        public bool QueryCount(string tableName, string whereSQL, WithEnums with, Dictionary<string, object> parms,
            ref ulong count)
        {
            IDataWrapper data = null;
            StringBuilder sb = new StringBuilder()
                .Append("select count(*) as _row_count_ from ").Append(ProcessTableName(tableName));

            if (!string.IsNullOrEmpty(whereSQL))
                sb.Append(" where ").Append(whereSQL);

            if (Query(sb.ToString(), parms, ref data))
                try
                {
                    if (data.Read())
                    {
                        object o = data.GetValue(0);

                        if (o.GetType() == typeof(long))
                            count = (ulong)(long)o;
                        else
                            count = (ulong)(int)o;
                    }
                    else
                        count = 0;

                    return true;
                }
                finally
                {
                    data.Close();
                }
            else
                return false;

        }

        public bool QueryPage(Table table, uint fromRow, uint toRow, WithEnums with, Dictionary<string, object> parms,
            ref IDataWrapper reader)
        {
            // 语法格式形如：
            // SELECT <fieldsSQL> FROM <tableName> {WHERE <whereSQL>}
            // {ORDER BY <orderSQL>} LIMIT <toRow - fromRow + 1> OFFSET <fromRow - 1>
            string fieldsSQL = ProcessFieldNames(table.SourceFields);
            StringBuilder sb = new StringBuilder()
                .Append("select ").Append(fieldsSQL).Append(" from ").Append(ProcessTableName(table.SourceName));

            if (!string.IsNullOrEmpty(table.SourceWhereSQL))
                sb.Append(" where ").Append(table.SourceWhereSQL);
            if (!string.IsNullOrEmpty(table.OrderSQL))
                sb.Append(" order by ").Append(table.OrderSQL);

            sb.Append(" limit ").Append(toRow - fromRow + 1).Append(" offset ").Append(fromRow - 1);

            return Query(sb.ToString(), parms, ref reader);
        }

        public bool QueryParam(string sql, Dictionary<string, object> parms)
        {
            IDataWrapper data = null;

            if (!string.IsNullOrEmpty(sql))
            {
                if (Query(sql, null, ref data))
                    try
                    {
                        if (data.Read())
                        {
                            for (int i = 0; i < data.FieldCount; i++)
                                parms.Add(data.GetFieldName(i), data.GetValue(i));

                            return true;
                        }
                    }
                    finally
                    {
                        data.Close();
                    }

                return false;
            }

            return true;
        }

        public bool RollbackTransaction()
        {
            try
            {
                trans.Rollback();

                return true;
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);

                return false;
            }
            finally
            {
                trans = null;
            }
        }
    }
}
