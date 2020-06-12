using IBM.Data.DB2;
using JHWork.DataMigration.Common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace DB2
{
    /// <summary>
    /// DB2
    /// </summary>
    public class DB2 : IDBMSAssistant, IDBMSReader, IDBMSWriter, IAssemblyLoader
    {
        private readonly DB2Connection conn = new DB2Connection();
        private DB2Transaction trans = null;
        private string errMsg = "";
        private string title = "DB2";
        private string schema = "";

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

        public bool BuildScript(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            // DB2BulkCopy 不支持事务，只能使用脚本模式
            if (data.Read())
            {
                if (table is MaskingTable)
                    BuildScriptWithMaskSQL(table, data, filter, out script);
                else if (table.WriteMode == WriteModes.Append)
                    BuildScriptWithInsertSQL(table, table.DestName, data, filter, out script);
                else
                    BuildScriptWithMergeSQL(table, data, filter, out script);

                return true;
            }
            else
            {
                script = null;

                return false;
            }
        }

        private void BuildScriptWithInsertSQL(Table table, string tableName, IDataWrapper data, IDataFilter filter,
            out object script)
        {
            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(table.DestFields, table.SkipFields);

            data.MapFields(fields);

            sb.Append($"INSERT INTO {ProcessTableName(tableName)} ({ProcessFieldNames(fields)})")
                .AppendLine().Append("VALUES").AppendLine()
                .Append("(").Append(GetFmtValue(filter.GetValue(data, 0, fields[0])));
            for (int i = 1; i < fields.Length; i++)
                sb.Append(", ").Append(GetFmtValue(filter.GetValue(data, i, fields[i])));
            sb.Append(")");

            int r = 1;
            while (r < table.PageSize && data.Read())
            {
                r++;
                sb.Append(",").AppendLine().Append("(").Append(GetFmtValue(filter.GetValue(data, 0, fields[0])));
                for (int i = 1; i < fields.Length; i++)
                    sb.Append(", ").Append(GetFmtValue(filter.GetValue(data, i, fields[i])));
                sb.Append(")");
            }

            script = sb.ToString();
        }

        private void BuildScriptWithMaskSQL(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            string destTable = ProcessTableName(table.DestName);
            string tmpTable = $"destTable.Substring(1, destTable.Length - 2)_{Guid.NewGuid():N}";
            string processedTmpTable = ProcessTableName(tmpTable);
            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(table.DestFields, table.KeyFields, table.SkipFields);
            string field = ProcessFieldName(fields[0]);

            sb.Append($"UPDATE {destTable} SET {field} = B.{field}");
            for (int i = 1; i < fields.Length; i++)
            {
                field = ProcessFieldName(fields[i]);
                sb.Append($", {field} = B.{field}");
            }
            field = ProcessFieldName(table.KeyFields[0]);
            sb.Append($" FROM {processedTmpTable} B WHERE {destTable}.{field} = B.{field}");
            for (int i = 1; i < table.KeyFields.Length; i++)
            {
                field = ProcessFieldName(table.KeyFields[i]);
                sb.Append($" AND {destTable}.{field} = B.{field}");
            }

            BuildScriptWithInsertSQL(table, tmpTable, data, filter, out script);
            script = new MergeScript()
            {
                TableName = tmpTable,
                PrepareSQL = $"CREATE TABLE {processedTmpTable} AS"
                    + $" (SELECT {ProcessFieldNames(table.DestFields)} FROM {destTable} WHERE 1 = 0) WITH NO DATA",
                InsertSQL = script.ToString(),
                MergeSQL = sb.ToString(),
                CleanSQL = $"DROP TABLE {processedTmpTable}"
            };
        }

        private void BuildScriptWithMergeSQL(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            string destTable = ProcessTableName(table.DestName);
            string tmpTable = $"destTable.Substring(1, destTable.Length - 2)_{Guid.NewGuid():N}";
            string processedTmpTable = ProcessTableName(tmpTable);
            StringBuilder sb = new StringBuilder();
            string[] fields = ExcludeFields(table.DestFields, table.KeyFields, table.SkipFields);
            string field = ProcessFieldName(table.KeyFields[0]);

            sb.Append($"MERGE INTO {destTable} A USING {processedTmpTable} B ON A.{field} = B.{field}");
            for (int i = 1; i < table.KeyFields.Length; i++)
            {
                field = ProcessFieldName(table.KeyFields[i]);
                sb.Append($" AND A.{field} = B.{field}");
            }
            field = ProcessFieldName(fields[0]);
            sb.AppendLine().Append($" WHEN MATCHED THEN UPDATE SET A.{field} = B.{field}");
            for (int i = 1; i < fields.Length; i++)
            {
                field = ProcessFieldName(fields[i]);
                sb.Append($", A.{field} = B.{field}");
            }

            fields = ExcludeFields(table.DestFields, table.SkipFields);
            sb.AppendLine().Append($" WHEN NOT MATCHED THEN INSERT ({ProcessFieldNames(fields)}")
                .Append($") VALUES ({ProcessFieldNames(fields, "B")});"); // 语句以分号结尾

            BuildScriptWithInsertSQL(table, tmpTable, data, filter, out script);
            script = new MergeScript()
            {
                TableName = tmpTable,
                PrepareSQL = $"CREATE TABLE {processedTmpTable} LIKE {destTable}",
                InsertSQL = script.ToString(),
                MergeSQL = sb.ToString(),
                CleanSQL = $"DROP TABLE {processedTmpTable}"
            };
        }

        private string BytesToStr(byte[] bytes)
        {
            if (bytes == null) return "NULL";

            StringBuilder sb = new StringBuilder("0x", bytes.Length * 2 + 4); // 冗余两个字符，确保不触发内存扩展

            foreach (byte b in bytes)
                sb.Append(b.ToString("X2"));

            return sb.ToString();
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
            title = $"{db.Server}/{db.DB}";

            string security = db.Encrypt ? ";Security=SSL" : "";
            string[] ss = db.DB.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);

            if (ss.Length > 1)
                schema = ss[1];
            else
                schema = "";

            try
            {
                conn.Close();
                conn.ConnectionString = $"Server={db.Server}:{db.Port};Database={ss[0]};User ID={db.User}"
                    + $";Password={db.Pwd};Pooling=false;Persist Security Info=True{security}";
                conn.Open();

                if (!string.IsNullOrWhiteSpace(schema))
                    Execute($"SET SCHEMA {schema}", null, out _);

                return true;
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);

                return false;
            }
        }

        private bool EnableIdentityInsert(string tableName, bool status)
        {
            string sql = $"SELECT COLNAME FROM SYSCAT.COLUMNS WHERE TABNAME = '{tableName}' AND IDENTITY = 'Y'";
            if (!string.IsNullOrWhiteSpace(schema)) sql += $" AND TABSCHEMA = '{schema}'";

            if (Query(sql, null, out IDataWrapper data))
            {
                try
                {
                    while (data.Read())
                    {
                        string col = data.GetValue(0).ToString();

                        Execute("ALTER TABLE {ProcessTableName(tableName)}"
                            + $" ALTER COLUMN {ProcessFieldName(col)} SET GENERATED "
                            + (status ? "BY DEFAULT" : "ALWAYS"), null, out _);
                    }
                }
                finally
                {
                    data.Close();
                }

                return true;
            }

            return false;
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

        private string[] ExcludeFields(string[] fields, string[] skipFields, string[] skipFields2)
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

        public bool ExecScript(Table table, object script, out uint count)
        {
            count = 0;
            try
            {
                if (script is string s)
                    return Execute(s, null, out count);
                else if (script is MergeScript ms)
                {
                    if (Execute(ms.PrepareSQL, null, out _))
                        try
                        {
                            if (table.KeepIdentity) EnableIdentityInsert(ms.TableName, true);
                            try
                            {
                                if (!Execute(ms.InsertSQL, null, out count)) return false;

                                if (table.KeepIdentity) EnableIdentityInsert(table.DestName, true);
                                try
                                {
                                    if (Execute(ms.MergeSQL, null, out _)) return true;
                                }
                                finally
                                {
                                    if (table.KeepIdentity) EnableIdentityInsert(table.DestName, false);
                                }
                            }
                            finally
                            {
                                if (table.KeepIdentity) EnableIdentityInsert(ms.TableName, false);
                            }
                        }
                        finally
                        {
                            Execute(ms.CleanSQL, null, out _);
                        }
                }
            }
            catch (Exception ex)
            {
                errMsg = $"{table.DestName}：{ex.Message}";
                Logger.WriteLogExcept(title, ex);
            }

            return false;
        }

        private bool Execute(string sql, Dictionary<string, object> parms, out uint count)
        {
            try
            {
                DB2Command cmd = new DB2Command(sql, conn, trans);

                if (parms != null)
                    foreach (string key in parms.Keys)
                        cmd.Parameters.Add(key, parms[key]);

                count = (uint)cmd.ExecuteNonQuery();

                return true;
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);
                Logger.WriteLog(title, sql);
                count = 0;

                return false;
            }
        }

        public bool GetFieldNames(string tableName, out string[] fieldNames)
        {
            if (Query($"SELECT * FROM {ProcessTableName(tableName)} WHERE 1 = 0", null, out IDataWrapper data))
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
            {
                fieldNames = null;
                return false;
            }
        }

        private string GetFmtValue(object obj)
        {
            if (obj is DBNull)
                return "NULL";
            else if (obj is string)
                return ProcessString(obj as string);
            else if (obj is DateTime dt)
                return ProcessString(dt.ToString("yyyy-MM-dd HH:mm:ss.fff"));
            else if (obj is bool b)
                return b ? "1" : "0";
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
            return "DB2";
        }

        public bool GetTables(IProgress progress, List<TableInfo> lst)
        {
            List<TableFK> fks = new List<TableFK>();
            int total = 0, position = 0;

            // 获取所有用户表清单
            string sql = "SELECT TABNAME FROM SYSCAT.TABLES WHERE TYPE = 'T'";

            if (!string.IsNullOrWhiteSpace(schema)) sql += $" AND TABSCHEMA = '{schema}'";
            if (Query(sql + " ORDER BY TABNAME ASC", null, out IDataWrapper data))
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

                sql = $"SELECT COLNAME FROM SYSCAT.COLUMNS WHERE TABNAME = '{fk.Name}' AND KEYSEQ = 1";
                if (!string.IsNullOrWhiteSpace(schema)) sql += $" AND TABSCHEMA = '{schema}'";
                if (Query(sql + " ORDER BY COLNAME ASC", null, out data))
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
                sql = $"SELECT REFTABNAME FROM SYSCAT.REFERENCES WHERE TABNAME = '{fk.Name}'";
                if (!string.IsNullOrWhiteSpace(schema)) sql += $" AND TABSCHEMA = '{schema}'";
                if (Query(sql, null, out data))
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

        private string ProcessFieldName(string fieldName, string prefix = "")
        {
            if (string.IsNullOrEmpty(fieldName)) return "";

            if (prefix == null)
                prefix = "";
            else if (prefix.Length > 0)
                prefix += ".";

            if (fieldName.StartsWith("\""))
                return prefix + fieldName;
            else
                return prefix + $"\"{fieldName}\"";
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

        private string ProcessTableName(string tableName, string alias = "")
        {
            if (string.IsNullOrEmpty(tableName))
                return "";
            else
            {
                if (!tableName.StartsWith("\""))
                    tableName = $"\"{tableName}\"";

                if (!string.IsNullOrEmpty(alias))
                    tableName += $" {alias}";

                return tableName;
            }
        }

        private bool Query(string sql, Dictionary<string, object> parms, out IDataWrapper reader)
        {
            try
            {
                DB2Command cmd = new DB2Command(sql, conn, trans);

                if (parms != null)
                    foreach (string key in parms.Keys)
                        cmd.Parameters.Add(key, parms[key]);

                reader = new IDataReaderWrapper(cmd.ExecuteReader(CommandBehavior.SingleResult));

                return true;
            }
            catch (Exception ex)
            {
                errMsg = ex.Message;
                Logger.WriteLogExcept(title, ex);
                Logger.WriteLog(title, sql);
                reader = null;

                return false;
            }

        }

        public bool QueryCount(Table table, WithEnums with, Dictionary<string, object> parms, out ulong count)
        {
            StringBuilder sb = new StringBuilder()
                .Append("SELECT COUNT(*) AS \"_ROW_COUNT_\" FROM ").Append(ProcessTableName(table.SourceName));

            if (!string.IsNullOrEmpty(table.WhereSQL))
            {
                if (table.WhereSQL.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
                    sb.Append(" WHERE");
                sb.Append($" {table.WhereSQL}");
            }

            count = 0;
            if (Query(sb.ToString(), parms, out IDataWrapper data))
                try
                {
                    if (data.Read())
                        count = ulong.Parse(data.GetValue(0).ToString());

                    return true;
                }
                finally
                {
                    data.Close();
                }
            else
                return false;
        }

        public bool QueryPage(Table table, uint fromRow, uint toRow, WithEnums with, Dictionary<string, object> parms, out IDataWrapper reader)
        {
            StringBuilder sb = new StringBuilder();

            // 如果存在主键：
            // SELECT <B.fieldsSQL> FROM <tableName> B JOIN (SELECT <keyFields> FROM
            // (SELECT <keyFields>, ROW_NUMBER() OVER (ORDER BY <orderSQL>) AS "_RowNum_"
            // FROM <tableName>
            // {WHERE <whereSQL>}
            // ) A WHERE "_RowNum_" BETWEEN <fromRow> AND <toRow>) A ON <B.keyFields> = <A.keyFields>
            if (table.KeyFields.Length > 0)
            {
                string fieldsSQL = ProcessFieldNames(table.SourceFields, "B");
                string tableName = ProcessTableName(table.SourceName);
                string tableNameWithB = ProcessTableName(table.SourceName, "B");
                string keyFields = ProcessFieldNames(table.KeyFields);
                string keyFieldsWithAlias = ProcessFieldNames(table.KeyFields, ProcessTableName(table.SourceName));
                string keyField = ProcessFieldName(table.KeyFields[0]);

                sb.Append($"SELECT {fieldsSQL} FROM {tableNameWithB} JOIN (SELECT {keyFields} FROM")
                    .Append($" (SELECT {keyFieldsWithAlias}, ROW_NUMBER() OVER (ORDER BY {table.OrderSQL})")
                    .Append($" AS \"_RowNum_\" FROM {tableName}");
                if (!string.IsNullOrEmpty(table.WhereSQL))
                {
                    if (table.WhereSQL.IndexOf(" WEHRE ", StringComparison.OrdinalIgnoreCase) < 0)
                        sb.Append(" WHERE");
                    sb.Append($" {table.WhereSQL}");
                }
                sb.Append($") A WHERE A.\"_RowNum_\" BETWEEN {fromRow} AND {toRow}) A ON B.{keyField} = A.{keyField}");
                for (int i = 1; i < table.KeyFields.Length; i++)
                {
                    keyField = ProcessFieldName(table.KeyFields[i]);
                    sb.Append($" AND B.{keyField} = A.{keyField}");
                }

                return Query(sb.ToString(), parms, out reader);
            }
            else
            {
                // 语法格式形如：
                // SELECT <fieldsSQL> FROM (SELECT ROW_NUMBER() OVER (ORDER BY <orderSQL>)
                // AS "_RowNum_", <fieldsSQL> FROM <tableName>
                // {WHERE <whereSQL>}
                // ) A WHERE A."_RowNum_" BETWEEN <fromRow> AND <toRow>
                string fieldsSQL = ProcessFieldNames(table.SourceFields);
                string fieldsWithAlias = ProcessFieldNames(table.SourceFields, ProcessTableName(table.SourceName));

                sb.Append($"SELECT {fieldsSQL} FROM (SELECT ROW_NUMBER() OVER (ORDER BY {table.OrderSQL})")
                    .Append($" AS \"_RowNum_\", {fieldsWithAlias} FROM {ProcessTableName(table.SourceName)}");
                if (!string.IsNullOrEmpty(table.WhereSQL))
                {
                    if (table.WhereSQL.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
                        sb.Append(" WHERE");
                    sb.Append($" {table.WhereSQL}");
                }
                sb.Append($") A WHERE A.\"_RowNum_\" BETWEEN {fromRow} AND {toRow}");

                return Query(sb.ToString(), parms, out reader);
            }
        }

        public bool QueryParam(string sql, Dictionary<string, object> parms)
        {
            if (!string.IsNullOrEmpty(sql))
            {
                if (Query(sql, null, out IDataWrapper data))
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

    /// <summary>
    /// 基于 MERGE INTO 语法的更新数据脚本对象
    /// </summary>
    internal class MergeScript
    {
        public string TableName { get; set; }  // 临表名称
        public string PrepareSQL { get; set; } // 准备 SQL（创建临表）
        public string InsertSQL { get; set; }  // 数据
        public string MergeSQL { get; set; }   // 合并 SQL
        public string CleanSQL { get; set; }   // 清理 SQL
    }

    /// <summary>
    /// 表外键信息
    /// </summary>
    internal class TableFK : TableInfo
    {
        public List<string> FKs { get; } = new List<string>(); // 外键指向表
    }
}
