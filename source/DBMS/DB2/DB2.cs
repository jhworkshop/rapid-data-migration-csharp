using IBM.Data.DB2;
using JHWork.DataMigration.Common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace JHWork.DataMigration.DBMS.DB2
{
    /// <summary>
    /// DB2
    /// </summary>
    public class DB2 : DBMSBase, IDBMSAssistant, IDBMSReader, IDBMSWriter, IAssemblyLoader
    {
        private readonly DB2Connection conn = new DB2Connection();
        private DB2Transaction trans = null;

        public DB2()
        {
            LogTitle = GetName();
        }

        public bool BeginTransaction()
        {
            try
            {
                trans = conn.BeginTransaction();

                return true;
            }
            catch (Exception ex)
            {
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);

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

            sb.Append($"INSERT INTO {ProcessTableName(tableName, table.DestSchema)} ({ProcessFieldNames(fields)})")
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
            string destTable = ProcessTableName(table.DestName, table.DestSchema);
            string tmpTable = $"{ExtractTableName(table.DestName)}_{Guid.NewGuid():N}";
            string processedTmpTable = ProcessTableName(tmpTable, table.DestSchema);
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
            string destTable = ProcessTableName(table.DestName, table.DestSchema);
            string tmpTable = $"{ExtractTableName(table.DestName)}_{Guid.NewGuid():N}";
            string processedTmpTable = ProcessTableName(tmpTable, table.DestSchema);
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

            StringBuilder sb = new StringBuilder("BLOB(x'", bytes.Length * 2 + 16); // 冗余几个字符，确保不触发内存扩展

            foreach (byte b in bytes)
                sb.Append(b.ToString("X2"));

            return sb.Append("')").ToString();
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
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);

                return false;
            }
            finally
            {
                trans = null;
            }
        }

        public bool Connect(Database db)
        {
            string security = db.Encrypt ? ";Security=SSL" : "";

            LogTitle = $"{db.Server}/{db.DB}";
            Schema = db.Schema;
            Timeout = db.Timeout;
            try
            {
                conn.Close();
                conn.ConnectionString = $"Server={db.Server}:{db.Port};Database={db.DB};User ID={db.User}"
                    + $";Password={db.Pwd};Pooling=false;Persist Security Info=True{security}";
                conn.Open();

                if (!string.IsNullOrEmpty(Schema))
                    Execute($"SET SCHEMA {Schema}", null, out _);

                return true;
            }
            catch (Exception ex)
            {
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);

                return false;
            }
        }

        private bool EnableIdentityInsert(string tableName, string schema, bool status)
        {
            string sql = $"SELECT COLNAME FROM SYSCAT.COLUMNS WHERE TABNAME = '{tableName}' AND IDENTITY = 'Y'";

            if (!IsEmpty(out string s, schema, Schema)) sql += $" AND TABSCHEMA = '{s}'";

            if (Query(sql, null, out IDataWrapper data))
            {
                List<string> cols = new List<string>();

                try
                {
                    while (data.Read())
                        cols.Add((string)data.GetValue(0));
                }
                finally
                {
                    data.Close();
                }

                foreach (string col in cols)
                    Execute($"ALTER TABLE {ProcessTableName(tableName, schema)}"
                        + $" ALTER COLUMN {ProcessFieldName(col)} SET GENERATED "
                        + (status ? "BY DEFAULT" : "ALWAYS"), null, out _);

                return true;
            }

            return false;
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
                            if (table.KeepIdentity) EnableIdentityInsert(ms.TableName, table.DestSchema, true);
                            try
                            {
                                if (!Execute(ms.InsertSQL, null, out count)) return false;
                            }
                            finally
                            {
                                if (table.KeepIdentity) EnableIdentityInsert(ms.TableName, table.DestSchema, false);
                            }

                            if (table.KeepIdentity) EnableIdentityInsert(table.DestName, table.DestSchema, true);
                            try
                            {
                                if (Execute(ms.MergeSQL, null, out _)) return true;
                            }
                            finally
                            {
                                if (table.KeepIdentity) EnableIdentityInsert(table.DestName, table.DestSchema, false);
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
                LastError = $"{table.DestName}：{ex.Message}";
                Logger.WriteLogExcept(LogTitle, ex);
            }

            return false;
        }

        private bool Execute(string sql, Dictionary<string, object> parms, out uint count)
        {
            try
            {
                DB2Command cmd = new DB2Command(sql, conn, trans)
                {
                    CommandTimeout = (int)Timeout,
                    CommandType = CommandType.Text
                };

                if (parms != null)
                    foreach (string key in parms.Keys)
                        cmd.Parameters.Add(key, parms[key]);

                count = (uint)cmd.ExecuteNonQuery();

                return true;
            }
            catch (Exception ex)
            {
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);
                Logger.WriteLog(LogTitle, sql);
                count = 0;

                return false;
            }
        }

        private string ExtractTableName(string name)
        {
            if (string.IsNullOrEmpty(name)) return "";

            string[] parts = name.Split('.');

            name = parts[parts.Length - 1]; // 最后一段

            return name.Replace("\"", "");
        }

        public bool GetFieldNames(string tableName, string schema, out string[] fieldNames)
        {
            if (Query($"SELECT * FROM {ProcessTableName(tableName, schema)} WHERE 1 = 0", null, out IDataWrapper data))
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

        public string GetName()
        {
            return "DB2";
        }

        public DBMSParams GetParams()
        {
            return new DBMSParams()
            {
                CharSet = false,
                Compress = false
            };
        }

        protected override string[] GetTableKeys(string table, string schema)
        {
            string sql = $"SELECT COLNAME FROM SYSCAT.COLUMNS WHERE TABNAME = '{table}' AND KEYSEQ = 1";

            if (string.IsNullOrEmpty(schema)) schema = Schema;
            if (!string.IsNullOrEmpty(schema)) sql += $" AND TABSCHEMA = '{schema}'";

            if (Query(sql + " ORDER BY COLNAME ASC", null, out IDataWrapper data))
                return GetValues(data);
            else
                return new string[] { };
        }

        protected override string[] GetTableRefs(string table, string schema)
        {
            string sql = $"SELECT REFTABNAME FROM SYSCAT.REFERENCES WHERE TABNAME = '{table}'";

            if (string.IsNullOrEmpty(schema)) schema = Schema;
            if (!string.IsNullOrEmpty(schema)) sql += $" AND TABSCHEMA = '{schema}'";

            if (Query(sql, null, out IDataWrapper data))
                return GetValues(data);
            else
                return new string[] { };
        }

        protected override string[] GetTables()
        {
            string sql = "SELECT TABNAME, TABSCHEMA FROM SYSCAT.TABLES WHERE TYPE = 'T'";

            if (!string.IsNullOrEmpty(Schema)) sql += $" AND TABSCHEMA = '{Schema}'";

            if (Query(sql + " ORDER BY TABSCHEMA ASC, TABNAME ASC", null, out IDataWrapper data))
                try
                {
                    List<string> lst = new List<string>();

                    while (data.Read())
                        lst.Add($"{data.GetValue(1)}.{data.GetValue(0)}");

                    return lst.ToArray();
                }
                finally
                {
                    data.Close();
                }
            else
                return new string[] { };
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

        private string ProcessTableName(string tableName, string schema, string alias = "")
        {
            if (string.IsNullOrEmpty(tableName))
                return "";
            else
            {
                if (!tableName.StartsWith("\""))
                    tableName = $"\"{tableName}\"";

                if (!IsEmpty(out string s, schema, Schema))
                    if (!s.StartsWith("\""))
                        tableName = $"\"{s}\".{tableName}";
                    else
                        tableName = $"{s}.{tableName}";

                if (!string.IsNullOrEmpty(alias))
                    tableName += " " + alias;

                return tableName;
            }
        }

        private bool Query(string sql, Dictionary<string, object> parms, out IDataWrapper reader)
        {
            try
            {
                DB2Command cmd = new DB2Command(sql, conn, trans)
                {
                    CommandTimeout = (int)Timeout,
                    CommandType = CommandType.Text
                };

                if (parms != null)
                    foreach (string key in parms.Keys)
                        cmd.Parameters.Add(key, parms[key]);

                reader = new IDataReaderWrapper(cmd.ExecuteReader(CommandBehavior.SingleResult));

                return true;
            }
            catch (Exception ex)
            {
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);
                Logger.WriteLog(LogTitle, sql);
                reader = null;

                return false;
            }

        }

        public bool QueryCount(Table table, WithEnums with, Dictionary<string, object> parms, out ulong count)
        {
            StringBuilder sb = new StringBuilder()
                .Append("SELECT COUNT(*) AS \"_ROW_COUNT_\" FROM ")
                .Append(ProcessTableName(table.SourceName, table.SourceSchema));

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
                string tableName = ProcessTableName(table.SourceName, table.SourceSchema);
                string tableNameWithB = ProcessTableName(table.SourceName, table.SourceSchema, "B");
                string keyFields = ProcessFieldNames(table.KeyFields);
                string keyFieldsWithAlias = ProcessFieldNames(table.KeyFields, ProcessTableName(table.SourceName, table.SourceSchema));
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
                string fieldsWithAlias = ProcessFieldNames(table.SourceFields, ProcessTableName(table.SourceName, table.SourceSchema));

                sb.Append($"SELECT {fieldsSQL} FROM (SELECT ROW_NUMBER() OVER (ORDER BY {table.OrderSQL})")
                    .Append($" AS \"_RowNum_\", {fieldsWithAlias} FROM {ProcessTableName(table.SourceName, table.SourceSchema)}");
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
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);

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
}
