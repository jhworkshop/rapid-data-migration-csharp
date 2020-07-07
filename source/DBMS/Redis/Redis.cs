using JHWork.DataMigration.Common;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace JHWork.DataMigration.DBMS.Redis
{
    /// <summary>
    /// Redis
    /// </summary>
    public class Redis : DBMSBase, IAssemblyLoader, IDBMSAssistant, IDBMSReader, IDBMSWriter
    {
        private const string KeyField = "key";
        private const string ValueField = "value";

        private class RedisWrapper : IDataWrapper
        {
            private readonly RedisKey[] keys;
            private readonly RedisValue[] values;
            private int curRow = -1;
            private int[] indexMaps;
            private string[] names;

            public int FieldCount => indexMaps.Length;

            public int ReadCount { get; private set; } = 0;

            public RedisWrapper(RedisKey[] keys, RedisValue[] values)
            {
                this.keys = keys;
                this.values = values;

                ResetMap();
            }

            public void Close() { }

            public string GetFieldName(int i)
            {
                return names[i];
            }

            public string[] GetFieldNames()
            {
                return names;
            }

            public Type GetFieldType(int i)
            {
                return typeof(byte[]);
            }

            private int GetMappedIndex(int i)
            {
                return indexMaps[i];
            }

            public object GetValue(int i)
            {
                if (i >= 0 && curRow >= 0 && curRow < keys.Length)
                {
                    i = GetMappedIndex(i);
                    if (i == 0)
                        return (byte[])keys[i];
                    else if (i == 1)
                        if (values[i] == RedisValue.Null)
                            return DBNull.Value;
                        else
                            return (byte[])values[i];
                }

                return DBNull.Value;
            }

            public object GetValueByOriName(string field)
            {
                if (curRow >= 0 && curRow < keys.Length)
                    if (ValueField.Equals(field))
                        return values[curRow];
                    else if (KeyField.Equals(field))
                        return keys[curRow];

                return DBNull.Value;
            }

            public void MapFields(string[] fields)
            {
                if (fields == null || fields.Length == 0)
                    ResetMap();
                else
                {
                    indexMaps = new int[fields.Length];
                    names = fields;

                    // 对照
                    for (int i = 0; i < fields.Length; i++)
                        if (KeyField.Equals(fields[i]))
                            indexMaps[i] = 0;
                        else if (ValueField.Equals(fields[i]))
                            indexMaps[i] = 1;
                        else
                            indexMaps[i] = -1;

                }
            }

            public bool Read()
            {
                curRow++;
                if (curRow < keys.Length)
                {
                    ReadCount++;
                    return true;
                }
                else
                    return false;
            }

            private void ResetMap()
            {
                indexMaps = new int[] { 0, 1 };
                names = new string[] { KeyField, ValueField };
            }
        }

        private ConnectionMultiplexer redisClient = null;
        private IServer redisServer = null;
        private IDatabase redisDB = null;
        private Encoding encoding = Encoding.UTF8;
        private int dbIndex = 0;

        public bool BeginTransaction()
        {
            // StackExchange.Redis 1.2.6 事务不确定怎么 DISCARD，暂时不支持事务
            return true;
        }

        public bool BuildScript(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            if (data.Read())
            {
                BuildScriptWithKeyValuePair(table, data, filter, out script);
                return true;
            }
            else
            {
                script = null;
                return false;
            }
        }

        private void BuildScriptWithKeyValuePair(Table table, IDataWrapper data, IDataFilter filter, out object script)
        {
            List<KeyValuePair<RedisKey, RedisValue>> lst = new List<KeyValuePair<RedisKey, RedisValue>>();
            int r = 1;

            if (data.GetValueByOriName(KeyField) is RedisKey rk)
            {
                object obj = filter.GetValue(data, -1, KeyField);

                if (obj != DBNull.Value && obj != null) rk = (RedisKey)obj;
                obj = filter.GetValue(data, -1, ValueField);
                if (obj == DBNull.Value || obj == null) obj = data.GetValueByOriName(ValueField);

                lst.Add(new KeyValuePair<RedisKey, RedisValue>(rk, (RedisValue)obj));
                while (r < table.PageSize && data.Read())
                {
                    r++;
                    obj = filter.GetValue(data, -1, KeyField);
                    if (obj != DBNull.Value && obj != null) rk = (RedisKey)obj;
                    obj = filter.GetValue(data, -1, ValueField);
                    if (obj == DBNull.Value || obj == null) obj = data.GetValueByOriName(ValueField);

                    lst.Add(new KeyValuePair<RedisKey, RedisValue>(rk, (RedisValue)obj));
                }
            }
            else
            {
                RedisKey key = GetFmtValue(filter.GetValue(data, 0, KeyField));
                RedisValue value = GetFmtValue(filter.GetValue(data, 1, ValueField));

                lst.Add(new KeyValuePair<RedisKey, RedisValue>(key, value));
                while (r < table.PageSize && data.Read())
                {
                    r++;
                    key = GetFmtValue(filter.GetValue(data, 0, KeyField));
                    value = GetFmtValue(filter.GetValue(data, 1, ValueField));

                    lst.Add(new KeyValuePair<RedisKey, RedisValue>(key, value));
                }
            }

            script = lst;
        }

        public void Close()
        {
            redisClient.Dispose();
            redisClient = null;
        }

        public bool CommitTransaction()
        {
            return true;
        }

        public bool Connect(Database db)
        {
            LogTitle = $"{db.Server}:{db.Port}/{db.DB}";
            try
            {
                ConfigurationOptions options = new ConfigurationOptions()
                {
                    EndPoints =
                        {
                            { db.Server, (int)db.Port }
                        },
                    Password = string.IsNullOrEmpty(db.Pwd)? null:db.Pwd,
                    Ssl = db.Encrypt,
                    SyncTimeout = 1000 * (int)db.Timeout
                };

                redisClient = ConnectionMultiplexer.Connect(options);
                redisServer = redisClient.GetServer(db.Server, (int)db.Port);
                dbIndex = int.Parse(db.DB);
                redisDB = redisClient.GetDatabase(dbIndex);
                encoding = Encoding.GetEncoding(db.CharSet);

                return true;
            }
            catch (Exception ex)
            {
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);

                return false;
            }
        }

        public bool ExecScript(Table table, object script, out uint count)
        {
            if (script is List<KeyValuePair<RedisKey, RedisValue>> pair)
            {
                redisDB.StringSet(pair.ToArray());
                count = (uint)pair.Count;
                return true;
            }
            else
            {
                count = 0;
                return false;
            }
        }

        public bool GetFieldNames(string tableName, string schema, out string[] fieldNames)
        {
            fieldNames = new string[] { KeyField, ValueField };
            return true;
        }

        private byte[] GetFmtValue(object value)
        {
            if (value == DBNull.Value || value == null)
                return encoding.GetBytes("null");
            if (value is byte[] bs)
                return bs;
            else if (value is string s)
                return encoding.GetBytes(s);
            else
                return encoding.GetBytes(value.ToString());
        }

        public string GetName()
        {
            return "Redis";
        }

        public DBMSParams GetParams()
        {
            return new DBMSParams()
            {
                Schema = false,
                User = false,
                Compress = false,
                DefaultPort = "6379",
                DefaultCharSet = "utf-8"
            };
        }

        protected override string[] GetTableKeys(string table, string schema)
        {
            return new string[] { KeyField };
        }

        protected override string[] GetTableRefs(string table, string schema)
        {
            return new string[] { };
        }

        protected override string[] GetTables()
        {
            return new string[] { "redis" };
        }

        public bool QueryCount(Table table, WithEnums with, Dictionary<string, object> parms, out ulong count)
        {
            RedisValue pattern = default;

            if (!string.IsNullOrEmpty(table.WhereSQL))
            {
                string where = table.WhereSQL;

                foreach (string key in parms.Keys)
                    where = where.Replace("@" + key, parms[key].ToString());

                pattern = where;
            }

            try
            {
                count = (ulong)redisServer.Keys(dbIndex, pattern, int.MaxValue).Count();

                return true;
            }
            catch (Exception ex)
            {
                count = 0;
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);

                return false;
            }
        }

        public bool QueryPage(Table table, uint fromRow, uint toRow, WithEnums with, Dictionary<string, object> parms, out IDataWrapper reader)
        {
            RedisValue pattern = default;

            if (!string.IsNullOrEmpty(table.WhereSQL))
            {
                string where = table.WhereSQL;

                foreach (string key in parms.Keys)
                    where = where.Replace("@" + key, parms[key].ToString());

                pattern = where;
            }

            try
            {
                RedisKey[] keys = redisServer.Keys(dbIndex, pattern, (int)(toRow - fromRow + 1), 0,
                    (int)((fromRow - 1) / (toRow - fromRow + 1))).ToArray();
                RedisValue[] values = redisDB.StringGet(keys);

                reader = new RedisWrapper(keys, values);

                return true;
            }
            catch (Exception ex)
            {
                reader = null;
                LastError = ex.Message;
                Logger.WriteLogExcept(LogTitle, ex);

                return false;
            }
        }

        public bool QueryParam(string sql, Dictionary<string, object> parms)
        {
            return true;
        }

        public bool RollbackTransaction()
        {
            return false;
        }
    }
}
