using System;
using System.Collections.Generic;
using System.Data;

namespace JHWork.DataMigration.Common
{
    /// <summary>
    /// IDataWrapper 的 IDataReader 实现
    /// </summary>
    public class IDataReaderWrapper : IDataWrapper
    {
        private readonly IDataReader reader;
        private int[] indexMaps;
        private string[] names;

        public int FieldCount => indexMaps.Length;

        public int ReadCount { get; private set; } = 0;

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="reader">源数据</param>
        public IDataReaderWrapper(IDataReader reader)
        {
            this.reader = reader;

            ResetMap();
        }

        public void Close()
        {
            reader.Close();
        }

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
            i = GetMappedIndex(i);
            if (i < 0)
                return typeof(string);
            else
                return reader.GetFieldType(i);
        }

        private int GetMappedIndex(int i)
        {
            return indexMaps[i];
        }

        public object GetValue(int i)
        {
            i = GetMappedIndex(i);
            if (i < 0)
                return DBNull.Value;
            else
                return reader.GetValue(i);
        }

        public object GetValueByOriName(string field)
        {
            int i = reader.GetOrdinal(field);

            if (i < 0)
                return DBNull.Value;
            else
                return reader.GetValue(i);
        }

        public void MapFields(string[] fields)
        {
            if (fields == null || fields.Length == 0)
                ResetMap();
            else
            {
                Dictionary<string, int> dict = new Dictionary<string, int>();

                // 构造原始字段清单，以支持多次映射
                for (int i = 0; i < reader.FieldCount; i++)
                    dict.Add(reader.GetName(i).ToLower(), i);

                indexMaps = new int[fields.Length];
                names = fields;

                // 对照
                for (int i = 0; i < fields.Length; i++)
                {
                    string key = fields[i].ToLower();

                    if (dict.ContainsKey(key))
                        indexMaps[i] = dict[key];
                    else
                        indexMaps[i] = -1;
                }
            }
        }

        public bool Read()
        {
            if (reader.Read())
            {
                ReadCount++;

                return true;
            }
            else
                return false;
        }

        private void ResetMap()
        {
            // 创建默认映射
            indexMaps = new int[reader.FieldCount];
            names = new string[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; i++)
            {
                indexMaps[i] = i;
                names[i] = reader.GetName(i);
            }
        }
    }

    /// <summary>
    /// 数据封装接口
    /// </summary>
    public interface IDataWrapper
    {
        /// <summary>
        /// 字段个数
        /// </summary>
        int FieldCount { get; }

        /// <summary>
        /// 成功读取记录数
        /// </summary>
        int ReadCount { get; }

        /// <summary>
        /// 关闭并释放资源
        /// </summary>
        void Close();

        /// <summary>
        /// 获取字段名称
        /// </summary>
        /// <param name="i">字段索引</param>
        /// <returns>字段名称；如果操作过字段映射，只返回映射的字段</returns>
        string GetFieldName(int i);

        /// <summary>
        /// 获取字段清单
        /// </summary>
        /// <returns>字段清单；如果操作过字段映射，只返回映射的字段</returns>
        string[] GetFieldNames();

        /// <summary>
        /// 获取字段类型
        /// </summary>
        /// <param name="i">字段索引</param>
        /// <returns>字段类型；如果操作过字段映射，只返回映射的字段</returns>
        Type GetFieldType(int i);

        /// <summary>
        /// 获取字段值
        /// </summary>
        /// <param name="i">字段索引</param>
        /// <returns>字段值；如果操作过字段映射，只返回映射的字段</returns>
        object GetValue(int i);

        /// <summary>
        /// 根据原字段名称获取字段值
        /// </summary>
        /// <param name="field">字段名称</param>
        /// <returns>字段值</returns>
        object GetValueByOriName(string field);

        /// <summary>
        /// 映射外部字段，以便基于外部字段索引、名称快速读取数据
        /// </summary>
        /// <param name="fields">外部字段清单，不区分大小写</param>
        void MapFields(string[] fields);

        /// <summary>
        /// 读取一行
        /// </summary>
        /// <returns>成功读取则返回 true，否则返回 false</returns>
        bool Read();
    }
}
