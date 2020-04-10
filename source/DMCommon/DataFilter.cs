using System;

namespace JHWork.DataMigration.Common
{
    /// <summary>
    /// 数据过滤器接口，当目标表与源表字段不是简单一一对应时，可用数据过滤器做数据转换
    /// </summary>
    public interface IDataFilter
    {
        /// <summary>
        /// 获取原始字段值
        /// </summary>
        /// <param name="data">数据</param>
        /// <param name="fieldIndex">字段索引</param>
        /// <param name="fieldName">字段名称</param>
        /// <returns>原始字段值</returns>
        object GetValue(IDataWrapper data, int fieldIndex, string fieldName);
    }

    /// <summary>
    /// 默认过滤器
    /// </summary>
    public class DefaultDataFilter : IDataFilter
    {
        public object GetValue(IDataWrapper data, int fieldIndex, string fieldName)
        {
            return data.GetValue(fieldIndex);
        }
    }

    /// <summary>
    /// 数据过滤器工厂
    /// </summary>
    public class DataFilterFactory
    {
        private static readonly AssemblyLoader<IDataFilter> loader = new AssemblyLoader<IDataFilter>(
            AppDomain.CurrentDomain.BaseDirectory + "Filter", "IDataFilter");
        public static readonly DefaultDataFilter filter = new DefaultDataFilter();

        /// <summary>
        /// 按名称获取数据过滤器
        /// </summary>
        /// <param name="name">名称</param>
        /// <returns>数据过滤器</returns>
        public static IDataFilter GetFilterByName(string name)
        {
            IDataFilter dataFilter = loader.GetInstanceByName(name);

            if (dataFilter == null)
                return filter;
            else
                return dataFilter;
        }
    }
}
