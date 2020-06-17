using JHWork.DataMigration.Common;

namespace SampleFilter
{
    /// <summary>
    /// 数据过滤器示例
    /// </summary>
    public class SampleFilter : IDataFilter, IAssemblyLoader
    {
        public string GetName()
        {
            return "Sample";
        }

        public object GetValue(IDataWrapper data, int fieldIndex, string fieldName)
        {
            switch (fieldName.ToLower())
            {
                default:
                    return data.GetValue(fieldIndex);
            }
        }
    }
}
