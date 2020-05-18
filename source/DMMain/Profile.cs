using JHWork.DataMigration.Common;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.IO;

namespace JHWork.DataMigration
{
    /// <summary>
    /// 执行模式
    /// </summary>
    internal enum RunModes
    {
        Once,      // 单次
        Daily,     // 每日
        Continuous // 持续
    }

    /// <summary>
    /// 持续运行间隔
    /// </summary>
    class RunInterval
    {
        public int Hours { get; set; }   // 小时数
        public int Minutes { get; set; } // 分钟数
        public int Seconds { get; set; } // 秒数
    }

    /// <summary>
    /// 配置类
    /// </summary>
    class Profile
    {
        /// <summary>
        /// 回调定义
        /// </summary>
        public delegate void CallbackFunc();

        /// <summary>
        /// 回调函数
        /// </summary>
        public CallbackFunc Callback { get; set; } = null;

        /// <summary>
        /// 实例列表
        /// </summary>
        public Instance[] Instances { get; private set; }

        /// <summary>
        /// 执行间隔
        /// </summary>
        public RunInterval Interval { get; } = new RunInterval();

        /// <summary>
        /// 执行模式
        /// </summary>
        public RunModes Mode { get; private set; } = RunModes.Once;

        /// <summary>
        /// 执行器分析器
        /// </summary>
        public IRunnerAnalyzer Analyzer { get; private set; } = null;

        /// <summary>
        /// 执行器
        /// </summary>
        public IRunnerExecutor Executor { get; private set; } = null;

        /// <summary>
        /// 执行器名称
        /// </summary>
        public string Runner { get; private set; } = "";

        /// <summary>
        /// 执行时间
        /// </summary>
        public DateTime RunTime { get; private set; } = DateTime.Today.AddDays(1);

        /// <summary>
        /// 实例并发线程数
        /// </summary>
        public uint Threads { get; private set; }

        private string path = "";

        private void AnalyseMode(JObject obj)
        {
            string mode = obj["mode"].ToString().ToUpper();

            Mode = "DAILY".Equals(mode) ? RunModes.Daily : "CONTINUOUS".Equals(mode) ? RunModes.Continuous
                : RunModes.Once;
        }

        private void AnalyseRunner(JObject obj)
        {
            Runner = obj["runner"].ToString();

            Executor = RunnerFactory.GetRunnerExecutorByName(Runner);
            Analyzer = RunnerFactory.GetRunnerAnalyzerByName(Runner);

            if (Executor == null || Analyzer == null)
                throw new Exception($"执行器 {Runner} 不支持！");
        }

        private void AnalyseRunTime(JObject obj)
        {
            if (Mode == RunModes.Daily)
            {
                string rt = obj["runtime"].ToString();
                DateTime dt = DateTime.Today.AddHours(int.Parse(rt.Substring(0, 2)))
                    .AddMinutes(int.Parse(rt.Substring(3, 2)));

                if (rt.Length >= 8)
                    dt.AddSeconds(int.Parse(rt.Substring(6, 2)));

                if (dt < DateTime.Now) dt.AddDays(1);

                RunTime = dt;
            }
            else if (Mode == RunModes.Continuous)
            {
                string rt = obj["runtime"].ToString();

                Interval.Hours = int.Parse(rt.Substring(0, 2));
                Interval.Minutes = int.Parse(rt.Substring(3, 2));
                Interval.Seconds = int.Parse(rt.Substring(6, 2));

                RunTime = DateTime.Now.AddHours(Interval.Hours).AddMinutes(Interval.Minutes)
                    .AddSeconds(Interval.Seconds);
            }
        }

        private void AnalyseThreads(JObject obj)
        {
            Threads = uint.Parse(obj["threads"].ToString());
        }

        /// <summary>
        /// 加载配置
        /// </summary>
        /// <param name="file">配置文件</param>
        public void Load(string file)
        {
            JObject obj = LoadAndDeserialize(file);

            path = Path.GetDirectoryName(file);

            // 执行模式
            AnalyseMode(obj);

            // 执行器
            AnalyseRunner(obj);

            // 执行时间
            AnalyseRunTime(obj);

            // 线程数
            AnalyseThreads(obj);

            // 实例
            Instances = Analyzer.AnalyseInstance(obj["instances"] as JArray, obj["inherited"] as JObject, path);
        }

        private JObject LoadAndDeserialize(string file)
        {
            return JsonConvert.DeserializeObject(File.ReadAllText(file)) as JObject;
        }

        /// <summary>
        /// 重置状态
        /// </summary>
        public void Reset()
        {
            // 重置执行时间
            if (Mode == RunModes.Daily)
                while (RunTime < DateTime.Now)
                    RunTime = RunTime.AddDays(1);
            else if (Mode == RunModes.Continuous)
                if (RunTime < DateTime.Now)
                    RunTime = DateTime.Now.AddHours(Interval.Hours).AddMinutes(Interval.Minutes)
                        .AddSeconds(Interval.Seconds);

            // 重置实例状态
            foreach (Instance ins in Instances)
                Analyzer.Reset(ins);
        }
    }
}
