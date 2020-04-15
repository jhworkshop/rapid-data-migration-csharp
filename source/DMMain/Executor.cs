using JHWork.DataMigration.Common;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace JHWork.DataMigration
{
    /// <summary>
    /// 执行器状态
    /// </summary>
    public enum ExecutorState
    {
        Idle,    // 空闲
        Testing, // 检测中
        Running, // 执行中
        Planning // 计划中
    }

    /// <summary>
    /// 执行器
    /// </summary>
    class Executor : IStopStatus
    {
        /// <summary>
        /// 停止执行标志
        /// </summary>
        private bool stopped = false;

        /// <summary>
        /// 执行器状态
        /// </summary>
        public ExecutorState State { get; private set; } = ExecutorState.Idle;

        public bool IsStopped()
        {
            return stopped;
        }

        private void InternalRunWithCallback(object obj)
        {
            Profile profile = obj as Profile;

            try
            {
                if (profile.Mode == RunModes.Once)
                    RunOnce(profile);
                else
                    RunTimes(profile);
            }
            finally
            {
                State = ExecutorState.Idle;
                profile.Callback?.Invoke();
            }
        }

        private void InternalTest(Profile profile)
        {
            Parallel.ForEach(profile.Instances, new ParallelOptions() { MaxDegreeOfParallelism = (int)profile.Threads },
                ins =>
            {
                profile.Executor.Prefetch(ins, this);
            });
        }

        private void InternalTestWithCallback(object obj)
        {
            Profile profile = obj as Profile;

            try
            {
                InternalTest(profile);
            }
            finally
            {
                State = ExecutorState.Idle;
                profile.Callback?.Invoke();
            }
        }

        /// <summary>
        /// 执行迁移
        /// </summary>
        /// <param name="profile">配置</param>
        public void Run(Profile profile)
        {
            stopped = false;

            profile.Reset();
            new Thread(InternalRunWithCallback).Start(profile);
        }

        private void RunOnce(Profile profile)
        {
            State = ExecutorState.Running;
            Logger.SetRptFile($"Report-{DateTime.Now:yyMMddHHmm}.csv");
            profile.Reset();
            InternalTest(profile);

            Parallel.ForEach(profile.Instances, new ParallelOptions() { MaxDegreeOfParallelism = (int)profile.Threads },
                ins =>
            {
                profile.Executor.Execute(ins, this);
            });
        }

        private void RunTimes(Profile profile)
        {
            State = ExecutorState.Planning;

            while (!stopped)
            {
                Thread.Sleep(1);
                if (DateTime.Now >= profile.RunTime)
                {
                    RunOnce(profile);
                    State = ExecutorState.Planning;
                }
            }
        }

        /// <summary>
        /// 停止执行
        /// </summary>
        public void Stop()
        {
            stopped = true;
        }

        /// <summary>
        /// 检测配置
        /// </summary>
        /// <param name="profile">配置</param>
        public void Test(Profile profile)
        {
            stopped = false;
            State = ExecutorState.Testing;

            profile.Reset();
            new Thread(InternalTestWithCallback)
            {
                IsBackground = true
            }.Start(profile);
        }
    }
}
