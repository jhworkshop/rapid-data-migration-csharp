# 快速数据迁移
顾名思义，这是一个将数据从一个数据库迁移到另一个数据库的工具。与很多同类型工具相比，它主打的特点是“快”！
没错，就是
### “快”！

## 为什么“快”？
要解释这个问题，首先要说说数据迁移是什么一回事。

数据迁移通常是这样子的：

![](https://ebuy.ucoz.com/rdm/seq1.png)

如果通用些，可能就类似这个样子了：

![](https://ebuy.ucoz.com/rdm/seq2.png)

这是一个串行处理的过程，提取数据的时候，目标库空闲着，写入数据的时候，源库空闲着，处理能力白白浪费了。
可见，只要将这个串行处理改为并行，数据迁移效率即可大大提高。

![](https://ebuy.ucoz.com/rdm/seq3.png)

在某些测试场景下，仅这项小小的改动，就可以提升 30% 的迁移效率，没写错，30%，而已（因为是未经严谨优化的测试场景，就不具体展开介绍了）！

## 制约迁移效率的其它主要因素
除了执行机制的制约以外，还有其它一些制约因素会明显影响迁移效率，本工具对此都有所考量。

### 1、网络传输速度
显然，如果只有一根小水管，算法层面优化能带来的提升空间就不大了，不过也不是什么事情都不能做，比如 MySQL 的客户端就支持压缩传输协议，只需一个连接串的配置项，就能用 CPU 运算能力换来一定的效率提升。

### 2、中间格式编码解码
对于立足“通用”的解决方案，在 reader 和 writer 之间引入中间数据格式貌似无可避免，这令中间格式的选择变得非常重要，JSON，或者 XML，通用性无可置疑，编码解码代价却不小。其实对于特定场景，存在更高效的中间格式。

### 3、目标库写入效率
如果目标库客户端支持 Bulk Copy，使用批量复制接口，写入性能自然更佳，在某些测试场景下，批量复制比对数据脚本，效率可提升 20 倍，没写错，是 20 倍（因为是未经严谨优化的测试场景，就不点名了）！

这也是为什么优先选择 C# 的原因，无它，支持更佳而已。

## 其它重要特性
除了主打的性能特性外，本工具还有一些有意思的特性。

### 1、接口化
定义了一系列的功能接口，只需实现相应接口，即可实现扩展功能。主要接口如下：

| 接口 | 功能说明 |
| ---- | -------- |
| IDataFilter | 数据 filter 接口，用于源表和目标表非完全匹配的场景 |
| IDBMSReader | 数据 reader 接口 |
| IDBMSWriter | 数据 writer 接口 |
| IRunnerAnalyzer | 解析器接口，用于解析配置 |
| IRunnerExecutor | 执行器，执行数据任务 |

### 2、支持超大数据集
简单的实现，分批读取、写入而已。

### 3、配置助手
虽然目前只是一个简单粗暴的实现，用来帮助了解配置文件格式还是可以的，而且还实现了一个很不错的功能：解决表之间的外键依赖关系。关系数据库迁移数据，最麻烦之处是表与表之间可能存在外键约束，导致对迁移顺序有要求。配置助手能分析出这个先后依赖关系来。

## 授权
MIT License

Copyright (c) 2020 jhworkshop

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

### 写在最后
虽然完成度颇高，本质上它只是一个实验性项目，代码即文档，而且它实现的是一种解决思路，而不是解决方案，应用须谨慎。
