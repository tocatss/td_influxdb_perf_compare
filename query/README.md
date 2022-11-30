# 查询测试
query文件夹中模拟下列几个场景进行测试。

### 数据准备
测试表"cpu"为模拟telegraf定期采集的机器cpu(12核，机器数26)指标，

线上的采集周期为10s一次，保留时长为15天，数据量较大，使用同等规模数据测试周期较长，于是将保留时长缩短为10分钟，其余参数不变来作为测试数据集。

datagen文件夹中，提供了几种数据生成方式，
比如方法TestSimulatorCSV中的测试用例" typeFlagCommonOne",可以模拟生成10分钟的cpu.csv文件，之后通过 telegraf 可以将csv导入到influx和tdengine中，
详见dataload和datagen中的README。

### 场景1：指定tag查询，SELECT * FROM cpu WHERE cpu="randomCPU" and host="randomHost" ORDER BY time DESC LIMIT N

执行100次的平均结果为：
| LIMIT N | influx耗时 | tdEngin耗时 |
| - | - | - |
| 1| 7ms | 26ms |
| 12 | 57ms | 77ms |

### 场景2：按tag分类，SELECT * FROM cpu GROUP BY cpu,host ORDER BY time DESC LIMIT 1

执行100次的平均结果为：
| influx耗时 | tdEngin耗时 |
| - | - |
| 73ms | 175ms |

### 场景3：按时间聚合，SELECT max FROM cpu WHERE cpu="randomCPU" AND host="randomHost" and time IN random1Hour GROUP BY interval(Nm)

执行100次的平均结果为：
| interval(Nm) | influx耗时 | tdEngin耗时 |
| - | - | - |
| interval(1m) | 3ms | 18ms |
| interval(10m) | 2ms | 23ms |


### 问题：
无法对分表中的结果集进行排序，已提issue

How to sort results in ascending or descending order in PARTITION BY clause 
https://github.com/taosdata/TDengine/issues/16745