# 写入测试
write文件夹中模拟下面2个场景进行写入测试
### 场景1：telegraf插入
   - 数据类型为cpu机器指标，采集间隔为10s，数据量可配置
   - 写入方式为多线程批量写入，worker个数和batchSize均可配置

测试方式：执行write_test.go 中的 TestSimulate方法，耗时会在log中输出
`go test -timeout 160s -run ^TestSimulate$ ivst/write -v`
结果：
| 数据量 | worker数 | batchSize | influx耗时 | tdEngin耗时 |
| - | - | - | - | - |
| 10,000 | 5 | 100 | 3568ms | 9630ms |
| 100,000 | 5 | 100 | 5283ms | 6645ms |
| 1,000,000 | 10 | 1000 | 61.64s | 60.49s |

### 场景2：日志写入
   - 数据类型为cpu机器指标
   - 写入方式为单线程单条写入

测试方式：执行write_test.go 中的 TestSimulate方法，耗时会在log中输出
`go test -timeout 160s -run ^TestSimulate$ ivst/write -v`
结果：
| 数据量 | influx耗时 | tdEngin耗时 |
| - | - | - |
| 1,000 | 1997ms | 3712ms |
| 10,000 | 19.66s | 27.39s |

### tdengine 出现稳定性问题