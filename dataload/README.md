1. 数据生成模拟问题：尽量真实，降低时间
2. 大文件导入问题:telegraf,tdengine,influxdb无命令
3. influx 导入命令 
   1. python -m ensurepip --upgrade 
   2. python -m pip install -r requirement.txt
   3. python ./csv-to-influxdb.py --dbname csv -m cpu -tc time --tagcolumns host,cpu --fieldcolumns usage_guest,usage_guest_nice,usage_idle,usage_iowait,usage_irq,usage_nice,usage_softirq,usage_steal,usage_system,usage_user -i ../ivst/asset/common/cpu.csv -tf '%Y-%m-%d %H:%M:%S.%f'

4. 数据存储结构原理文件位置
5. dbschema
```
CREATE DATABASE `csv` BUFFER 96 CACHEMODEL 'none' COMP 2 DURATION 3600d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 KEEP 5256000m,5256000m,5256000m PAGES 256 PAGESIZE 4 PRECISION 'ns' REPLICA 1 STRICT 'off' WAL_LEVEL 1 VGROUPS 2 SINGLE_STABLE 0
```
6. telegraf导入csv
   