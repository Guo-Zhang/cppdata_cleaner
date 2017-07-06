# Tmall Data Cleaning

## Steps

1. 整理成如下目录：
```
- tmall_source
  - 2016-05
    - 2016-05-01
    - ...
```

2. 按月和关键词提取，整理成如下文件：
```
- tmall_classified
  - 2016-05
    - 2016-05-some_keyword_and_parameters
```

3. 读入MongoDB数据库（新硬盘，单独存，制定标准）
```
- tmall
  - tmall
  - ...
- jd
  - jd
```

## Reference
- https://github.com/Guo-Zhang/online_price_patterns/blob/master/prepare_data/csv2mongo.py
- https://github.com/Guo-Zhang/online_price_patterns/blob/master/prepare_data/pre_mongodata.py

