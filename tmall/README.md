# Tmall Data Cleaning

## Package Document

  * Project name: datacleaning/tmall
  * Project version: 0.1
  * Author: Guo Zhang
  * Contributer:
  * Created Date: 2016-10-17 
  * Updated Date: 2016-12-11
  * Python version: 2.7.9
  * Descrption: This is a Tmall data cleaning sub-repository for China's Prices Project


## Package Structure

- pyfiles
  * tools.py (some tools for data cleanning)
  * csv_reorg.py (reorganize the source csv files to formal format for China's Prices Project)
  * var_extraction.py (extract variables from formal data)

- lists
  * tmall_kwd2cat (a list of category name, category id and keyword)
  * map_dict.py (a map from category id to category name)
  * cat_id2name (a map from category name to category id)


## Processes

1. csv_reorg.py
2. var_extraction.py


## Requirements

* Python 2.7.9
* gevent   

    
## Changelog

* Version 0.1(2016-10-17)
  * create the repository
