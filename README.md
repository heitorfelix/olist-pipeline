# olist-pipeline

## How to define new tables

1. Create table schema json file on **bronze**
2. Create sql query file on **silver**
3. Create bronze job with no dependence
4. Create silver job depending on bronze job
5. ...
6. Commit changes
