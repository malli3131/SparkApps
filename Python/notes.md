

```python
import findspark
```


```python
findspark.init()
```


```python
import pyspark
```


```python
sc = pyspark.SparkContext()
```


```python
sc.version
```




    u'2.2.0'




```python
data = sc.parallelize([1,2,3,4,5,6])
mydata = data.map(lambda num : num * 10)
```


```python
mydata.collect()
```




    [10, 20, 30, 40, 50, 60]




```python

```
