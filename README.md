# Data Preparation

This section describes a fully automated process of continuously aggregating the streaming weather data within a sliding window, and saving these data to a file.  Once new data are transmitted, they are processed and appended to a file.  Thus, the file is being continuously updated with new data.

Data extraction process is described in the [previous section](https://github.com/eagronin/weather-streaming-spark-acquire).

In the [previous section](https://github.com/eagronin/weather-streaming-spark-acquire), we have already started processing the data transmitted by the weather station when we extracted the average wind direction from each line obtained from the station's sensors.  We would like to futher process the data on average wind direction to find the minimum and maximum values in our 10-second window. The following function prints all the values of average wind direction within a window along with the minimum and maximum of these values to the screen and outputs the same values to a file:

```python
def stats(rdd):
    
    print(rdd.collect())
    print(rdd.collect(), file = open('/Users/eagronin/Documents/Data Science/Portfolio/Project Output/Spark Output/wind_direction_streaming.txt', 'a'))
        
    if rdd.count() > 0:
        print("max = {}, min = {}".format(rdd.max(), rdd.min()))
        print("max = {}, min = {}".format(rdd.max(), rdd.min()), file = open('/Users/eagronin/Documents/Data Science/Portfolio/Project Output/Spark Output/wind_direction_streaming.txt', 'a'))
```

The first `print()` command in the function above outputs the contents of the RDD (which, in our case, are the observations on average wind direction collected during a 10-second interval) into the screen, while the second `print()` command outputs the same contents into a file `wind_direction_streaming.txt`.  The command below opens the file for writing:
        
```python        
file = open('/Users/eagronin/Documents/Data Science/Portfolio/Project Output/Spark Output/wind_direction_streaming.txt', 'w')
```

Next, we call the stats() function for each RDD in our sliding window:

```python
window.foreachRDD(lambda rdd: stats(rdd))
```

All the calls to the `stats()` function append (rather than overwirte) new data to `wind_direction_streaming.txt`.

We begin the process of reading and processing the data from the weather station by calling `start()` on the `StreamingContext`:

```python
ssc.start()
```

The output starts appearing on the screen and is being written to the file `wind_direction_streaming.txt`:

```
[295, 294, 294, 293]
max = 295, min = 293
[295, 294, 294, 293, 293, 293, 292, 291, 290]
max = 295, min = 290
[293, 293, 292, 291, 290, 292, 291, 289, 287, 286]
max = 293, min = 286
[292, 291, 289, 287, 286, 285, 285, 285, 285, 288]
max = 292, min = 285
[285, 285, 285, 285, 288, 287, 289, 293, 297, 300]
max = 300, min = 285
...
```

As discussed in the [previous section](https://github.com/eagronin/weather-streaming-spark-acquire), the sliding window contains ten seconds worth of data and slides every five seconds. In the beginning, the number of values in the windows are increasing as the data accumulates.  After the third window, the size stays approximately the same. Because the window slides half as often as the size of the window, the second half of a window becomes the first half of the next window. For example, the second half of the fourth window is 285, 285, 285, 285, 288, which becomes the first half of the fifth window.

We stop the process of reading the data from the weather station and wrtiting them to the file `wind_direction_streaming.txt` by calling `stop()` on the `StreamingContext`:

```python
ssc.stop()
```

Previous step: [Data Acquisition](https://github.com/eagronin/weather-streaming-spark-acquire)
