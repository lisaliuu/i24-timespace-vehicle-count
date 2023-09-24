# I24 Timespace Vehicle Count

## 3 methods for counting unique vehicles within a window.

This Python module is developed for the [I24-MOTION testbed](https://i24motion.org/) data processing pipeline. The module contains 3 different ways to count the number of unique vehicles within a time-space grid from spatio-temporal data stored in MongoDB. The methods leverage varying amounts of processing in Python and using the MongoDB aggregation pipeline.

* Method 1: Entirely in Python
* Method 2: Combination of Python and MongoDB aggregation pipeline
* Method 3: Entirely using Mongodb aggregation pipeline

#### Configuration
* MongoDB connection parameters referenced from `config.json`
