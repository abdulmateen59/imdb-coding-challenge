
# IMDB Coding Challenge

## Note
Make sure that the correct host is selected in the configuration file. Also, provide the data path for `Movies Title`, `Actors Details`, and `Principals` in the configuration or place the files in already created folders.


## Getting Started
The solution is containerized and automated using Dockers and Make. Use following commands to run the solution.

First, build and start the containers:
```sh
make build
```
Execute the solution:
```sh
make run
```
Execute test cases:
```sh
make run-testcases
```
Prune/Delete containers:
```sh
make stop
```

*`Execution time of the solution is aproximately 02 mins with 04 cores and 12G of memory.`*
</br>*`The distribution graph is stored in resources/graph`*

## Without Docker
To run the solution locally, open config file, comment out line `07` and remove the comment character from line `08`.
```sh
python runner.py --Remote False
```


## Approach Used
A top-down approach is used in combination with divide-and-conquer. First, the larger data frame is selected and reduced using the filters, then the calculations are performed on the reduced data to achieve better performance.



---
For more samples of my work, please visit [GitHub](https://github.com/abdulmateen59)

Email :[abdul.mateen59@yahoo.com](abdul.mateen59@yahoo.com)
