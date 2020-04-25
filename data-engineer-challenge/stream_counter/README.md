# Stream Counter

## Building
The project is structured as a Java Maven project. The requirements are JAVA 8 and a recent maven version. I have tested
it with
* openjdk 8
* maven 3.6.3

In order to build simply execute
```
./build.sh
```

## Running
Similarly the executable can be run as a maven executable. A convenience script `run.sh` is provided which passes all the
command-line arguments to the java executable. In order to get the usage text run
```
./run.sh --help
```

To run the executable in benchmark mode with default parameters, you can use
```
./run.sh --benchmark
```

One minor caveat that I didn't have time to fix is that the process doesn't exit after being interrupted with Ctrl-C.
Therefore, you will need to shut down the shell that spawned the executable to shut it off.

## Output Structure
The program outputs its unique user count structure every 5 seconds by default. Every minute is given
on a separate line along with its unique user count. In order to terminate the process, you can use Ctrl-C
which will also print the benchmarking statistics at the end.
