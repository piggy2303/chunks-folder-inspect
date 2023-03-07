# Tools for inspecting Loki chunks

This tool can parse Loki chunks and print details from them. Useful for Loki developers.

To build the tool, simply run `go build` in this directory. Running resulting program with chunks file name gives you some basic chunks information:

```shell
$ ./chunks-inspect YOUR_FOLDER > output.csv
```
