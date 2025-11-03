The __LineReader__ reads the lines and writes them to the entire column: `values[columnIndex] = line`

For example if you have a file with the following contents:
```
    row1
    row2
    row3
```

Then it will be written in 3 `changeitem` with values row1, row2, row3
