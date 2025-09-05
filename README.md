# Database shootout

For filesystem metadata loads.

```
cpu: Apple M4
BenchmarkCreateFolderBadger-10                26          39618575 ns/op        97883766 B/op      22860 allocs/op
BenchmarkLookupBadger-10                 1080279              1124 ns/op            1006 B/op         18 allocs/op
BenchmarkReaddirBadger-10                    174           5780074 ns/op         2585608 B/op       4651 allocs/op
BenchmarkCreateFolderBolt-10                  51          21298930 ns/op          884118 B/op      11940 allocs/op
BenchmarkLookupBolt-10                   2729758               412.5 ns/op           566 B/op         10 allocs/op
BenchmarkReaddirBolt-10                   121028              9636 ns/op            1072 B/op          8 allocs/op
BenchmarkCreateFolderSqlite-10               745           1584026 ns/op          108748 B/op       3863 allocs/op
BenchmarkCreateFolderCDB-10                 3296            423866 ns/op          306312 B/op       8434 allocs/op
BenchmarkLookupSqlite-10                  243204              4143 ns/op             438 B/op          8 allocs/op
BenchmarkLookupCDB-10                    1000000              1224 ns/op             199 B/op          6 allocs/op
BenchmarkLookupCDB64-10                 14693292                81.12 ns/op           86 B/op          2 allocs/op
BenchmarkReaddirSqlite-10                  10000            107741 ns/op           24757 B/op       1793 allocs/op
BenchmarkReaddirCDB-10                    206308              5914 ns/op            4472 B/op          5 allocs/op
BenchmarkReaddirCDB64-10                  147753              8026 ns/op            5192 B/op          5 allocs/op
BenchmarkCreateFolderPebble-10                21          53636655 ns/op          544933 B/op       3215 allocs/op
BenchmarkLookupPebble-10                 3375397               335.0 ns/op           102 B/op          3 allocs/op
BenchmarkReaddirPebble-10                   5658            211391 ns/op          483097 B/op        334 allocs/op
```