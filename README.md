a sample code for kvt

install
========
```
git clone https://github.com/simpleKV/kvtsample.git
cd kvtsample

go get github.com/simpleKV/kvt
go get go.etcd.io/bbolt
go get github.com/redis/go-redis/v9
```
then build you project with One driver build tag: boltdb/buntdb
```
go build -tags boltdb   //build with boltdb, if you want use BoltDB
go build -tags redis    //build with redis  if you want use Redis
```

enjoy kvt !
