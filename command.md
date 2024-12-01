# raft
// 进入路径
cd src/raft


// 调试用，检测数据竞争问题
go test -race

// 测试
go test
go test -race -run 2A
go test -run 2B

# map reduce
cd 6.824/src/main

// 删除文件
rm mr-out*

// 编译工具
go build -race -buildmode=plugin ../mrapps/wc.go

// 运行协调者
go run -race mrcoordinator.go pg-*.txt


// 运行工作者
go run -race mrworker.go wc.so


// 测试
bash test-mr.sh


bash test-d5error.sh
