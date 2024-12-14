# Legend
* zgh
    * @278395****
* D5
    * @124705****

# 6.824
## Raft
* 英文链接：http://nil.csail.mit.edu/6.824/2022/labs/lab-raft.html
* 中文链接：https://zhuanlan.zhihu.com/p/248686289 
* 指导：https://thesquareplanet.com/blog/students-guide-to-raft/
* 动画：http://thesecretlivesofdata.com/raft/

![alt text](assets/image.png)

### 2A
1. 实现计时器的重置和暂停
    ```go
    package main

    import (
        "time"
    )

    type Raft struct {
        resetTickerChan  chan bool
        isTimeoutChan    chan bool
    }

    func (rf *Raft) ticker() {
        for {
            timeout := 3 * time.Second
            println("\n\n新的一轮倒计时")
            select {
            case <-rf.resetTickerChan:
                println("接收到重置信号")
                continue // 接收到重置计时信号


            case <-time.After(timeout):
                println("超时了，计时器暂停") // 计时器超时，进入阻塞状态，再次收到重置计时信号则重新开始计时
                select {
                case <-rf.resetTickerChan:
                    println("暂停的计时器重新开始计时")
                    continue
                }
            }
        }
    }

    func main() {
        rf := &Raft{}
        rf.isTimeoutChan = make(chan bool)
        rf.resetTickerChan = make(chan bool)
        go rf.ticker()

        // 发送重置信号
        time.Sleep(1 * time.Second)
        rf.resetTickerChan <- true

        // 超时
        time.Sleep(4 * time.Second)
        rf.resetTickerChan <- true

        // 发送重置信号
        time.Sleep(7 * time.Second)
        rf.resetTickerChan <- true

        // 发送重置信号
        time.Sleep(1 * time.Second)
        rf.resetTickerChan <- true

        time.Sleep(100 * time.Second)
    }
    ```
### 2B
### 2C
### 2D
