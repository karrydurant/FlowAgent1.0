# 八股

## 线程池

### 什么是线程池

就是管理一系列线程的资源池，当有任务要处理时，直接从线程池中获取线程来处理，处理完之后线程并不会被立即销毁，而是等待下一个任务

### 为什么要用线程池

1.降低资源消耗：线程池里的线程是可以重复利用的，避免了频繁创建和销毁线程带来的开销

2.提高响应速度：任务来了之后可以直接交给那些已经存在的、空闲的线程去执行，省去了创建线程的时间，任务能够更快地得到处理。

3.更好地管理：我们可以配置线程池的大小（核心线程数、最大线程数）、任务队列的类型和大小、拒绝策略等。

### 如何创建线程池

通过 ThreadPoolExecutor 构造函数直接创建

```java
ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
    2, 5, 10L, TimeUnit.SECONDS,
    new ArrayBlockingQueue<>(10),
    threadFactory,   //线程工厂
    new ThreadPoolExecutor.AbortPolicy()
);
```

### 线程池的拒绝策略有哪些

如果当前同时运行的线程数量达到最大线程数量并且队列也已经被放满了任务时，需要拒绝策略。

1.AbortPolicy：抛出 RejectedExecutionException 异常拒绝处理

2.CallerRunsPolicy：调用执行者自己的线程运行任务，如果执行程序已关闭，则会丢弃该任务。

3.DiscardPolicy：不处理新任务，直接丢弃掉。

4.DiscardOldestPolicy：此策略将丢弃最早的未处理的任务请求。

### 使用 CallerRunsPolicy 可能会带来什么问题，怎么解决

如果走到 CallerRunsPolicy 的任务是个非常耗时的任务，且处理提交任务的线程是主线程，可能会导致主线程阻塞（线程池nei）。
一定会导致线程池阻塞，进而导致后续任务无法及时执行，严重的情况下很可能导致 OOM。

1.内存允许的情况下，增加阻塞队列 BlockingQueue 的大小并调整堆内存以容纳更多的任务，确保任务能够被准确执行。

2.调整线程池的 maximumPoolSize 参数：

这样可以提高任务处理速度，避免累计在 BlockingQueue 的任务过多导致内存用完。

3.任务持久化：

设计一张任务表将任务存储到 MySQL 数据库中。

Redis 缓存任务。

将任务提交到消息队列中。

### 使用线程池不当导致 OOM 的场景

1.使用无界队列：上游疯狂提交任务，任务对象、任务里携带的参数 / 大对象源源不断塞进队列，线程处理速度赶不上提交速度 → 队列无限膨胀 → 堆 OOM

2.最大线程是 Integer.MAX_VALUE，任务来了就新建线程，每个线程自带栈内存，直接出现 java.lang.OutOfMemoryError: unable to create new native thread （不是堆 OOM，是操作系统线程资源耗尽）

3.搭配 CallerRunsPolicy 时，间接诱发 OOM 的场景：在阻塞前创建大量大对象，阻塞是延长了这些大对象的存活时间，导致更容易产生 OOM
