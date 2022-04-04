# kafka go client

因为kafka中涉及到选举的地方有多处，最常提及的也有：①cotroller选举 、 ②分区leader选举 和 ③consumer group leader的选举。我们在前面说过同一个partition有多个副本，其中一个副本作为leader，其余的作为follower。这里我们再说一个角色：controller！kafka集群中多个broker，有一个会被选举为controller，注意区分两者，一个是broker的leader，我们称为controller，一个是分区副本的leader，我们称为leader。

① controller的选举【broker的leader】

controller的选举是通过broker在zookeeper的"/controller"节点下创建临时节点来实现的，并在该节点中写入当前broker的信息 {“version”:1,”brokerid”:1,”timestamp”:”1512018424988”} ，利用zookeeper的强一致性特性，一个节点只能被一个客户端创建成功，创建成功的broker即为controller，即"先到先得"。 

当controller宕机或者和zookeeper失去连接时，zookeeper检测不到心跳，zookeeper上的临时节点会被删除，而其它broker会监听临时节点的变化，当节点被删除时，其它broker会收到通知，重新发起controller选举。

② leader的选举【分区副本的leader】

分区leader的选举由 controller 负责管理和实施，当leader发生故障时，controller会将leader的改变直接通过RPC的方式通知需要为此作出响应的broker，需要为此作出响应的broker即该分区的ISR集合中follower所在的broker，kafka在zookeeper中动态维护了一个ISR，只有ISR里的follower才有被选为Leader的可能。

具体过程是这样的：按照AR集合中副本的顺序 查找到 第一个 存活的、并且属于ISR集合的 副本作为新的leader。一个分区的AR集合在创建分区副本的时候就被指定，只要不发生重分配的情况，AR集合内部副本的顺序是保持不变的，而分区的ISR集合上面说过因为同步滞后等原因可能会改变，所以注意这里是根据AR的顺序而不是ISR的顺序找。

※ 对于上面描述的过程我们假设一种极端的情况，如果partition的所有副本都不可用时，怎么办？这种情况下kafka提供了两种可行的方案：

1、选择 ISR中 第一个活过来的副本作为Leader；

2、选择第一个活过来的副本（不一定是ISR中的）作为Leader；

这就需要在可用性和数据一致性当中做出选择，如果一定要等待ISR中的副本活过来，那不可用的时间可能会相对较长。选择第一个活过来的副本作为Leader，如果这个副本不在ISR中，那数据的一致性则难以保证。kafka支持用户通过配置选择，以根据业务场景在可用性和数据一致性之间做出权衡。

③消费组leader的选举

组协调器会为消费组（consumer group）内的所有消费者选举出一个leader，这个选举的算法也很简单，第一个加入consumer group的consumer即为leader，如果某一时刻leader消费者退出了消费组，那么会重新 随机 选举一个新的leader。
————————————————
版权声明：本文为CSDN博主「Felix-Yuan」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/yuanlong122716/article/details/105162293