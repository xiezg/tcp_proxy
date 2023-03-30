# tcp_proxy
epoll tcp python proxy 

tcp转发 监听端口 7777，并将tcp流量转发到 target_addr:target_port
使用一个epollfd + 多线程。

## epoll
#### Q1、 EPOLLIN触发条件
1. epoll_ctl()注册EPOLLIN时，读缓冲区有数据。
2. 对端调用send,导致读缓冲区发生变化。
#### Q2、 EPOLLOUT触发条件。
1. epoll_ctl()注册EPOLLOUT时，写缓冲区可写。
2. 写缓冲区由满变为空时。
#### Q3、 epll_wait多线程调用
1. 多线程调用epoll_wait，同一个socket的事件，会同时在多个线程返回，导致多个线程操作同一个socket。
#### Q4、 EPOLLONTSHOT
1. 当事件触发后，socket不会再次触发，即使状态发生变化。这个模式可以在多线程模式时使用。
#### Q5、 epoll边缘触发
1. 该模式下，注册的事件触发后，后续的事件不再触发，除非socket的状态再次发生变化，比如对端再次调用send,则注册的EPOLLIN事件会再次触发。
#### Q6、 epoll事件覆盖
1. 多次调用epoll_ctl注册不同的事件，会导致覆盖之前注册的事件。
#### Q7、 epoll框架下的sockt send
1. 当socket设置为非阻塞模式，没有必要在通过设置EPOLLOUT来触发后，才进行send操作。可以直接操作send，不过也要根据具体业务来进行调整。

# 测试
1. 开启一个echo server
```
socat TCP4-LISTEN:9999,fork EXEC:cat
```
2. 启动tcp_proyx
```
python3 tcp_proxy.py
```
3. 开启客户端
```
telnet x.x.x.x 7777
```
