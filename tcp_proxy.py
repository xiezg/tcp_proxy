####    python 3.8.16
####    URL: https://github.com/xiezg/tcp_proxy

from abc import ABC, abstractmethod
import time
import threading
import sys
import os
import errno
import socket, select
import logging
import traceback

logger = logging.getLogger()

MYSQL_CONN_PENDING   = 0      #接收到客户端的连接请求并就绪后，连接到mysql的请求连接还未就绪,这时只可以从clinet接收数据，但是不可以向数据库发送数据
MYSQL_CONN_READY     = 1      #连接到mysqld的连接已就绪，可以在双方进行数据传输

MY_EPOLL_FLAGS = select.EPOLLERR| select.EPOLLRDHUP | select.EPOLLET | select.EPOLLONESHOT

class ConnShutDown( Exception ):
    pass

class MyNonBlockSocket:
    def __init__(self, epoll, socket ):
        self.__socket=socket
        self.__epoll = epoll
        self.__fd = socket.fileno()
        self.recv_buf = bytearray()
        self.__socket.setblocking( 0 )

    def shutdown(self, flag):
        try:
            self.__socket.shutdown( flag )
        except socket.error as e:
            if e.errno == errno.ENOTCONN:   #发生这个异常的原因：1、当对端已经关闭 2、重复调用
                logger.warning( "fd[{}] shutdown exception:[{}]".format( self.__socket.fileno(), e ) )
                return
            raise e

    def close(self):
        self.__socket.close()

    def connect(self, address):
        return self.__socket.connect( address)

    def fileno(self):
        return self.__socket.fileno()

    def enable_epoll_read(self):
        self.__epoll.modify( self.fileno(), select.EPOLLIN|MY_EPOLL_FLAGS) 

    def send(self, data ):
        try:
            return self.__socket.send( data )
        except BlockingIOError as e:
            logger.debug( e )
            raise e
        except ConnectionResetError as e:
            logger.error(e)
            raise ConnShutDown
        except BrokenPipeError as e:
            logger.error(e)
            raise ConnShutDown
        except ConnectionError as e:
            logger.error(e)
            raise ConnShutDown
        except OSError as e:
            logger.error(e)
            if e.errno == errno.EBADF:
                raise ConnShutDown
            if e.errno == errno.ENOTCONN:
                raise ConnShutDown
            raise e
        except socket.error as e:
            logger.error("send fd[{}], err:[{}]".format(self.__fd, e) )
            raise e

    def recv(self, size ):
        try:
            data = self.__socket.recv(size)
            if len( data ) == 0:
                logger.warning( "recv empty, peer close fd:[{}]".format( self.__socket.fileno() ) )
                raise ConnShutDown
            return data
        except BlockingIOError as e:
            logger.debug(e)
            raise e
        except ConnectionResetError as e:
            logger.error(e)
            raise ConnShutDown
        except ConnectionError as e:
            logger.error(e)
            raise ConnShutDown
        except OSError as e:
            logger.error(e)
            if e.errno == errno.EBADF:
                raise ConnShutDown
            if e.errno == errno.ENOTCONN:
                raise ConnShutDown
            raise e
        except socket.error as e:
            logger.error("recv fd[{}], err:[{}]".format(self.__fd, e) )
            raise e

#接收到数据库，直接执行send，而不在通过EPOLLOUT事件判断写缓冲区是否可写
#当写事件因为BlockingIOError失败后，停止接收数据，并重新注册EPOLLIN事件，等待下一轮epoll_wait触发
#BUG: 当写缓冲区满后并再次可写之前，EPOLLIN事件可能会再次触发，会导致写再次失败,延迟500ms后在注册EPOLLIN事件
def epoll_read_event( src, dst ):
    if len( src.recv_buf) > 0:
        try:
            while len( src.recv_buf )> 0:
                n = dst.send(src.recv_buf)
                src.recv_buf = src.recv_buf[n:]
        except BlockingIOError as e:
            time.sleep(0.5) #避免因dst写缓冲区满导致的频繁send失败
            src.enable_epoll_read()
            return

    try:
        while True:
            data = src.recv(4096)
            try:
                while len(data)>0:
                    n = dst.send( data )
                    data = data[n:]
            except BlockingIOError as e:
                src.recv_buf = data
                src.enable_epoll_read()
                return
    except BlockingIOError as e:
        src.enable_epoll_read()

#socket读事件总是开启，写事件选择性开启
class conn_pair:

    def __init__( self, client_socket, mysqld_socket ):
        #记录下文件号，方便删除
        self.client_socket = client_socket
        self.mysqld_socket = mysqld_socket
        self.__close_lock = threading.Lock()
        self.__ref_count = 2
        self.status = MYSQL_CONN_PENDING    ##没有加锁保护
        self.reset_active_status()
        self.__client_socket_fd = client_socket.fileno()
        self.__mysqld_socket_fd = mysqld_socket.fileno()

    def __str__(self):
        return "client_fd:[{}] mysqld_fd:[{}]".format( self.__client_socket_fd, self.__mysqld_socket_fd )

    def is_active(self):
        return self.__active

    def reset_active_status(self):
        self.__active = False 

    def epoll_in( self, fd):
        self.__active = True

        if self.client_socket != None and fd == self.client_socket.fileno():
            return self.__client_read()

        if self.mysqld_socket != None and fd == self.mysqld_socket.fileno():
            return self.__mysqld_read()

        raise Exception( "invalid fd: {}".format( fd ) )

    def epoll_out( self, fd):
        self.__active = True

        if self.client_socket != None and fd == self.client_socket.fileno():
            return self.__client_write()

        if self.mysqld_socket != None and fd == self.mysqld_socket.fileno():
            return self.__mysqld_write()

        raise Exception( "invalid fd: {}".format( fd ) )

    def clean(self, fd ):
        with self.__close_lock:

            self.__ref_count = self.__ref_count - 1

            if self.__ref_count == 1:
                self.client_socket.shutdown(socket.SHUT_RDWR)
                self.mysqld_socket.shutdown(socket.SHUT_RDWR)
                logger.debug( "shutdown client_socket:[{}] mysqld_socket:[{}]".format(\
                        self.client_socket.fileno(), \
                        self.mysqld_socket.fileno() ))
                return

            if self.__ref_count == 0:
                self.client_socket.close()
                self.client_socket = None
                self.mysqld_socket.close()
                self.mysqld_socket = None
                logger.debug( "close client_socket and mysqld_socket" )
                return

            raise Exception( "invalid refcount:[{}]".format( self.__ref_count) )

            #if self.client_socket != None and fd == self.client_socket.fileno():
            #    logger.info( "close client fd:[{}] connect".format( self.client_socket.fileno()) )
            #    self.client_socket.close()
            #    self.client_socket = None
            #    self.mysqld_socket != None and self.mysqld_socket.shutdown(socket.SHUT_RDWR)
            #    return

            #if self.mysqld_socket != None and fd == self.mysqld_socket.fileno():
            #    logger.info( "close mysqld fd:[{}] connect".format( self.mysqld_socket.fileno()) )
            #    self.mysqld_socket.close()
            #    self.mysqld_socket = None
            #    self.client_socket != None and self.client_socket.shutdown(socket.SHUT_RDWR)
            #    return

            #raise Exception( "invalid fd: {}".format( fd ) )

    def __client_read( self):
        epoll_read_event( self.client_socket, self.mysqld_socket )

    def __mysqld_write( self):
        #连接mysqld完成,开启socket的读取
        if self.status == MYSQL_CONN_PENDING:
            logger.debug( "connect mysqld complate" )
            self.status = MYSQL_CONN_READY
            self.client_socket.enable_epoll_read()
            self.mysqld_socket.enable_epoll_read()
            return

        raise Exception()

    #当mysqld connect失败后，即使没有订阅EPOLLIN事件，epoll_wait也会返回epollin，标识连接错误，这个连接错误目前由EPOLLRDHUP处理了
    def __mysqld_read( self):
        if self.status == MYSQL_CONN_PENDING:
            raise Exception( "mysql connect pending" )

        epoll_read_event( self.mysqld_socket, self.client_socket )

    def __client_write( self ):
        raise Exception()

class MyTCPProxyAbstract( ABC ):
    @abstractmethod
    def stop( self ):
        pass

    @abstractmethod
    def start( self ):
        pass

##TCPProxy 管理一个epollfd以及一个listenfd，以及在此基础上衍生出来的
class TCPProxy( MyTCPProxyAbstract):

    def __init__(self, tcp_idle_timeout):
        self.connections = {};
        self.__work_thread_list = None
        self.__work_t1_stop = True
        self.__tcp_idle_timeout = tcp_idle_timeout
        th_obj=threading.Thread( target=self.connection_timeout_clean, args=(self,))
        th_obj.start()

    def __del__(self):
        #TODO 在该函数中需要结束 connection_timeout_clean 检测线程
        pass

    def __accept(self):
        try:
           while True:
              connection, address = self.listen_socket.accept()
              connection = MyNonBlockSocket( self.epoll, connection )
              logger.info( "accept fd:[{}] remote_addr:[{}] ".format( connection.fileno(),  str( address ) ) ) 

              #接收到一个连接，立即准备连接到目的端，进行流量转发，此处是转发到mysql
              mysqld_socket = MyNonBlockSocket( self.epoll , socket.socket(socket.AF_INET, socket.SOCK_STREAM) )
              logger.info( "new socket:{}".format( mysqld_socket.fileno() ) )

              #使用EPOLLOUT事件来检测连接成功，而不是使用EPOLLIN事件，是为了防止连接成功后，mysqld不发送数据
              conn_pair_obj = conn_pair( connection, mysqld_socket )

              try:
                  mysqld_socket.connect( self.__target_host )
              except socket.gaierror as e:  # __target_host 解析失败 gai 缩写getaddrinfo()
                  logger.error( "parse host[{}] fails. [{}]".format( self.__target_host, e ) )
                  connection.close()
                  mysqld_socket.close()
                  connection = None
                  mysqld_socket = None
                  conn_pair_obj = None
                  continue
              except BlockingIOError as e:
                  pass

              self.connections[ connection.fileno() ] = self.connections[ mysqld_socket.fileno() ] = conn_pair_obj
              self.epoll.register(connection.fileno(), MY_EPOLL_FLAGS )
              self.epoll.register(mysqld_socket.fileno(), select.EPOLLOUT | MY_EPOLL_FLAGS ) 

        except BlockingIOError as e:
            pass

    #导致关闭的原因包括client端、也包括mysqld端
    def __conn_shutdown( self, conn_pair_obj,  fileno ):
        self.epoll.unregister( fileno )
        del self.connections[ fileno ]
        conn_pair_obj.clean( fileno )

    def __find_conn_pair_with_fd(self, fileno):
        try:
            return self.connections[ fileno ]
        except KeyError as e:
            logger.error( "can't find fd:[{}] from connections [{}]".format( fileno, e) )
            return None

    def __event_name(self,event):
        rst = ""
        if event & select.EPOLLIN:
            rst += "|EPOLLIN"
        if event & select.EPOLLPRI:
            rst += "|EPOLLPRI"
        if event & select.EPOLLOUT:
            rst += "|EPOLLOUT"
        if event & select.EPOLLRDNORM:
            rst += "|EPOLLRDNORM"
        if event & select.EPOLLRDBAND:
            rst += "|EPOLLRDBAND"
        if event & select.EPOLLWRNORM:
            rst += "|EPOLLWRNORM"
        if event & select.EPOLLWRBAND:
            rst += "|EPOLLWRBAND"
        if event & select.EPOLLMSG:
            rst += "|EPOLLMSG"
        if event & select.EPOLLERR:
            rst += "|EPOLLERR"
        if event & select.EPOLLHUP:
            rst += "|EPOLLHUP"
        if event & select.EPOLLRDHUP:
            rst += "|EPOLLRDHUP"

        return rst

    #清除不活动超时的conn_pair
    @staticmethod
    def connection_timeout_clean( self ):

        while True:
            time.sleep( self.__tcp_idle_timeout )

            #关闭所有的TCP会话
            for conn_pair_obj in set(self.connections.values()):
                if conn_pair_obj.is_active():
                    conn_pair_obj.reset_active_status()
                    continue

                #仅关闭一侧即可，另一侧的关闭由EPOLL通过报错来触发关闭
                self.__conn_shutdown( conn_pair_obj, conn_pair_obj.client_socket.fileno())
                logger.debug( "close conn_pair_obj {}".format( conn_pair_obj ) )

    #当多线程调用epoll_wait，同一个socket的频繁产生的event可能会派生给不同的线程，导致多线程同时操作相同的socket
    @staticmethod
    def event_loop( self ):
        while not self.__work_t1_stop:
           events = self.epoll.poll( timeout=1 )
           for fileno, event in events:
              logger.debug( "recv fd:{} event: {:#06x} {}".format( fileno, event, self.__event_name(event) ) )

              #处理tcp accept
              if fileno == self.listen_socket.fileno():
                 self.__accept()
                 continue

              conn_pair_obj = self.__find_conn_pair_with_fd( fileno )

              if not conn_pair_obj:
                  continue

              logger.debug( "conn_pair_obj fd[{}] client:[{}] mysql:[{}]".format( \
                      fileno, \
                      conn_pair_obj.client_socket.fileno() if conn_pair_obj.client_socket else "None", \
                      conn_pair_obj.mysqld_socket.fileno() if conn_pair_obj.mysqld_socket else "None") )

              #Stream socket peer closed connection, or shut down writing half of connection
              if event & select.EPOLLRDHUP:
                 logger.error( "EPOLLRDHUP:[{}]".format(fileno) )
                 self.__conn_shutdown( conn_pair_obj, fileno )
                 continue

              if event & select.EPOLLERR:
                 logger.error( "EPOLLERR:[{}]".format(fileno) )
                 self.__conn_shutdown( conn_pair_obj, fileno )
                 continue

              if event & select.EPOLLHUP:
                 logger.error( "EPOLLHUP:[{}]".format(fileno) )
                 self.__conn_shutdown( conn_pair_obj, fileno )
                 continue

              if event & select.EPOLLIN:
                 logger.debug( "EPOLLIN:[{}]".format(fileno) )
                 try:
                     conn_pair_obj.epoll_in( fileno )
                 except ConnShutDown:
                     self.__conn_shutdown( conn_pair_obj, fileno )
                     continue

              if event & select.EPOLLOUT:
                 logger.debug( "EPOLLOUT:[{}]".format(fileno) )
                 try:
                     conn_pair_obj.epoll_out( fileno )
                 except ConnShutDown:
                     self.__conn_shutdown( conn_pair_obj, fileno )
                     continue

    def stop( self ):
        if self.__work_t1_stop:
            return

        #关闭所有的工作线程
        self.__work_t1_stop = True
        for thread_obj in self.__work_thread_list:
            thread_obj.join()
        logger.debug( "work_thread all stop, count:[{}]".format(  len( self.__work_thread_list) ) )
        self.__work_thread_list.clear()
        self.__work_thread_list = None

        #关闭listen
        self.epoll.unregister(self.listen_socket.fileno() )
        self.listen_socket.close()
        self.listen_socket = None
        logger.debug( "listen_socket close" )

        #关闭所有的TCP会话
        #字段在遍历过程中，不可以删除元素，因此在复制一个副本，在副本上进行遍历
        for fileno , conn_pair_obj in dict(self.connections).items():
            self.__conn_shutdown( conn_pair_obj, fileno )
        logger.debug( "connections all clean, count:[{}]".format(  len( self.connections) ) )
        self.connections.clear()

        #关闭epoll
        self.epoll.close()
        self.epoll = None
        logger.debug( "epollfd close" )

    def start( self, listen_port, target_host, work_thread_num ):

        if not self.__work_t1_stop:
            return

        self.__work_t1_stop = False 
        self.__target_host = target_host

        self.__work_thread_list=[]
        self.connections = {};

        self.epoll = select.epoll()
        ##需要支持多线程响应accept
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listen_socket.bind(('0.0.0.0', listen_port))
        self.listen_socket.listen(10)
        self.listen_socket.setblocking(0)

        self.epoll.register(self.listen_socket.fileno(), select.EPOLLIN | select.EPOLLET)

        for i in range(work_thread_num):
            th_obj=threading.Thread( target=self.event_loop, args=(self,))
            th_obj.start()
            self.__work_thread_list.append( th_obj )


