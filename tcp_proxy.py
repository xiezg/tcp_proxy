###
### URL: https://github.com/xiezg/tcp_proxy
###
import time
import threading
import sys
import os
import errno
import socket, select
import logging
import traceback

logging.basicConfig(level = logging.DEBUG, format = '%(asctime)s %(threadName)-9s line:%(lineno)-4d %(levelname)7s %(funcName)16s():%(message)s')
logger = logging.getLogger(__name__)

MYSQL_CONN_PENDING   = 0      #接收到客户端的连接请求并就绪后，连接到mysql的请求连接还未就绪,这时只可以从clinet接收数据，但是不可以向数据库发送数据
MYSQL_CONN_READY     = 1      #连接到mysqld的连接已就绪，可以在双方进行数据传输

MY_EPOLL_FLAGS = select.EPOLLERR| select.EPOLLRDHUP | select.EPOLLET | select.EPOLLONESHOT

class ConnShutDown( Exception ):
    pass

class MySocket:
    def __init__(self, epoll, socket ):
        self.__socket=socket
        self.__epoll = epoll
        self.__fd = socket.fileno()
        self.recv_buf = bytearray()

    def shutdown(self, flag):
        self.__socket.shutdown( flag )

    def close(self):
        self.__socket.close()

    def getpeername( self):
        return self.__socket.getpeername()

    def connect(self, address):
        return self.__socket.connect( address)

    def setblocking(self, flags ):
        return self.__socket.setblocking( flags )

    def fileno(self):
        return self.__socket.fileno()

    def enable_epoll_read(self):
        try:
            self.__epoll.modify( self.fileno(), select.EPOLLIN|MY_EPOLL_FLAGS)
        except FileNotFoundError as e:
            logger.error( e )

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
            raise e
        except socket.error as e:
            logger.error("send fd[{}], err:[{}]".format(self.__fd, e) )
            raise e

    def recv(self, size ):
        try:
            data = self.__socket.recv(size)
            if len( data ) == 0:
                logger.info( "recv empty, peer close fd:[{}] local/peer:[{}/{}]".format( self.__socket.fileno(), self.__socket.getsockname(), self.__socket.getpeername() ) )
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

class conn_pair:

    def __init__( self, client_socket, mysqld_socket ):
        self.client_socket = client_socket #客户端
        self.mysqld_socket = mysqld_socket #目的端
        self.__close_lock = threading.Lock()

        self.status = MYSQL_CONN_PENDING

    def epoll_in( self, fd):
        if self.client_socket != None and fd == self.client_socket.fileno():
            return self.__client_read()

        if self.mysqld_socket != None and fd == self.mysqld_socket.fileno():
            return self.__mysqld_read()

        raise Exception( "invalid fd: {}".format( fd ) )

    def epoll_out( self, fd):
        if self.client_socket != None and fd == self.client_socket.fileno():
            return self.__client_write()

        if self.mysqld_socket != None and fd == self.mysqld_socket.fileno():
            return self.__mysqld_write()

        raise Exception( "invalid fd: {}".format( fd ) )

    def clean(self, fd ):
        with self.__close_lock:
            if self.client_socket != None and fd == self.client_socket.fileno():
                logger.info( "close client fd:[{}] connect".format( self.client_socket.fileno()) )
                self.client_socket.close()
                self.client_socket = None
                self.mysqld_socket != None and self.mysqld_socket.shutdown(socket.SHUT_RDWR)
                return

            if self.mysqld_socket != None and fd == self.mysqld_socket.fileno():
                logger.info( "close mysqld fd:[{}] connect".format( self.mysqld_socket.fileno()) )
                self.mysqld_socket.close()
                self.mysqld_socket = None
                self.client_socket!= None and self.client_socket.shutdown(socket.SHUT_RDWR)
                return

            raise Exception( "invalid fd: {}".format( fd ) )

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

class TCPProxy(threading.Thread):
    def __init__( self, listen_port, target_addr, target_port ):
        threading.Thread.__init__(self, daemon=False )

        self.target_addr = target_addr
        self.target_port = target_port
        self.connections = {};

        self.epoll = select.epoll()
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listen_socket.bind(('0.0.0.0', listen_port))
        self.listen_socket.listen(10)
        self.listen_socket.setblocking(0)

        self.epoll.register(self.listen_socket.fileno(), select.EPOLLIN | select.EPOLLET)

    def __accept(self):
        try:
           while True:
              connection, address = self.listen_socket.accept()
              connection = MySocket( self.epoll, connection )
              connection.setblocking(0)
              logger.debug( "accept fd:[{}] remote_addr:[{}] ".format( connection.fileno(),  str( address ) ) )

              #接收到一个连接，立即准备连接到目的端，进行流量转发，此处是转发到mysql
              mysqld_socket = MySocket( self.epoll , socket.socket(socket.AF_INET, socket.SOCK_STREAM) )
              mysqld_socket.setblocking(0)

              logger.debug( "new socket:{}".format( mysqld_socket.fileno() ) )

              #使用EPOLLOUT事件来检测连接成功，而不是使用EPOLLIN事件，是为了防止连接成功后，mysqld不发送数据
              conn_pair_obj = conn_pair( connection, mysqld_socket )
              self.connections[ connection.fileno() ] = self.connections[ mysqld_socket.fileno() ] = conn_pair_obj

              try:
                  mysqld_socket.connect(( self.target_addr, self.target_port ))
              except BlockingIOError as e:
                  pass

              self.epoll.register(connection.fileno(), MY_EPOLL_FLAGS )
              self.epoll.register(mysqld_socket.fileno(), select.EPOLLOUT | MY_EPOLL_FLAGS )

        except BlockingIOError as e:
            pass

    #导致关闭的原因包括client端、也包括mysqld端
    def __conn_shutdown( self, conn_pair_obj,  fileno ):
        self.epoll.unregister( fileno )
        del self.connections[ fileno ]
        conn_pair_obj.clean( fileno )

    def __error(self,fileno):
        try:
            conn_pair_obj = self.connections[ fileno ]
            self.__conn_shutdown( conn_pair_obj, fileno )
        except KeyError as e:
            logger.warning( "can't find fd:[{}] from connections [{}]".format( fileno, e) )

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

    #当多线程调用epoll_wait，同一个socket的频繁产生的event可能会派生给不同的线程，导致多线程同时操作相同的socket
    #通过设置 EPOLLONESHOT 来避免上述问题
    def event_loop( self):
        while True:
           events = self.epoll.poll(1)
           for fileno, event in events:
              logger.debug( "recv fd:{} event: {:#06x} {}".format( fileno, event, self.__event_name(event) ) )
              if fileno == self.listen_socket.fileno():
                 self.__accept()
                 continue

              #Stream socket peer closed connection, or shut down writing half of connection
              if event & select.EPOLLRDHUP:
                 logger.error( "EPOLLRDHUP:[{}]".format(fileno) )
                 self.__error(fileno)
                 continue

              if event & select.EPOLLERR:
                 logger.error( "EPOLLERR:[{}]".format(fileno) )
                 self.__error(fileno)
                 continue

              if event & select.EPOLLHUP:
                 logger.error( "EPOLLHUP:[{}]".format(fileno) )

              if event & select.EPOLLIN:
                 logger.debug( "EPOLLIN:[{}]".format(fileno) )
                 try:
                     conn_pair_obj = self.connections[ fileno ]
                     conn_pair_obj.epoll_in( fileno )
                 except KeyError as e:    #直接忽律该错误
                     logger.warning( "can't find fd:[{}] from connections [{}]".format( fileno, e) )
                 except ConnShutDown:
                     self.__conn_shutdown( conn_pair_obj, fileno )

              if event & select.EPOLLOUT:
                 logger.debug( "EPOLLOUT:[{}]".format(fileno) )
                 try:
                     conn_pair_obj = self.connections[ fileno ]
                     conn_pair_obj.epoll_out( fileno )
                 except KeyError as e:
                     logger.warning( "can't find fd:[{}] from connections".format( fileno) )
                 except ConnShutDown:
                     self.__conn_shutdown( conn_pair_obj, fileno )

proxy = TCPProxy(7777, "172.18.10.22", 9999 )

def worker():
    try:
        proxy.event_loop()
    except Exception as e:
        traceback.print_exc()
        logger.critical( "event_loop Exception:{}".format(e) )
        logging.shutdown()
        os._exit(1)

#起开16个线程
for i in range(16):
    t1=threading.Thread( target=worker, daemon=False )
    t1.start()
