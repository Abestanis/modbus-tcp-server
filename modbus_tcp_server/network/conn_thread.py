import logging
import socket
import time

from satella.coding import silence_excs
from satella.coding.concurrent import TerminableThread

from ..datagrams import MODBUSTCPMessage
from ..exceptions import InvalidFrame

logger = logging.getLogger(__name__)

MAX_TIME_WITHOUT_ACTIVITY = 60.0    # 60 seconds


class ConnectionThread(TerminableThread):
    def __init__(self, sock, addr, server):
        super().__init__(terminate_on=(socket.error, InvalidFrame), daemon=True)
        self.socket = sock
        self.server = server
        self.addr = addr
        self.socket.setblocking(True)
        self.socket.settimeout(5)
        self.buffer = bytearray()
        self.last_activity = time.monotonic()

    def cleanup(self):
        self.socket.close()

    @silence_excs(socket.timeout)
    def loop(self):
        # noinspection PyProtectedMember
        if self.server._terminating:
            self.terminate()
            return
        if time.monotonic() - self.last_activity > MAX_TIME_WITHOUT_ACTIVITY:
            self.terminate()
            return
        data = self.socket.recv(128)

        if not data:
            raise socket.error()
        logger.info('Received %s', repr(data))
        self.buffer.extend(data)

        # Extract MODBUS packets
        while self.buffer:
            try:
                packet = MODBUSTCPMessage.from_bytes(self.buffer)
                del self.buffer[:len(packet)]
            except ValueError:
                break
            self.last_activity = time.monotonic()
            msg = self.server.process_message(packet)
            b = bytes(msg)
            logger.debug('Sent %s', repr(b))
            self.socket.sendall(b)
