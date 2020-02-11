#Client for sending metrics, synchronous
import socket
import time

class ClientError(Exception):
    pass

class Client:
    def __init__(self, ip, port, timeout = None):
        try:
            self.sock = socket.create_connection((ip, port),timeout)
        except socket.error as err:
            raise ClientError
        
    #проверка валидности вводимых данных    
    def _is_number(self,str):
        try:
            float(str)
            return True
        except ValueError:
            return False

    def _check_server_status(self,answer):
        if not answer.startswith('ok\n'):
            return False
        if not answer.endswith('\n\n'):
            return False
        return True

    def _send(self, message):
        try:
            self.sock.send(message.encode('utf-8'))
        except socket.error as er:
            raise ClientError
 
    def _read(self):
        data = b""
        while not data.endswith(b"\n\n"):
            try:
                data = self.sock.recv(1024)
            except socket.error as er:
                raise ClientError
            return data.decode('utf8')

    def close(self):
        try:
            self.sock.close()
        except socket.error as er:
            raise ClientError 
    
    #sending messages to server
    def put(self, key, value, timestamp=None):
            if not self._is_number(value):
                raise ClientError
            if timestamp is not None:
                if not self._is_number(timestamp):
                    raise ClientError
            timestamp = timestamp or int(time.time())
            self._send(f"put {key} {value} {timestamp}\n")
            answer = self._read()
            if not self._check_server_status(answer):
                raise ClientError

    #geting messages from server
    def get(self, need_key):
        dict = {}
        self._send(f"get {need_key}\n")
        answer = self._read()
        if not self._check_server_status(answer):
            raise ClientError
        answer = answer.rstrip('\n\n').lstrip('ok\n')
        array_answer = answer.splitlines()
        if len(array_answer)>0:
            for i in array_answer:
                try:
                    key, value, timestamp = i.split(' ')  
                except ValueError:
                    raise ClientError 
                if key in dict:
                    dict[key].append((int(timestamp),float(value)))
                else:
                    dict[key]=[(int(timestamp),float(value))]
        if need_key == "*":
            return dict
        elif need_key in dict:
            s = sorted(dict.get(key), key=lambda tup: tup[0])
            dict[need_key] = s
            return dict
        else:
            return {}

    def get_test(self):
        self._send(f"got *\n")

if __name__ == "__main__":
    client = Client("127.0.0.1", 8888, timeout=15)
    client.put("test", 0.5, timestamp=1)
    client.put("test", 2.0, timestamp=2)
    client.put("test", 0.5, timestamp=3)
    client.put("load", 3, timestamp=4)
    client.put("load", 4, timestamp=5)
    print(client.get("*"))

    client.close()
          