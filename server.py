import asyncio
from collections import defaultdict
from copy import deepcopy

class Storage:
    def __init__(self):
        self.dictionary = defaultdict(dict)

    def get(self, key):
        if key == "*":
            return deepcopy(self.dictionary)
        if key in self.dictionary:
            return {key: deepcopy(self.dictionary.get(key))}
        return {}

    def put(self,key,value,timestamp):
        self.dictionary[key][timestamp] = value

class ClientError(Exception):
    pass

class StorageDriverError(ValueError):
    pass

class ClientServerProtocol(asyncio.Protocol):

    storage = Storage()
    sep = '\n'
    error_message = "wrong command"
    code_err = "error"
    code_ok = "ok"

    def __init__(self):
        super().__init__()
        self._buffer = b''

    def connection_made(self, transport):
        self.transport = transport
        
    def data_received(self, data):
        self._buffer += data
        try:
            request = self._buffer.decode()
            if not request.endswith(self.sep):
                print("возврат")
                return
            self._buffer, message = b'', ''
            raw_data = self.process_data_filter(request.rstrip(self.sep))
            for key, values in raw_data.items():
                message += self.sep.join(f'{key} {value} {timestamp}' \
                                         for timestamp, value in sorted(values.items()))
                message += self.sep
            code = self.code_ok

        except(ValueError, UnicodeDecodeError,IndexError, StorageDriverError):         
            message = self.error_message + self.sep
            code = self.code_err
            
        resp = f'{code}{self.sep}{message}{self.sep}'
        self.transport.write(resp.encode())
       
    def process_data_filter(self, data):
        
        method, *params = data.split()
        if method == "put":
            key, value, timestamp = params
            value, timestamp = float(value), int(timestamp)
            self.storage.put(key, value, timestamp)
            return {}
        elif method == "get":
            key = params.pop()
            if params:
                raise StorageDriverError
            return self.storage.get(key)
        else:
            
            raise StorageDriverError



def run_server(ip, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(ClientServerProtocol,ip,port)
    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

if __name__ == "__main__":
    run_server("127.0.0.1", 8888)