
import socket
DATA={}
def handle_put(key,value):
    print ('handle_put')
    DATA[key]=value
    return (True,'key [{}] set to [{}]'.format(key,value))
def handle_putlist(key,value):
    print ('handle_putlist')
    return handle_put(key,value)
def handle_append(key,value):
    print ('handle_append')
    return_value=exists,list_value=handle_get(key)
    if not exists:
        return return_value
    elif not isinstance(list_value,list):
        return(False,'error:key[{}]contains non-list value([{}])'.format(key,value))
    else:
        DATA[key].append(value)
        return (True,'key[{}] had value[{}]append'.format(key,value))
def handle_delete(key):
    print ('handle_delete')
    if key not in DATA:
        print ('key not in DATA')
        return(False,'error:key[{}]not found and could not be deletde'.format(key))
    else:
        del DATA[key]
        return(True,'delele key success')
def handle_get(key):
    print ('handle_get')
    if key not in DATA:
        return (False,'error:key[{}]not fount'.format(key))
    else:
        return (True,DATA[key])
def handle_getlist(key):
    print('handle_getlist')
    return_value=exists,value=handle_get(key)
    
    print (return_value)
    print (exists,value)
    if not exists:
        return return_value
    elif not isinstance(value,list):
        return (False,'error:key[{}]contains non-list value([{}])'.format(key,value))
    else:
        return return_value
def handle_increment(key):
    print ('handle_increment')
    return_value=exists,value=handle_get(key)
    if not exists:
        return return_value
    elif not isinstance(value,int):
        return (False,'error:key[{}]contains non-list value([{}])'.format(key,value))
    else:
        DATA[key]=value+1
        return (True,'key [{}]incrementde'.format(key,value))
def parse_message(data):
    print ('parse_message')
    command,key,value,value_type=data.strip().split(';')
    print(command,key,value,value_type)
    if value_type:
        if value_type=='LIST':
            value=value.split(',')
        elif value_type=='INT':
            value=int(value)
        else:
            value=str(value)
    else:
        value=None  
    return command,key,value
COMMAND_HANDERS = {
    'PUT': handle_put,
    'GET': handle_get,
    'GETLIST': handle_getlist,
    'PUTLIST': handle_putlist,
    'INCREMENT': handle_increment,
    'APPEND': handle_append,
    'DELETe': handle_delete,
}
class kvnosql:
    def __init__(self):
        self.host='localhost'
        self.port=8888
        self.sock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.DATA={}
    #解析客户端数据
    
        
    def start(self):
        self.sock.bind((self.host,self.port))
        self.sock.listen(5)
        while 1:
            connection,address=self.sock.accept()
            print('new connection from[{}]'.format(address))
            data=connection.recv(4096).decode()
            command,key,value=parse_message(data)

            print ('parse_message after command,key,value=',DATA,command,key,value)
            if command in ('GET','GETLIST','INCREMENT','DELETe'):
                response=COMMAND_HANDERS[command](key)
            elif command in ('PUT','PUTLIST','APPEND'):
                response=COMMAND_HANDERS[command](key,value)
            else:
                response=(False,'UNKNOWN command tyep {}'.format(command))
            data1='{};\n{}\n'.format(response[0],response[1])
            connection.sendall(bytearray(data1,'utf-8'))
            connection.close()


nosql=kvnosql()
nosql.start()

