import os
import platform
import json
import time
import atexit

if platform.system().lower() == 'windows':
    import msvcrt
else:
    import fcntl

class Lock:
    """
    Class to Lock & Unlock files in the file system
    Attributes
    ----------
    MAX_SIZE : int (constant)
        Maximum size value of the file in bytes
    Methods
    -------
    get_file_size(file)
        Returns the size of the given file
    lock_file(file)
        Locks the given file in the file system
    unlock_file(file)
        Unlocks the given file in the file system
    """
    MAX_SIZE = 1024 ** 3

    @staticmethod
    def get_file_size(file):
        """
        Returns the size of the file
        Parameters
        ----------
        file : _io.TextIOWrapper
            file to get the size
        """
        return os.path.getsize(os.path.realpath(file.name))

    @staticmethod
    def lock_file(file):
        """
        Locks the given file
        Parameters
        ----------
        file : _io.TextIOWrapper
            file to lock
        """
        if platform.system().lower() == 'windows':
            msvcrt.locking(file.fileno(), msvcrt.LK_RLCK, Lock.get_file_size(file))
        else:
            fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)

    @staticmethod
    def unlock_file(file):
        """
        Unlocks the given file
        Parameters
        ----------
        file : _io.TextIOWrapper
            file to unlock
        """
        if platform.system().lower() == 'windows':
            msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, Lock.get_file_size(file))
        else:
            fcntl.flock(file, fcntl.LOCK_UN)

class DataStore:
    """
    Class to perform data store
    Methods
    -------
    insert(self, key, value)
        inserts the given key value pair
    read(self, key)
        reads the data of given key
    delete(self, key)
        deletes the key value pair entry of the given key
    save(self)
        saves the data from the buffer to the file
    """

    get_len = lambda x: len(json.dumps(x, separators=(',',':')))

    def __new__(cls, file_path=''):
        """
        New methods, prevents from creating instance if a locked/not accessible data_store is given
        """
        if file_path == '':
            file_path = os.path.abspath('./data_store/data_store.json')
            print('Default file: {}'.format(file_path))
        else:
            file_path = os.path.abspath(file_path)
            print('Given input file is {}'.format(file_path))
        try:
            if os.path.exists(file_path):
                file = open(file_path,'r')
                if Lock.get_file_size(file) >= Lock.MAX_SIZE:
                    print('File size is >= 1GB')
                    return None
                Lock.lock_file(file)
                Lock.unlock_file(file)
            return super(DataStore, cls).__new__(cls)
        except:
            print('The file is being used by another process!')
            return None


    def __init__(self, file_path=''):
        """
        Initializer method
        Parameters
        ----------
        file_path : str, optional
            specifies the file path in the file system
        """
        if file_path == '':
            dir = os.path.abspath('./data_store')
            if not os.path.exists(dir):
                os.makedirs(dir)
            self.__file_path = os.path.join(dir, 'data_store.json')
        else:
            self.__file_path = os.path.abspath(file_path)
        if not os.path.exists(self.__file_path):
            with open(self.__file_path, 'w') as j_file:
                js_d = {}
                json.dump(js_d,j_file,separators=(',',':'))
        self.__file = open(self.__file_path, 'r')
        Lock.lock_file(self.__file)
        self.__processing = False
        self.__total_size = Lock.get_file_size(self.__file)
        self.__data = json.load(self.__file)
        if platform.system().lower() == 'windows':
            Lock.lock_file(self.__file)
        self.__keys = list(self.__data.keys())
        self.__keys_len = len(self.__keys)
        self.__modified = False
        atexit.register(self.cleanup)


    def insert(self, key, value):
        """
        inserts the given key value pair data in the data store
        Parameters
        ----------
        key : str
            key for the data store
        value : str
            value for the given key
        Returns
        -------
        dict data with status and message
        """
        while self.__processing:
            time.sleep(0.2)
        self.__processing = True
        if key.upper() in self.__keys:
            self.__processing = False
            return {'status': False, 'message': 'Key already exists'}
        size_checker = self.__total_size + DataStore.get_len({key:value}) + (1 if self.__keys_len > 0 else 0)
        if size_checker > Lock.MAX_SIZE:
            self.__processing = False
            return {'status': False, 'message': 'No enough space to store'}
        self.__data[key.upper()] = value
        self.__keys.append(key.upper())
        self.__total_size = size_checker
        self.__keys_len += 1
        self.__processing = False
        self.__modified = True
        return {'status': True, 'message': 'Successfully inserted'}

    def read(self, key):
        """
        reads the data of the given key from data store
        Parameters
        ----------
        key : str
            key in the data store
        Returns
        -------
        dict data with status and data/message
        """
        while self.__processing:
            time.sleep(0.2)
        self.__processing = True
        if key.upper() not in self.__keys:
            self.__processing = False
            return {'status': False, 'message': 'Key does not exists'}
        self.__processing = False
        return {'status': True, 'data': self.__data[key.upper()]}

    def delete(self, key):
        """
        deletes the key value pair data of the given key from the data store
        Parameters
        ----------
        key : str
            key in the data store
        Returns
        -------
        dict data with status and message.
        """
        while self.__processing:
            time.sleep(0.2)
        self.__processing = True
        if key.upper() not in self.__keys:
            self.__processing = False
            return {'status': False, 'message': 'Key does not exists'}
        size_checker = self.__total_size - (DataStore.get_len({key: self.__data[key.upper()]}) + (1 if self.__keys_len > 1 else 0))
        del self.__data[key.upper()]
        self.__keys.pop(self.__keys.index(key.upper()))
        self.__total_size = size_checker
        self.__keys_len -= 1
        self.__processing = False
        self.__modified = True
        return {'status': True, 'message': 'Deleted successfully'}


    def save(self):
        """
        saves the data store buffer to the file
        """
        if not self.__modified:
            return
        while self.__processing:
            time.sleep(0.2)
        self.__processing = True
        Lock.unlock_file(self.__file)
        self.__file.close()
        self.__file = open(self.__file_path, 'w')
        Lock.lock_file(self.__file)
        json.dump(self.__data, self.__file, separators=(',',':'))
        Lock.unlock_file(self.__file)
        self.__file.close()
        self.__file = open(self.__file_path, 'r')
        Lock.lock_file(self.__file)
        self.__modified = False
        self.__processing = False

    def cleanup(self):
        """
        cleanup method to save data and close file
        """
        self.save()
        Lock.unlock_file(self.__file)
        self.__file.close()
