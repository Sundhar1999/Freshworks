import sys
import os
sys.path.append(os.path.abspath('../src'))
from datastore import *
import pytest

@pytest.mark.first
def test_insert():
    ds = DataStore('ut_test.json')
    val1 = ds.insert('key1','val1')
    print(val1)
    assert val1['status']
    assert val1['message'] == 'Successfully inserted'
    val2 = ds.insert('key2','val2')
    assert val2['status']
    assert val2['message'] == 'Successfully inserted'
    val3 = ds.insert('key1','val3')
    assert not val3['status']
    assert val3['message'] == 'Key already exists'
    del ds

@pytest.mark.second
def test_read():
    ds = DataStore('ut_test.json')
    val1 = ds.read('key1')
    assert val1['status']
    assert val1['data'] == 'val1'
    val2 = ds.read('key2')
    assert val2['status']
    assert val2['data'] == 'val2'
    val3 = ds.read('key3')
    assert not val3['status']
    assert val3['message'] == 'Key does not exists'
    print(val1,val2,val3)
    del ds

@pytest.mark.third
def test_delete():
    ds = DataStore('ut_test.json')
    val1 = ds.delete('key1')
    assert val1['status']
    assert val1['message'] == 'Deleted successfully'
    val2 = ds.delete('key2')
    assert val2['status']
    assert val2['message'] == 'Deleted successfully'
    val3 = ds.delete('key3')
    assert not val3['status']
    assert val3['message'] == 'Key does not exists'
    del ds

@pytest.mark.parallel
def test_parallel_access():
    ds = DataStore('ut_test.json')
    ds1 = DataStore('ut_test.json')
    assert not ds1
    del ds

@pytest.mark.open_limit
def test_size_open_limit():
    Lock.MAX_SIZE = 1
    ds = DataStore('ut_test.json')
    assert not ds

@pytest.mark.write_limit
def test_write_limit():
    Lock.MAX_SIZE = 10
    ds = DataStore('ut_test1.json')
    write_res = ds.insert('key1','Some test value')
    assert not write_res['status']
    assert write_res['message'] == 'No enough space to store'
    del ds

@pytest.mark.cleanup
def test_cleanup():
    files = ['./ut_test.json', './ut_test1.json']
    for file in files:
        if os.path.exists(os.path.abspath(file)):
            os.remove(os.path.abspath(file))

test_read()