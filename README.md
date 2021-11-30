# BPIT_flask_autorest

自动生成模型restfulapi--陈嘉琅

# Autorest  


> Author: galain1994  
> Reference: Restless



## Design  

- workflow: Restful API   
```
-> request -> restapi ->

|-> get |-> query     -> serialize
        |-> instance

|-> put -> instance_id -> deserialize(update)

|-> post -> deserialize(create)

|-> delete -> instance_id -> instance.delete/session.delete
```    

### APIManager  


- Features:  
  - *Singleton*  
  - *Manage Blueprints*  
  - *Control Views*  



### Serializer  


- Serializer:   
```
serialize: instance -> dict -> converter -> filter -> result
```  




- Deserializer:  
```
deserialize: dict -> filters -> validators -> converters ->
             -> get_by_id |-> instance(exist) -> update
                          |-> instance -> create
```    

- Filters: 过滤字典的字段(也可用于serializer中)  
    - IncludeFilter  从字典中筛选所需要的字段  
    - ExcludeFilter  从字典中过滤所需要的字段  

- *params*:   
    - *filter_list*: 过滤的字段列表  
    - *name*: 过滤器名称(可为None)  
    - *global_fields*: 全局字段  


## Usage  


### Serializer


```

class A(model):

    b_ids -> [b1, b2, b3]
    c_ids -> [c1, c2, c3]


class B(model):

    a_id -> a
    d_ids -> [d1, d2, d3]


class C(model):

    a_ids -> [a1, a2, a3]


class D(model):

    b_id -> b
```



### Filter   

####  IncludeFilter   

```python

data = {
    "f1": "v1",
    "f2": {
        "f3": "v3",
        "f4": "v4"
    }
}
>>> include_filter1 = IncludeFilter(['f1', 'f2'])
>>> ret = include_filter1.filter(data)
{
    "f1": "v1",
    "f2": {
        "f3": "v3",
        "f4": "v4"
    }
}
>>> include_filter2 = IncludeFilter(['f1', 'f2.'])
>>> ret = include_filter2.filter(data)
{
    "f1": "v1",
    "f2": {}
}
>>> include_filter3 = IncludeFilter(['f1', 'f2.f3'])
>>> ret = include_filter3.filter(data)
{
    "f1": "v1",
    "f2": {
        "f3": "v3"
    }
}
```

#### ExcludeFilter   


```python

data = {
    "f1": "v1",
    "f2": {
        "f3": "v3",
        "f4": "v4"
    }
}

>>> exclude_filter1 = ExcludeFilter(['f2', ])
>>> ret = exclude_filter1.filter(data)
{
    "f1": "v1"
}
>>> exclude_filter2 = ExcludeFilter(['f2.'])
{
    "f1": "v1",
    "f2": {
        "f3": "v3",
        "f4": "v4"
    }
}

```

__对于instance对象具有同样的过滤效果__   





----------
## Testing   

current directory: `BPIT_flask_services`   

- Run Certain Test Function  
```shell
pytest base/autorest/test_autorest.py::TestMultiModel::test_put -vv
```  

  
- Run all Tests  
```shell
pytest base -vv
```

