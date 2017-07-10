# es_demo
elasticsearch demo

# create mapping
curl -XPUT "http://127.0.0.1:9200/product/product/_mapping?pretty" -d'{
    "product": {
        "properties": {
            "name": {
                "type": "string"
            },
            "age": {
                "type": "long"
            },
            "address": {
                "properties": {
                    "country": {
                        "type": "string"
                    },
                    "city": {
                        "type": "string"
                    }
                }
            }
        }
    }
}'

# input data
curl -XPUT 'http: //localhost: 9200/product/product/1' -d'{
    "name": "trying out Elasticsearch",
    "age": 23,
    "address": [
        {
            "city": "hangzhou",
            "country": "china"
        }
    ]
}'
