import structures


def to_mongo(document, exclude=()):
    result =  structures.to_dict(document)
    return {k: v for k, v in result.items() if k not in exclude}


def from_mongo(document_class, data):
    return structures.from_dict(document_class, data)
