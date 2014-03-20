import structures


def to_mongo(document, exclude=(), include=None):
    result =  structures.to_dict(document)

    if include is None:
        return {k: v for k, v in result.items() if k not in exclude}
    else:
        return {k: v for k, v in result.items() if (k not in exclude and k in include)}

def from_mongo(document_class, data):
    return structures.from_dict(document_class, data)