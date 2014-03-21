import structures


def to_mongo(document, exclude=(), include=None):
    result = {}

    for name, field in document.__fields__.items():
        if name in exclude:
            continue

        if include is not None and name not in include:
            continue

        if not hasattr(document, name):
            continue

        value = getattr(document, name)

        if hasattr(field, 'to_mongo'):
            result[name] = field.to_mongo(document, value)
        else:
            result[name] = value

    return result


def from_mongo(document_class, data, clear_fields_changed=True):
    document = document_class()

    for name, field in document.__fields__.items():
        if name not in data:
            continue

        value = data[name]

        if hasattr(field, 'from_mongo'):
            setattr(document, name, field.from_mongo(document, value))
        else:
            setattr(document, name, value)

    if clear_fields_changed:
        document.__fields_changed__.clear()

    return document
