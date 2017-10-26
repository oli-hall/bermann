class Row(object):
    """
    N.B. only supports Dict-style usage, e.g.

    Row(key='value', key2='value2')

    tuple-style usage (Row('value1', 'value2')) is
    not currently implemented.
    """
    def __init__(self, **kwargs):
        self.fields = kwargs

    def asDict(self, recursive=False):
        if recursive:
            def conv(obj):
                if isinstance(obj, Row):
                    return obj.asDict(True)
                elif isinstance(obj, list):
                    return [conv(o) for o in obj]
                elif isinstance(obj, dict):
                    return dict((k, conv(v)) for k, v in obj.items())
                else:
                    return obj

            return {k: conv(v) for k, v in self.fields.items()}

        return self.fields

    def __contains__(self, item):
        return item in self.fields

    def __getitem__(self, item):
        try:
            return self.fields[item]
        except KeyError:
            raise ValueError(item)

    def __getattr__(self, item):
        try:
            return self.fields[item]
        except KeyError:
            raise AttributeError(item)

    def __repr__(self):
        return "Row(%s)" % ", ".join("%s=%r" % (k, v) for k, v in self.fields.items())
