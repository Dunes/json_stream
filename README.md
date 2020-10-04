# jsonstream
Load multiple delimited JSON documents from a single string or file-like object.

This allows for storing multiple object roots to be stored in a single document,
but without having to load all the object roots from the document at once.
``jsonstream`` does not even require that you have the entire document
immediately stored in memory. This allows you to stream objects from files
which are too large to store in memory, or from network connections.

# Basic Usage


Retrieve objects from the document one at a time, or all at once.

```python
from jsonstream import loads
>>> doc = '[1, 2, 3] {"some": "object"}\n   null'
>>> it = loads(doc)
>>> next(it)
[1, 2, 3]
>>> list(it)
[{"some": "object"}, None]
```

Using file-like objects when the whole document is not immediately available


```python
>>> from jsonstream import load
>>> from io import StringIO
>>> fh = StringIO('["first"] ["second"]')
>>> it = load(fh)
>>> next(it)
["first"]
>>> fh.write('["and", "a", "third"]')
>>> list(it)
[["second"], ["and", "a", "third"]]
```

Further documentation can be found on the doc strings of ``loads`` and ``load``.

