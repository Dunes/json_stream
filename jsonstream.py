from functools import partial
import io
import re
from json import JSONDecoder, JSONDecodeError, dump, dumps


__all__ = ['load', 'loads', 'dump', 'dumps']


NOT_WHITESPACE = re.compile(r'\S')
KEYWORDS = (
    'null',
    'true',
    'false',
    # not in JSON spec, but decoded by Python JSON
    'NaN',
    'Infinity',
    '-Infinity',
)
MAX_KEYWORD_LEN = max(len(keyword) for keyword in KEYWORDS)


def load(
        fp, *, cls=None, object_hook=None, parse_float=None,
        parse_int=None, parse_constant=None, object_pairs_hook=None,
        separator=None,
        bufsize=1048576, # 1MB
        max_bufsize=float('inf'),
        stream_offset=0,
        **kw
):
    """
        Deserialize ``fp`` (a ``.read()``-supporting file-like object containing
        multiple JSON documents separated by whitespace) to Python objects.

        Almost identical to ``json.load``, except that it produces and iterator.
        Which returns zero or more objects that can be decoded by JSON documents
        separated by whitespace, or the given string or regex.

        ``separator`` defaults to zero or more whitespace characters. You can
        provide a different, but fixed width, separator if desired.

        If ``fp`` produces bytes, then it will be wrapped in a ``io.TextIOWrapper``
        using UTF-8
        decoding. If the data is not UTF-8 then you must wrap it manually with the
        appropriate encoding.

        ``bufsize`` the amount of characters to read from ``fp`` in single pass

        ``max_bufsize`` limit the size of the buffer used to hold unparsed
        parts of the document. Must be at least as large as the largest document
        in the stream, or a JSONDecodeError will be raised.

        ``stream_offset`` The number of characters read from the stream before it
        has been passed to ``load``. Used in error messages if ``fp`` is not at the start
        of the stream.

        Where possible, prefer ``loads(fp.read())`` over ``load(fp)``. The implementation
        of ``loads`` is considerably simpler and has less edge cases to deal with. This
        makes ``loads`` more performant, except for when dealing with very large data
        streams or streams where calling read() is undesirable (eg. a long lived socket).
        """
    if not hasattr(fp, 'encoding'):
        fp = io.TextIOWrapper(fp, encoding='utf8')
    cls, kw = _parse_kw(
        cls, object_hook, parse_float, parse_int, parse_constant, object_pairs_hook, kw
    )
    next_pos, pos = get_first_pos_and_next_pos_func(separator)
        
    return iter(DecodeStream(
        fp,
        cls(**kw),
        next_pos,
        pos,
        kw.get('parse_int'),
        kw.get('parse_float'),
        bufsize,
        max_bufsize,
        stream_offset,
    ))


def loads(
        s, *, cls=None, object_hook=None, parse_float=None,
        parse_int=None, parse_constant=None, object_pairs_hook=None,
        pos=0,
        separator=None,
        **kw
):
    """
        Deserialize ``s`` (a ``str``, ``bytes`` or ``bytearray`` instance
        containing a multiple JSON documents) to Python objects.
        
        Almost identical to ``json.loads``, except that it produces and iterator.
        Which returns zero or more objects that are decoded from JSON documents
        separated by whitespace, or the given string or regex.

        Always decodes bytes and bytearrays as UTF-8. Manually decode if this is
        not desired.

        ``pos`` can be used to provide an offset from where to start parsing ``s``.

        ``separator`` defaults to zero or more whitespace characters. You can
        provide a different, but fixed width, separator if desired.
    """
    if isinstance(s, str):
        if s.startswith('\ufeff'):
            raise JSONDecodeError("Unexpected UTF-8 BOM (decode using utf-8-sig)",
                                  s, 0)
    else:
        if not isinstance(s, (bytes, bytearray)):
            raise TypeError(f'the JSON object must be str, bytes or bytearray, '
                            f'not {s.__class__.__name__}')
        s = s.decode('utf8')

    cls, kw = _parse_kw(
        cls, object_hook, parse_float, parse_int, parse_constant, object_pairs_hook, kw
    )
    next_pos, pos = get_first_pos_and_next_pos_func(separator)
    return decode_stacked(s, cls(**kw), next_pos, pos)


def _parse_kw(
    cls, object_hook, parse_float, parse_int, parse_constant, object_pairs_hook, kw
):
    if cls is None:
        cls = JSONDecoder
    if object_hook is not None:
        kw['object_hook'] = object_hook
    if object_pairs_hook is not None:
        kw['object_pairs_hook'] = object_pairs_hook
    if parse_float is not None:
        kw['parse_float'] = parse_float
    if parse_int is not None:
        kw['parse_int'] = parse_int
    if parse_constant is not None:
        kw['parse_constant'] = parse_constant
    return cls, kw


def get_first_pos_and_next_pos_func(separator):
    if separator is None:
        return next_position_by_non_whitespace, None
    else:
        return partial(next_position_by_separator, separator), 0


def next_position_by_separator(separator, document, pos):
    if document.startswith(separator, pos):
        return pos + len(separator)
    elif pos == len(document):
        return None
    else:
        raise ValueError(f'Expected {separator!r} delimiter')


def next_position_by_non_whitespace(document, pos):
    match = NOT_WHITESPACE.search(document, pos)
    return match and match.start()


def decode_stacked(document, decoder, next_pos, pos=None):
    if not document:
        return
    if pos is None:
        # if pos is None, then we don't actually know where the first
        # object starts, so scan for it
        pos = next_pos(document, 0)
        if pos is None:
            return
    while True:
        try:
            obj, pos = decoder.raw_decode(document, pos)
        except JSONDecodeError as ex:
            # do something sensible if there's some error
            raise
        yield obj
        
        pos = next_pos(document, pos)
        if pos is None:
            return


class DecodeStream:
    def __init__(
        self,
        stream,
        decoder,
        next_pos,
        pos,
        parse_int,
        parse_float,
        bufsize=1048576, # 1MB
        max_bufsize=float('inf'),
        stream_offset=0,
    ):
        self.stream = stream
        self.decoder = decoder
        self.next_pos_helper = next_pos
        self.bufsize = bufsize
        self.max_bufsize = max_bufsize
        self.pos = pos
        self.partial_doc = ''
        self.stream_offset = stream_offset
        self._iter = None

        int_type = int if parse_int is None else type(parse_int('0'))
        float_type = float if parse_float is None else type(parse_float('0'))
        self.number_types = (int_type, float_type)

        if bufsize < 1:
            raise ValueError('expect positive value for bufsize')

    def __iter__(self):
        # avoid trying to create multiple iterators over the same stream
        if self._iter is None:
            self._iter = self._decode_stream_generator()
        return self._iter

    def _decode_stream_generator(self):
        if self.pos is None:
            # check to see where the first object might start
            self.pos = 0
            if not self.next_pos():
                return
        elif not self._try_read(self.partial_doc):
            # otherwise check there is actually some data in the document
            return
            
        while True:
            try:
                obj, new_pos = self.decoder.raw_decode(self.partial_doc, self.pos)
            except JSONDecodeError as ex:
                if self._match_error(ex):
                    if self._try_read(self.partial_doc[self.pos:]):
                        continue
                # decode did not reach end of document
                # or it did, but there is no new data to be had
                self._update_error(ex)
                raise

            # edge case: a number is split by the buffer eg. 123/456 (or 123./456)
            # We must decode this as 123456 not as 123 followed by 456
            # This edge case is only possible if the document has numbers as the data root
            #   eg. stream gives '123 456 789' as opposed to '[123, 456] [789]'
            # This is not strictly permitted by the JSON standard (which only allows arrays
            # and objects as the root). However, the Python JSON module happily decodes
            # numbers when they are the root.
            if new_pos >= len(self.partial_doc) - 1 and isinstance(obj, self.number_types):
                # subtract 1 from length of document because we parse '1.' as:
                # 1 followed by an error
                if self._try_read(self.partial_doc[self.pos:]):
                    # we got new data, ignore this parse
                    continue

            self.pos = new_pos
            del new_pos
            yield obj

            # find start of next object
            if not self.next_pos():
                return

    def next_pos(self):
        """Read enough data from the stream until:

        * the start of the next object is found (return true)
        * the delimiter check fails (raise an error)
        * the stream is empty (return false)
        """
        while True:
            new_pos = self.next_pos_helper(self.partial_doc, self.pos)
            if new_pos is None:
                if self._try_read(''):
                    continue
                return False
            else:
                self.pos = new_pos
                return True

    def _try_read(self, remaining_buffer):
        """
        Reads new data, and adds to any unparsed data.

        Returns true if new data was read
        """
        if len(remaining_buffer) + self.bufsize > self.max_bufsize:
            to_read = self.max_bufsize - len(remaining_buffer)
            if to_read <= 0:
                raise ValueError('max buffer size exceeded')
        else:
            to_read = self.bufsize
        new = self.stream.read(to_read)
        self.stream_offset += len(self.partial_doc) - len(remaining_buffer)
        self.partial_doc = remaining_buffer + new
        self.pos = 0
        return bool(new)


    @staticmethod
    def _match_error(ex):
        """Given the JSONDecodeError, would new data be useful?"""
        # very fragile?
        # On the face of it, this regex looks like it includes all error messages
        # However, it also deals with
        # * invalid escapes eg. \s
        # * invalid unicode escapes \uxxxx
        # * disallowed characters in strings eg \t -- tabs are not allowed to
        #       appear directly in strings and instead must be escaped
        error_message_pattern = (
            'Unterminated string starting at'
            # eg. "123
            '|Expecting value'
            # eg. [,1]
            '|Expecting property name enclosed in double quotes'
            # eg. {1}
            "|Expecting '[,:]' delimiter"
            # eg. [1 2] or {"x"}
        )
        match = re.match(error_message_pattern, ex.msg)
        if match and ex.msg.startswith('Expecting'):
            # We only match the "Expecting" messages if the position marks the
            # end of the document.
            # Do not need to subtract 1 from len as pos is 1-indexed
            return (
                ex.pos == len(ex.doc)
                or (
                    # edge-case: a float is split by the buffer eg. [1./2]
                    # reported error is expecting an item delimiter at penultimate position,
                    # but could also be expecting a digit after end of doc
                    # this only occurs when the number is not the root of the document
                    ex.pos == len(ex.doc) - 1
                    and ex.doc[ex.pos] == '.'
                    and ex.doc[ex.pos-1].isdigit()
                )
                or (
                    # edge case: a keyword is split by the buffer eg. nul/l
                    ex.msg.startswith('Expecting value')
                    and len(ex.doc) - ex.pos < MAX_KEYWORD_LEN
                    and any(kw.startswith(ex.doc[ex.pos:]) for kw in KEYWORDS)
                        
                )
            )
        else:
            return bool(match)

    def _update_error(self, ex):
        # fragile... but update ex to include how much of the stream that has been
        # read, but is not included in ex.doc
        formatted_message, *rest = ex.args
        formatted_message += f' (stream offset {self.stream_offset})'
        ValueError.__init__(ex, formatted_message, *rest)
        ex.stream_offset = self.stream_offset
