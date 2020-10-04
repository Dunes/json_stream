import pytest

from decimal import Decimal
from io import StringIO, BytesIO
from json import JSONDecodeError
import math

from jsonstream import load, loads


LOADERS = [
    pytest.param(loads),
    pytest.param(
        lambda s, **kw: load(StringIO(s), **kw),
        id='load'
    ),
    pytest.param(
        lambda s, **kw: load(StringIO(s), bufsize=1, **kw),
        id='load with bufsize=1'
    ),
]


@pytest.mark.parametrize(
    'test_input,expected',
    [
        pytest.param(
            '''
{
"a"
:
1
}

  [
  1
  ,
  2
  ]{"squashed":"together"}''',
            [{"a": 1}, [1, 2], {"squashed": "together"}],
            id='document spans multiple lines'
        ),
        pytest.param(
            '[] {} [1, 2]',
            [[], {}, [1, 2]],
            id='multiple documents on one line'
        ),
        pytest.param(
            'null true false',
            [None, True, False],
            id='json common constants'
        ),
        pytest.param(
           'Infinity -Infinity',
            [float('inf'), float('-inf')],
            id='python json module special constants'  # excluding NaN
        ),
        pytest.param(
            '1 234',
            [1, 234],
            id='split integers'
        ),
        pytest.param(
            '[1.5] 0.5',
            [[1.5], 0.5],
            id='split floats'
            # a split float in an array/object produces a different error if the number
            # is the root of the document
        ),
        pytest.param(
            '"some string"',
            ["some string"],
            id='split string'
        ),
    ],
)
@pytest.mark.parametrize('loader', LOADERS)
def test_loaders(test_input, expected, loader):
    assert list(loader(test_input)) == expected


@pytest.mark.parametrize(
    'test_input,expected,msg',
    [
        pytest.param(
            '{"a"',
            JSONDecodeError,
            "Expecting ':' delimiter",
            id='missing object key/value delimiter'
        ),
        pytest.param(
            '{',
            JSONDecodeError,
            'Expecting property name enclosed in double quotes',
            id='missing property'
        ),
        pytest.param(
            '"a',
            JSONDecodeError,
            'Unterminated string starting at',
            id='malformed string'
        ),
        pytest.param(
            'nul',
            JSONDecodeError,
            'Expecting value',
            id='malformed keyword'
        ),
        pytest.param(
            '[,1]',
            JSONDecodeError,
            'Expecting value',
            id='missing array item'
        ),
        pytest.param(
            '[1 2]',
            JSONDecodeError,
            "Expecting ',' delimiter",
            id='missing array/object item delimiter'
        ),
        pytest.param(
            '1.',
            JSONDecodeError,
            "Expecting value",
            id='malformed decimal'
        ),
    ]
)
@pytest.mark.parametrize('loader', LOADERS)
def test_failed_decodes(test_input, expected, msg, loader):
    with pytest.raises(expected) as cm:
        list(loader(test_input))
    assert cm.value.msg == msg


@pytest.mark.parametrize('loader', LOADERS)
def test_decode_nan(loader):
    doc = 'NaN'

    [result] = list(loader(doc))
    assert math.isnan(result)


def test_load_buffer_maxed_out():
    stream = StringIO('"tiny" "some very long string"')

    stream.seek(0)
    gen = iter(load(stream, bufsize=1, max_bufsize=6))

    assert next(gen) == 'tiny'
    
    with pytest.raises(ValueError) as cm:
        next(gen)
    assert cm.match('max buffer size exceeded')


def test_preceding_whitespace_discarded_when_buffer_overfull():
    doc = '"small"'
    max_buf = len(doc)
    stream = StringIO('  ' * max_buf + doc)

    stream.seek(0)
    assert list(load(stream, bufsize=1, max_bufsize=max_buf)) == ['small']


def test_stream_offset_in_error():
    offset = 10
    doc = ' ' * offset + 'cannot_decode'
    
    stream = StringIO(doc)

    stream.seek(0)    
    with pytest.raises(JSONDecodeError) as cm:
        list(load(stream, bufsize=1))
  
    assert cm.value.stream_offset == offset
    assert cm.match(r'\(stream offset 10\)$')


@pytest.mark.parametrize(
    'expected,loader_args',
    [
        pytest.param(
            [Decimal('123'), 5.67],
            dict(parse_int=Decimal),
            id='parse_int'
        ),
        pytest.param(
            [123, Decimal('5.67')],
            dict(parse_float=Decimal),
            id='parse_float'
        ),
        pytest.param(
            [123.0, (5.67+0j)],
            dict(parse_int=float, parse_float=complex),
            id='parse_both'
        ),
    ],
)
@pytest.mark.parametrize('loader', LOADERS)
def test_parse_number_hooks(loader, expected, loader_args):
    test_input = '123 5.67'

    result = list(loader(test_input, **loader_args))
    assert result == expected
    assert list(map(type, result)) == list(map(type, expected))
    


@pytest.mark.parametrize(
    'loader',
    [
        pytest.param(loads),
        pytest.param(lambda s: load(BytesIO(s)), id='load'),
        pytest.param(lambda s: load(BytesIO(s), bufsize=1), id='load with bufsize=1'),
    ]
)
def test_decode_bytes(loader):
    test_input = '"$$$ != £££"'.encode('utf8')
    expected = ['$$$ != £££']

    assert list(loader(test_input)) == expected


@pytest.mark.parametrize('loader', LOADERS)
def test_custom_delimiter_works(loader):
    doc ='[1,2],[3, 4]'
    assert list(loader(doc, separator=',')) == [[1, 2], [3, 4]]


@pytest.mark.parametrize('loader', LOADERS)
def test_custom_delimiter_with_wrong_delimiter(loader):
    doc ='[1,2],[3, 4]'
    with pytest.raises(ValueError) as cm:
        list(loader(doc, separator='/'))
    assert str(cm.value) == "Expected '/' delimiter"


@pytest.mark.parametrize('loader', LOADERS)
def test_custom_delimiter_with_empty_value(loader):
    doc = '[],,[]'
    with pytest.raises(JSONDecodeError) as cm:
        list(loader(doc, separator=','))
    assert cm.value.msg == 'Expecting value'
    assert cm.value.pos + getattr(cm.value, 'stream_offset', 0) == 3


@pytest.mark.parametrize(
    'test_input,args',
    [
        pytest.param('', {}, id='simple'),
        pytest.param('', dict(separator=','), id='with separator'),
        pytest.param(' \t\n ', {}, id='only whitespace'),
    ]
)
@pytest.mark.parametrize('loader', LOADERS)
def test_empty_document(test_input, loader, args):
    assert list(loader(test_input, **args)) == []


@pytest.mark.xfail
def test_async_load():
    pytest.fail('not implemented')


@pytest.mark.xfail
def test_optimised_newline_separator(loader):
    pytest.fail('not implemented')
