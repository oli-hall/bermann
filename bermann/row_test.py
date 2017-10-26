import unittest

from bermann import Row


class TestRow(unittest.TestCase):

    def test_row_attributes(self):
        r = Row(a='a', b=123)

        self.assertEqual(r.a, 'a')
        self.assertEqual(r.b, 123)

    def test_row_dict_style_access(self):
        r = Row(a='a', b=123)

        self.assertEqual(r['a'], 'a')
        self.assertEqual(r['b'], 123)

    def test_access_non_existent_attr_raises_attr_error(self):
        row = Row()

        with self.assertRaises(AttributeError) as e:
            foo = row.foo
        self.assertEqual(AttributeError, type(e.exception))

    def test_access_non_existent_key_raises_value_error(self):
        row = Row()

        with self.assertRaises(ValueError) as e:
            foo = row['foo']
        self.assertEqual(ValueError, type(e.exception))

    def test_small_row_asdict(self):
        r = Row(a='a', b=123)

        self.assertEqual(r.asDict(), {'a': 'a', 'b': 123})

    def test_small_row_asdict_recursive(self):
        r = Row(a='a', b=123)

        self.assertEqual(r.asDict(True), {'a': 'a', 'b': 123})

    def test_nested_row_asdict(self):
        r = Row(a=Row(nested_a='a'), b=123)

        self.assertEqual(r.asDict(), {'a': Row(nested_a='a'), 'b': 123})

    def test_nested_row_asdict_recursive(self):
        r = Row(a=Row(nested_a='a'), b=123)

        self.assertEqual(r.asDict(True), {'a': {'nested_a':'a'}, 'b': 123})
